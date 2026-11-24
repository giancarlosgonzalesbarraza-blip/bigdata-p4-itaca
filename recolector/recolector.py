"""Recolector del Bloque 1.

Descarga las fuentes de Open Data (Geoportal de Valencia + AEMET), guarda una
foto de cada fuente en Parquet (particionado por fecha y con columna
captura_utc) y registra cada ejecución en data/raw/_registro/ejecuciones.csv.
"""

import argparse
import csv
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

# ------------------------------------------------------------------
# Configuración
# ------------------------------------------------------------------

RAIZ = Path(__file__).resolve().parent.parent  # raíz del proyecto
DATA_DIR = RAIZ / "data" / "raw"

GEOPORTAL = "https://geoportal.valencia.es/server/rest/services/OPENDATA"
AEMET_API = "https://opendata.aemet.es/opendata/api"

ESTACION_AEMET = "8416"  # Valencia

# La API key vive en recolector/.env, nunca en el código ni en Git.
load_dotenv(Path(__file__).resolve().parent / ".env")
AEMET_API_KEY = os.environ.get("AEMET_API_KEY", "")

FUENTES = {
    "espiras": {"capa": "Trafico/MapServer/208", "ruta": "trafico/espiras"},
    "tramos": {"capa": "Trafico/MapServer/188", "ruta": "trafico/tramos"},
    "estado": {"capa": "Trafico/MapServer/192", "ruta": "trafico/estado"},
    "aire": {"capa": "MedioAmbiente/MapServer/156", "ruta": "calidad_aire/estaciones"},
    "meteo": {"capa": "MedioAmbiente/MapServer/157", "ruta": "meteo/municipal"},
    "aemet": {"ruta": f"aemet/{ESTACION_AEMET}"},  # sin "capa": se descarga con API key
}


# ------------------------------------------------------------------
# Descarga (apartado 3)
# ------------------------------------------------------------------


def get_con_reintentos(url, params=None, reintentos=3):
    """GET con reintentos ante errores de red, 429 y 5xx."""
    for intento in range(reintentos):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if intento == reintentos - 1:
                raise
            time.sleep(10 * (intento + 1))


def descargar_capa(capa: str) -> list[dict]:
    """Devuelve todos los registros de una capa ArcGIS (con paginación)."""
    url = f"{GEOPORTAL}/{capa}/query"
    params = {"where": "1=1", "outFields": "*", "f": "json", "resultOffset": 0}
    registros = []
    while True:
        datos = get_con_reintentos(url, params).json()
        registros += [f["attributes"] for f in datos["features"]]
        if not datos.get("exceededTransferLimit"):
            return registros
        params["resultOffset"] = len(registros)


def descargar_aemet(estacion: str = ESTACION_AEMET) -> list[dict]:
    """AEMET en dos pasos: 1) pedir el enlace de datos, 2) descargarlo."""
    if not AEMET_API_KEY:
        raise RuntimeError("Falta AEMET_API_KEY: define la clave en recolector/.env")

    # Paso 1: la API devuelve metadatos y el enlace al fichero de datos.
    url_1 = f"{AEMET_API}/observacion/convencional/datos/estacion/{estacion}"
    metadatos = get_con_reintentos(url_1, {"api_key": AEMET_API_KEY}).json()
    if metadatos.get("estado") != 200:
        raise RuntimeError(f"AEMET: {metadatos.get('descripcion', metadatos)}")

    # Paso 2: el enlace 'datos' es público, no necesita api_key.
    return get_con_reintentos(metadatos["datos"]).json()


# ------------------------------------------------------------------
# Guardado y registro (apartado 4)
# ------------------------------------------------------------------


def guardar_parquet(registros: list[dict], ruta: str, nombre: str, captura: datetime) -> Path:
    """Escribe <DATA_DIR>/<ruta>/fecha=AAAA-MM-DD/<nombre>_AAAAMMDDTHHMMSSZ.parquet."""
    tabla = pa.Table.from_pylist(registros)
    tabla = tabla.append_column("captura_utc", pa.array([captura] * tabla.num_rows))
    carpeta = DATA_DIR / ruta / f"fecha={captura:%Y-%m-%d}"
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / f"{nombre}_{captura:%Y%m%dT%H%M%SZ}.parquet"
    pq.write_table(tabla, destino, compression="snappy")
    return destino


def registrar(captura: datetime, fuente: str, ok: bool, filas: int, error: str = "") -> None:
    """Añade una línea a _registro/ejecuciones.csv."""
    fichero = DATA_DIR / "_registro" / "ejecuciones.csv"
    fichero.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not fichero.exists()
    with fichero.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nuevo:
            w.writerow(["captura_utc", "fuente", "ok", "filas", "error"])
        w.writerow([captura.isoformat(), fuente, int(ok), filas, error[:300]])


# ------------------------------------------------------------------
# Ejecución
# ------------------------------------------------------------------


def capturar(nombre: str, fuente: dict, captura: datetime) -> None:
    """Descarga una fuente, guarda su parquet y registra el resultado."""
    try:
        if "capa" in fuente:                       # Geoportal (una función para todas)
            registros = descargar_capa(fuente["capa"])
        elif nombre == "aemet":                    # AEMET (dos pasos + api_key)
            registros = descargar_aemet()
        else:
            raise ValueError(f"No sé descargar la fuente '{nombre}'")
        if not registros:
            raise ValueError("la fuente devolvió 0 registros")

        destino = guardar_parquet(registros, fuente["ruta"], nombre, captura)
        registrar(captura, nombre, True, len(registros))
        print(f"{nombre}: {len(registros)} registros -> {destino}")
    except Exception as e:
        registrar(captura, nombre, False, 0, f"{type(e).__name__}: {e}")
        print(f"{nombre}: ERROR -> {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Recolector: descarga las fuentes (todas por defecto).")
    parser.add_argument("fuentes", nargs="*", help="nombres de FUENTES a descargar, p. ej.: estado tramos")
    seleccion = parser.parse_args().fuentes

    desconocidas = set(seleccion) - set(FUENTES)
    if desconocidas:
        raise SystemExit(f"Fuentes desconocidas: {sorted(desconocidas)}. Válidas: {', '.join(FUENTES)}")

    fuentes = {n: FUENTES[n] for n in seleccion} if seleccion else FUENTES
    captura = datetime.now(timezone.utc)  # una sola marca UTC para toda la ejecución
    for nombre, fuente in fuentes.items():
        capturar(nombre, fuente, captura)
