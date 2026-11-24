"""Carga única del histórico horario de calidad del aire (Bloque 1, apartado 8).

El histórico de calidad del aire de València (con variables meteorológicas por
estación) está publicado en el CKAN de la ciudad (opendata.vlci.valencia.es)
en dos recursos accesibles con datastore_search: 2016-2021 (~449.000 filas) y
2021-2022 (~201.000 filas).

Los dos ficheros tienen formatos de fecha y de hora distintos, e incluso hay
filas con el día y el mes intercambiados. Se guarda TODO como texto, tal cual
viene de la API: arreglarlo es trabajo del Bloque 4. Aquí solo se normalizan
los nombres de columna (minúsculas, sin tildes ni espacios: PM2.5 -> pm2_5)
para que Spark pueda leerlos sin problemas.
"""

import argparse
import csv
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests

RAIZ = Path(__file__).resolve().parent.parent
HISTORICO = RAIZ / "data" / "raw" / "calidad_aire" / "historico"

CKAN = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"

# Recursos en castellano del histórico horario de la Red de Vigilancia de València.
# (El paquete "Datos por horas calidad aire 2016-2020" existe también, pero el
# completo 2016-2021 es el recurso "desde 2016" con 449.026 filas.)
RECURSOS = {
    "2016_2021": "4be7248b-9597-4017-89af-82a9b6e2382f",  # "Datos horarios ... desde 2016"
    "2021_2022": "19b5a1a7-5888-4d3e-b69b-3c1436d64e6e",  # "Datos por horas calidad aire 2021-2022"
}

# datastore_search admite hasta ~32.000 filas por llamada; pedimos 20.000.
PAGINA = 20_000


def get_con_reintentos(url, params, reintentos=3):
    """GET con reintentos ante errores de red, 429 y 5xx.

    Mismo patrón que en recolector/, copiado aquí para que el script se
    ejecute solo, sin montar paquetes Python entre carpetas.
    """
    for intento in range(reintentos):
        try:
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if intento == reintentos - 1:
                raise
            time.sleep(10 * (intento + 1))


def normalizar_columna(nombre: str) -> str:
    """Minúsculas, sin tildes, espacios ni puntos: 'PM2.5' -> 'pm2_5'."""
    sin_tildes = unicodedata.normalize("NFD", nombre)
    sin_tildes = "".join(c for c in sin_tildes if unicodedata.category(c) != "Mn")
    texto = sin_tildes.lower().strip()
    return re.sub(r"[\s.]+", "_", texto).strip("_")


def descargar_recurso(recurso: str, destino: Path):
    """Baja el recurso por paginación y lo escribe a CSV. Devuelve (filas, total).

    Va avanzando por offset con el tamaño real de cada página (el servidor puede
    devolver menos filas de las pedidas) y escribe primero en un .tmp: si la
    descarga se corta a medias, el CSV definitivo no se crea y la próxima
    ejecución lo vuelve a intentar.
    """
    params = {"resource_id": recurso, "limit": PAGINA, "offset": 0, "sort": "_id asc"}
    filas = 0
    tmp = destino.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        cols = None
        while True:
            datos = get_con_reintentos(CKAN, params).json()
            if not datos.get("success"):
                raise RuntimeError(f"CKAN devolvió error: {datos.get('error', datos)}")
            resultado = datos["result"]
            total = resultado["total"]
            if cols is None:
                cols = [c["id"] for c in resultado["fields"] if c["id"] != "_id"]
                w.writerow([normalizar_columna(c) for c in cols])
            records = resultado["records"]
            if not records:
                break
            for reg in records:
                w.writerow(["" if reg.get(c) is None else reg[c] for c in cols])
            filas += len(records)
            print(f"    {filas}/{total} filas", flush=True)
            params["offset"] += len(records)
            if filas >= total:
                break
    os.replace(tmp, destino)
    return filas, total


def main() -> None:
    parser = argparse.ArgumentParser(description="Descarga el histórico de calidad del aire.")
    parser.add_argument("--forzar", action="store_true", help="re-descarga aunque el CSV ya exista")
    args = parser.parse_args()

    for origen, recurso in RECURSOS.items():
        carpeta = HISTORICO / f"origen={origen}"
        destino = carpeta / "datos_horarios.csv"
        if destino.exists() and not args.forzar:
            print(f"{origen}: ya existe ({destino}), no se vuelve a descargar")
            continue

        carpeta.mkdir(parents=True, exist_ok=True)
        print(f"{origen}: descargando recurso {recurso}")
        filas, total = descargar_recurso(recurso, destino)
        if filas != total:
            print(f"{origen}: AVISO, descargadas {filas} filas de {total} anunciadas")

        nota = (
            f"recurso_ckan={recurso}\n"
            f"url={CKAN}?resource_id={recurso}\n"
            f"fecha_descarga_utc={datetime.now(timezone.utc).isoformat()}\n"
            f"filas={filas}\n"
        )
        (carpeta / "_descarga.txt").write_text(nota, encoding="utf-8")
        print(f"{origen}: {filas} filas -> {destino}")


if __name__ == "__main__":
    main()
