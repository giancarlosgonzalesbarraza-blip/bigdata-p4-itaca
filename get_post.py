# GET simple a la API de JSONPlaceholder (post con id 1)
import json
import requests

URL = "https://jsonplaceholder.typicode.com/posts/1"


def get_post():
    try:
        response = requests.get(URL, timeout=10)

        # raise_for_status() lanza una excepción si el status no es 200-299
        response.raise_for_status()

        data = response.json()  # convertimos la respuesta JSON a diccionario

        print(json.dumps(data, indent=2, ensure_ascii=False))  # JSON formateado
        print("Título:", data["title"])  # accedemos a un campo concreto

        return data

    except requests.exceptions.RequestException as error:
        print("La petición falló:", error)


if __name__ == "__main__":
    get_post()
