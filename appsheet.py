import requests
import pandas as pd
import json  # Importar json para convertir el cuerpo a string
from pandas import json_normalize
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

usuarios_data = {
    "1FBIHQHWPKhBmO6a9rQQ": "arodriguez",
    "DoRgcqP5JsJTmtwoaZ3p": "ecardozo",
    "5D6RDdQVyijpNnJTx7eR": "evelosa",
    "umIPiqJWX11Yb4dQVphl": "",
    "qhunbJtT2idw1wzGByUc": "evalencia",
    "sRzniY5hQegGYGHVXPLt": "",
    "xQUXxIsmeUdRLj0RhRXR": "",
    "UdYGVIpIyZ3YIFuloaKn": "abotello",
    "A2IuE4eVCYx8tRcOoF0s": "mbarrera",
    "fgBY5m2B9UUeidYhhDfT": "",
    "sBnFtxFSuTpQkeTqRJlX": "mlemus",
    "nvXnN7TquI0aYZCoO1U1": "csarmiento",
    "uE1k4lwxxz0zT6yHyzzB": "prey",
    "PpSNzWbNhH7lkwM7D2Jc": "",
    "qGtbNT6w1UK5oK28u85S": "sgonzalez",
    "3glalWI8ATLakUsqBeKg": "ygomez"
}

# Configuracion para la app de Bebe Genial
APP_ID = os.getenv('APP_ID')
APP_ACCESS_KEY = os.getenv('APP_ACCESS_KEY')

# Configuracion para la app de Bebe Genial
user_settings = {
    "nomb_usu": os.getenv('NOMB_USU'),
    "clave": os.getenv('CLAVE'),
    "rol": os.getenv('ROL')
}

# Cabecera de la peticion
headers = {
    "ApplicationAccessKey": APP_ACCESS_KEY,
    "Content-Type": "application/json"
}

def enviar_Peticion(action, rows_data, TABLE_NAME):
    """
    Envía una petición a la API de AppSheet para realizar acciones sobre una tabla.

    Parámetros:
        action (str): La acción a realizar ("A" para agregar, "F" para encontrar,
                      "D" para eliminar, "E" para editar).
        rows_data (list): Los datos a enviar en formato JSON.
        TABLE_NAME: Nombre de la tabla de la base de datos que deseas manipular.

    Retorna:
        dict: Respuesta JSON de la API o None si hubo un error.
    """
    
    url = f"https://www.appsheet.com/api/v2/apps/{APP_ID}/tables/{TABLE_NAME}/Action?applicationAccessKey={APP_ACCESS_KEY}"

    if action not in ["A", "F", "D", "E"] or (action in ["A", "D", "E"] and rows_data is None):
        print("\nLos parámetros especificados son incorrectos\n")
        return None

    body = {
        "Action": action_mapping(action),
        "Properties": {
            "Locale": "en-US",
            "Location": "47.623098, -122.330184",
            "Timezone": "Pacific Standard Time",
        },
        "Rows": rows_data if action != "F" else []
    }
    
    # Agregar user_settings si se proporciona
    if user_settings:
        body["Properties"]["UserSettings"] = user_settings

    response = requests.post(url, headers=headers, json=body)

    if response.status_code != 200:
        print(f"Error en la solicitud: {response.status_code} - {response.text}")
        return None

    return response

def action_mapping(action):
    """Mapea las acciones abreviadas a sus nombres completos."""
    mapping = {
        "A": "Add",
        "F": "Find",
        "D": "Delete",
        "E": "Edit"
    }
    return mapping.get(action)

def conector_AppSheet(action, df, TABLE_NAME):
    """
    Envía una petición a la API de AppSheet para realizar la acciones como la de eliminar, agregar, editar o extraer datos sobre una tabla.

    Parámetros:
        action (str): La acción a realizar ("A" para agregar, "F" para Extraer los datos,
                      "D" para eliminar, "E" para editar).
        df: Se requiere el Dataframe que desea agregar, debe tener las mismas variable que la tabla destino,
            debe contener el el ID del registro del dato para asi saber cual modificar, no se requiere cuando la accion es extraer los datos.
        TABLE_NAME: Nombre de la tabla de la base de datos que deseas manipular.

    Retorna:
        JSON cuando la eliminacion de datos fue exitosa   

    Ejemplos:
        Para extraer todos los datos de la tabla
            Datos = conector_AppSheet("F", None, "Actividad")
        
        Para Eliminar uno o varios datos de la tabla
            conector_AppSheet("D", df, "Actividad") # df debe contener al menos el ID del o los registros a eliminar
            
        Para editar uno o varios datos de la tabla
            Datos = conector_AppSheet("E", df, "Actividad") # df debe contener al menos el ID del o los registros a eliminar y las varibles a editar

        Para agregar uno o varios datos de la tabla
            Datos = conector_AppSheet("A", df, "Actividad") # df debe contener el ID unico del o los registros a agregar y las varibles
    """

    # Realiza la solicitud POST a la API
    if action=="F":
        response = enviar_Peticion(action, None, TABLE_NAME)
    else:
        # Rellenar los valores NaN con None
        # Esto asegura que los valores se serialicen como 'null' en JSON.
        df = df.replace({np.nan: None})
        # Convertir el DataFrame a una lista de diccionarios
        rows_data = df.to_dict(orient='records')
        response = enviar_Peticion(action, rows_data, TABLE_NAME)

    # Verifica el estado de la respuesta
    if response.status_code == 200:
        try:
            # Extrae los datos en formato JSON y los transforma en el dataframe
            data = json_normalize(response.json())
            print(f"La accion de {action_mapping(action)} fue realizada sobre los siguientes datos:\n", data.to_json(),"\n")
            if data.empty:
                print("No se encontraron datos en la tabla\n")
            if action=="F":
                return data            
        except json.JSONDecodeError:
            print("Error al decodificar JSON:", response.text, "\n Estatus:", response.status_code)
    else:
        print("Error al obtener los datos:", response.status_code)
        print("Contenido de la respuesta:", response.text)

def preparar_datos(nombre, apellido, correo, telefono, id_comercial, resultado_test):
    """
    Prepara los datos para ser enviados a AppSheet.
    """
    # Obteniendo el nombre del usuario
    nombre_usuario = usuarios_data.get(id_comercial, "hrosas")

    # Rellenar los valores NaN con None
    # Esto asegura que los valores se serialicen como 'null' en JSON.
    appsheet_data = pd.DataFrame({
        "Nombre": [nombre],
        "Apellidos": [apellido],
        "Correo electrónico": [correo],
        "Mobile Phone": [telefono],
        "nomb_usu": [nombre_usuario],
        "Estado": ["Pendiente Por Gestion"],
        "Toma de contacto": ["Centro Comercial"],
        "Descripción": [resultado_test]
    })
    
    # Creando el registro
    print("\nProcesando datos para AppSheet\n")
    conector_AppSheet("A", appsheet_data, "Datos")
    
    
