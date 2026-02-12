import os
import pandas as pd
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from datetime import datetime
#from appsheet import preparar_datos
import pytz
import http.client
import json
import streamlit as st
import streamlit.components.v1 as components


# Cargar variables de entorno desde el archivo .env
load_dotenv()

def guardar_en_google_sheets(nombre, correo, telefono, nombre_nino, fecha_nacimiento, comercial, respuestas, resultado_test):
    """
    Guarda los datos del test en una hoja de cálculo de Google Sheets.

    Args:
        nombre (str): Nombre del niño.
        correo (str): Correo electrónico.
        telefono (str): Teléfono.
        respuestas (dict): Diccionario con las respuestas del test.
    """
    # Configurar credenciales desde variables de entorno desde el archivo .env
    '''
    creds_dict = {
        "type": os.getenv("GOOGLE_SHEETS_TYPE"),
        "project_id": os.getenv("GOOGLE_SHEETS_PROJECT_ID"),
        "private_key_id": os.getenv("GOOGLE_SHEETS_PRIVATE_KEY_ID"),
        "private_key": os.getenv("GOOGLE_SHEETS_PRIVATE_KEY").replace('\\n', '\n'),
        "client_email": os.getenv("GOOGLE_SHEETS_CLIENT_EMAIL"),
        "client_id": os.getenv("GOOGLE_SHEETS_CLIENT_ID"),
        "auth_uri": os.getenv("GOOGLE_SHEETS_AUTH_URI"),
        "token_uri": os.getenv("GOOGLE_SHEETS_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("GOOGLE_SHEETS_AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("GOOGLE_SHEETS_CLIENT_X509_CERT_URL"),
        "universe_domain": "googleapis.com"
    }
    '''
    # Para utilizar desde streamlit
    creds_dict = {
        "type": st.secrets["GOOGLE_SHEETS_TYPE"],
        "project_id": st.secrets["GOOGLE_SHEETS_PROJECT_ID"],
        "private_key_id": st.secrets["GOOGLE_SHEETS_PRIVATE_KEY_ID"],
        "private_key": st.secrets["GOOGLE_SHEETS_PRIVATE_KEY"].replace('\\n', '\n'),
        "client_email": st.secrets["GOOGLE_SHEETS_CLIENT_EMAIL"],
        "client_id": st.secrets["GOOGLE_SHEETS_CLIENT_ID"],
        "auth_uri": st.secrets["GOOGLE_SHEETS_AUTH_URI"],
        "token_uri": st.secrets["GOOGLE_SHEETS_TOKEN_URI"],
        "auth_provider_x509_cert_url": st.secrets["GOOGLE_SHEETS_AUTH_PROVIDER_X509_CERT_URL"],
        "client_x509_cert_url": st.secrets["GOOGLE_SHEETS_CLIENT_X509_CERT_URL"],
        "universe_domain": "googleapis.com"
    }

    # Configuración de Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    # Abrir la hoja de cálculo usando el ID y el nombre de la pestaña
    '''
    spreadsheet = client.open_by_key(os.getenv("SPREADSHEET_ID"))
    sheet = spreadsheet.worksheet(os.getenv("GOOGLE_SHEETS_SPREADSHEET_NAME"))
    '''
    spreadsheet = client.open_by_key(st.secrets("SPREADSHEET_ID"))
    sheet = spreadsheet.worksheet(st.secrets("GOOGLE_SHEETS_SPREADSHEET_NAME"))

    # Obtener la fecha y hora actual en la zona horaria de Colombia
    bogota_tz = pytz.timezone("America/Bogota")
    fecha_hora = datetime.now(bogota_tz).strftime("%d/%m/%Y %H:%M:%S")

    # Preparar los datos para guardar
    datos = [fecha_hora, nombre, correo, telefono, nombre_nino, fecha_nacimiento, comercial] + [respuestas[f"pregunta_{i+1}"] for i in range(32)] + [resultado_test]

    # Guardar en Google Sheets
    sheet.append_row(datos)

#########################################################################
# Funciones de Go High Level
#########################################################################
'''
token = os.getenv("token_GHL")
locationId = os.getenv("locationId_GHL")
'''
token = st.secrets["token_GHL"]
locationId = st.secrets["locationId_GHL"]

def usuarios(token=token, locationId=locationId):
    """
    Obtiene y procesa la lista de usuarios (comerciales) desde la API de GoHighLevel.

    La función realiza una petición GET para listar todos los usuarios asociados a una
    ubicación específica, extrae sus datos personales y profesionales, y los organiza
    en un formato tabular (DataFrame) para facilitar búsquedas posteriores.

    Parámetros:
    -----------
    token : str
        Token de acceso Bearer para autenticación en LeadConnector.
    locationId : str
        Identificador único de la subcuenta/ubicación de la cual se desean obtener los usuarios.

    Retorna:
    --------
    pandas.DataFrame
        Un DataFrame con las columnas:
        - 'id_usuario': ID alfanumérico único del usuario (necesario para asignaciones).
        - 'nombre_usuario': Nombre completo del usuario.
        - 'email': Correo electrónico institucional.
        - 'phone': Número de teléfono registrado.
    """

    conn = http.client.HTTPSConnection("services.leadconnectorhq.com")
    payload = ''
    headers = {
    'Accept': 'application/json',
    'Version': '2021-07-28',
    'Authorization': f'Bearer {token}'
    }
    conn.request("GET", f"/users/?locationId={locationId}", payload, headers)
    res = conn.getresponse()
    data = res.read()
    
    # Parsear JSON
    json_data = json.loads(data.decode("utf-8"))
    usuarios = json_data.get('users', [])
    
    # Extraer datos principales y desanidar información relevante
    usuarios_list = []
    
    for opp in usuarios:
        # Datos principales
        opp_data = {
            'id': opp.get('id'),
            'name': opp.get('name'),
            'firstName': opp.get('firstName'),
            'lastName': opp.get('lastName'),
            'email': opp.get('email'),
            'phone': opp.get('phone'),
            'deleted': opp.get('deleted'),
            'roles': opp.get('roles'),
            'scopes': opp.get('scopes'),
            'scopesAssignedToOnly': opp.get('scopesAssignedToOnly'),
            'lcPhone': opp.get('lcPhone')
        }
        usuarios_list.append(opp_data)
    
    # Crear DataFrame
    df_usuarios = pd.DataFrame(usuarios_list)
    
    # seleccionando columnas de interes
    df_usuarios = df_usuarios[['id', 'name', 'email', 'phone']]
    
    # renobrando columnas
    df_usuarios = df_usuarios.rename(columns={'id': 'id_usuario', 'name': 'nombre_usuario'})
    df_usuarios.reset_index(drop=True, inplace=True)
    
    return df_usuarios

def crear_cliente(nombre_completo, correo, telefono, comercial, resultado_test, token=token, locationId=locationId):
    """
    Registra un nuevo contacto en la plataforma GoHighLevel (LeadConnector).

    Esta función realiza tres procesos principales:
    1. Procesa una cadena de texto para intentar separar nombres y apellidos según la cantidad de palabras.
    2. Busca en un DataFrame de usuarios el ID del asesor comercial asignado mediante una coincidencia parcial de nombre.
    3. Realiza una petición POST a la API de LeadConnector para crear el contacto con ubicación y zona horaria predefinida (Bogotá, CO).

    Parámetros:
    -----------
    nombre_completo : str
        Nombre del cliente (ej. "Juana Valentia Perez").
    correo : str
        Email de contacto del cliente.
    telefono : str
        Número telefónico del cliente.
    comercial : str
        Nombre del asesor comercial para buscar su ID de usuario.
    resultado : str
        Resultado del test.
    token : str
        Bearer token de autenticación para la API.
    locationId : str
        ID de la subcuenta/ubicación en GoHighLevel.

    Retorna:
    --------
    None : Imprime en consola la respuesta del servidor (JSON o error).
    """
    # separar el nombre completo en nombre y apellido
    # Limpiamos espacios extra a los lados y dividimos por palabras
    palabras = nombre_completo.strip().split()
    cantidad = len(palabras)
    
    nombre = ""
    apellido = ""

    if cantidad == 2:
        # 1 nombre, 1 apellido
        nombre = palabras[0]
        apellido = palabras[1]
    elif cantidad == 3:
        # 2 nombres, 1 apellido
        nombre = " ".join(palabras[:2])
        apellido = palabras[2]
    elif cantidad == 4:
        # 2 nombres, 2 apellidos
        nombre = " ".join(palabras[:2])
        apellido = " ".join(palabras[2:])
    elif cantidad >= 5:
        # 3 nombres, 2 apellidos (o más palabras)
        nombre = " ".join(palabras[:3])
        apellido = " ".join(palabras[3:])
    else:
        # Caso para nombres de una sola palabra
        nombre = palabras[0] if palabras else ""
        apellido = ""

    # Filtramos las filas donde el nombre contiene la cadena buscada
    # Obtenemos la lista de comerciales
    df = usuarios()
    # na=False evita errores si hay valores nulos, case=False ignora mayúsculas/minúsculas
    resultado = df[df['nombre_usuario'].str.contains(comercial, case=False, na=False)]
    
    # Si encontramos resultados, devolvemos la columna id_usuario
    if not resultado.empty:
        #id_usuario = resultado['id_usuario'].tolist()
        id_usuario = str(resultado['id_usuario'].iloc[0])
    else:
        id_usuario = ""    

    conn = http.client.HTTPSConnection("services.leadconnectorhq.com")
    payload = json.dumps({
        "firstName": nombre,
        "lastName": apellido,
        "name": nombre_completo,
        "email": correo,
        "locationId": locationId,
        "phone": telefono,
        "city": "Bogota",
        "type": "Centro Comercial",
        "timezone": "America/Bogota",
        "country": "CO",
        "assignedTo": id_usuario
        })
    headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Version': '2021-07-28',
    'Authorization': f'Bearer {token}'
    }
    conn.request("POST", "/contacts/", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

    # Registrando el dato en AppSheet
    '''
    try:
        preparar_datos(nombre, apellido, correo, telefono, id_usuario, resultado_test)
    except Exception as e:
        print(f"Error al registrar el dato en AppSheet: {e}")
    '''
    

def agregar_contacto(nombre_completo, correo, telefono, comercial, resultado_test):
    """
    Valida que el nombre no contenga la palabra 'prueba' antes de 
    proceder con la creación del cliente en el CRM.
    """
    
    # Convertimos a minúsculas para que la validación sea insensible a mayúsculas
    if "prueba" in nombre_completo.lower():
        print(f"Registro omitido: El nombre '{nombre_completo}' contiene la palabra de control 'prueba'.")
        console_log(f"Registro omitido: El nombre '{nombre_completo}' contiene la palabra de control 'prueba'.")
    else:
        print(f"Validación exitosa. Procediendo a crear cliente: {nombre_completo}")
        # Ejecutamos la función que ya definiste anteriormente
        try:
            crear_cliente(nombre_completo, correo, telefono, comercial, resultado_test)
        except Exception as e:
            print(f"Error al crear el cliente en GHL: {str(e)}")
            console_log(f"Error al crear el cliente en GHL: {str(e)}")
        
def console_log(mensaje):
    """Envía un mensaje a la consola de JavaScript del navegador."""
    components.html(
        f"""
        <script>
            console.log("{mensaje}");
        </script>
        """,
        height=0, # Evita que ocupe espacio visual en la interfaz
    )

