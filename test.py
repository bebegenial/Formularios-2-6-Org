import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from drive import guardar_en_google_sheets,agregar_contacto,console_log
import datetime
#pip freeze > requirements.txt
#streamlit run test.py

# Configuración de la página para que sea responsiva
st.set_page_config(layout="wide")

# Estilo CSS para mejorar la visualización en móviles y tablets
st.markdown("""
<style>
    .stRadio > label {
        font-size: 1.2rem;
    }
    .stButton>button {
        width: 100%;
    }
    .stTextInput>div>div>input {
        font-size: 1.3rem;
    }
    .stMarkdown {
        font-size: 1.3rem;
    }
</style>
""", unsafe_allow_html=True)

# Título y descripción
st.title("🧠 Test de Inteligencias Múltiples - El Expreso de Leo 🚂")
st.markdown("""
Este test es una herramienta de observación para familias de niños de 2 a 6 años.
Su propósito es ayudar a reconocer cómo aprende, se expresa y se relaciona su hijo.
""")

# Aviso de Habeas Data
st.header("📜 Aviso de Privacidad y Tratamiento de Datos")
acepto = st.checkbox("""
Este formulario tiene como finalidad recolectar sus datos personales para procesar los resultados del **Test de Inteligencias Múltiples** y brindar una orientación personalizada sobre el perfil de aprendizaje de su hijo(a). 

La información recolectada será tratada bajo los principios de confidencialidad y seguridad, conforme a la **Ley 1581 de 2012 de Protección de Datos Personales (Habeas Data)** en Colombia.

El responsable del tratamiento de estos datos es **Editorial Bebe Genial**.

Al registrar sus datos, **usted autoriza a la editorial para**:

1.Gestionar y entregar los resultados del presente test.

2.Enviarle información pedagógica, publicidad de nuestros productos y servicios educativos.

3.Contactarlo para asesoría sobre el material didáctico que mejor se adapte a los resultados obtenidos.

Usted tiene derecho a conocer, actualizar y rectificar sus datos en cualquier momento a través del correo **servicioalcliente@bebegenial.com**.

Puede consultar nuestra Política de Privacidad [aquí](https://www.bebegenial.com/privacy-policy/).

**Al marcar esta casilla, usted acepta el tratamiento de sus datos conforme a lo descrito.**
""")

# Si no acepta, no se muestra el formulario
if not acepto:
    st.stop()

# Diccionario para mapear ID a nombre del comercial
comerciales = {
    "0490": "Paula",
    "8971": "Pilar",
    "8949": "Lorena",
    "8870": "Sebastian",
    "7171": "Angelo",
    "9043": "Martha",
    "0640": "Edgar",
    "0080": "Estefania",
    "7863": "Eliana",
    "7509": "Sandra",
    "0048": "Nataly"
}

# Recolección de datos
st.header("✨ Recolección de datos")
nombre = st.text_input("Nombre del padre o representante legal:")
correo = st.text_input("Correo electrónico:")
telefono = st.text_input("Teléfono:")
nombre_nino = st.text_input("Nombre del niño:")
fecha_nacimiento = st.date_input(
    "Fecha de nacimiento del niño:",
    value=None, # Puedes poner datetime.date(2015, 1, 1) como predeterminado
    min_value=datetime.date.today() - datetime.timedelta(days=2555), # Límite inferior
    max_value=datetime.date.today() - datetime.timedelta(days=730),
    #max_value=datetime.date.today(),      # No permite fechas futuras
    format="DD/MM/YYYY"
)
# id_comercial = st.selectbox("ID del comercial:", index=None ,options=list(comerciales.keys()))

# Validación de correo y teléfono
def validar_correo(correo):
    return "@" in correo and "." in correo.split("@")[-1]

def validar_telefono(telefono):
    # Primero verificamos que sean solo números
    if not telefono.isdigit():
        return False
    
    # Si inicia con "3", la longitud debe ser >= 10
    if telefono.startswith("3"):
        return len(telefono) >= 10
    
    # Para cualquier otro caso, mantenemos la regla de longitud >= 7
    return len(telefono) >= 7

# Sección de preguntas
st.header("📝 Test de Inteligencias Múltiples")
inteligencias = [
    "Lingüística", "Lógico-Matemática", "Espacial",
    "Corporal-Kinestésica", "Musical", "Interpersonal", "Intrapersonal", "Naturalista"
]

preguntas = [
    # Lingüística
    "Disfruta que le lean cuentos o historias.",
    "Intenta contar lo que ve o lo que siente.",
    "Juega con sonidos, palabras o canciones.",
    "Usa gestos, sonidos o palabras para comunicarse.",
    # Lógico-Matemática
    "Le gusta clasificar objetos por color, forma o tamaño.",
    "Disfruta juegos de encajar, armar o resolver retos.",
    "Nota cuando algo cambia o no está como siempre.",
    "Explora relaciones de causa y efecto.",
    # Espacial
    "Reconoce espacios y lugares conocidos.",
    "Disfruta dibujar, armar rompecabezas o mirar imágenes.",
    "Observa detalles en su entorno.",
    "Usa el juego simbólico (casitas, carros, muñecos).",
    # Corporal-Kinestésica
    "Aprende mejor moviéndose y explorando.",
    "Disfruta correr, saltar, bailar o trepar.",
    "Expresa emociones con su cuerpo.",
    "Tiene buena coordinación para su edad.",
    # Musical
    "Reacciona positivamente a la música o sonidos.",
    "Tararea, canta o sigue ritmos.",
    "La música lo calma o lo activa.",
    "Reconoce canciones o sonidos familiares.",
    # Interpersonal
    "Busca interactuar con otros niños o adultos.",
    "Percibe emociones en los demás.",
    "Disfruta juegos en grupo.",
    "Muestra interés por ayudar o acompañar.",
    # Intrapersonal
    "Expresa lo que siente a su manera.",
    "A veces prefiere jugar solo.",
    "Tiene claras sus preferencias.",
    "Se siente seguro con rutinas.",
    # Naturalista
    "Muestra curiosidad por animales o plantas.",
    "Disfruta estar al aire libre.",
    "Observa cambios en la naturaleza.",
    "Cuida su entorno."
]

# Diccionario para almacenar respuestas
respuestas = {}

# Mostrar preguntas y opciones (sin valores numéricos y sin preselección)
for i, pregunta in enumerate(preguntas):
    respuestas[f"pregunta_{i+1}"] = st.radio(
        f"**{i+1}. {pregunta}**",
        options=["Nunca", "Ocasionalmente", "Frecuentemente", "Siempre"],
        index=None,  # Evita que esté preseleccionado
        key=f"pregunta_{i+1}"
    )

# Botón para procesar resultados
procesado = st.button("Procesar resultados", key="boton_procesar_1")

if procesado:
    # Validar datos
    if not validar_correo(correo):
        st.error("Por favor, ingresa un correo electrónico válido.")
    elif not nombre:
        st.error("Por favor, ingresa el nombre del padre o representante legal.")
    elif not nombre_nino:
        st.error("Por favor, ingresa el nombre del niño.")
    elif not fecha_nacimiento:
        st.error("Por favor, ingresa la fecha de nacimiento del niño.")
    elif not validar_telefono(telefono):
        st.error("Por favor, ingresa un número de teléfono válido.")
    elif any(respuesta is None for respuesta in respuestas.values()):
        st.error("Por favor, responde todas las preguntas.")
    # elif id_comercial is None or id_comercial == "":
    #     st.error("Por favor, selecciona el ID de un comercial.")
    else:
        # Asignar valores numéricos según la respuesta seleccionada
        valores_respuestas = {
            "Nunca": 0,
            "Ocasionalmente": 1,
            "Frecuentemente": 2,
            "Siempre": 3
        }

        # Calcular subtotales
        subtotales = {}
        for idx, inteligencia in enumerate(inteligencias):
            inicio = idx * 4
            fin = inicio + 4
            subtotales[inteligencia] = sum(
                valores_respuestas[respuestas[f"pregunta_{i+1}"]] for i in range(inicio, fin)
            )

        # Obtener el nombre del comercial a partir del ID seleccionado
        #nombre_comercial = comerciales[id_comercial]    

        # Mostrar resultados
        resultado_test = ""
        st.header("📊 Resultados")
        st.write("### Subtotales por inteligencia:")
        for inteligencia, puntaje in subtotales.items():
            st.write(f"- **{inteligencia}**: {puntaje}/12")
            # Creando una variable llamada 'Resultado' que almacena el resultado de los resultados
            resultado_test += f"- {inteligencia}: {puntaje}/12\n"
        
        # Gráfico de barras con etiquetas inclinadas
        fig, ax = plt.subplots(figsize=(10, 6))
        barras = ax.bar(subtotales.keys(), subtotales.values(), color=[
            "#9b59b6", "#3498db", "#2ecc71", "#e74c3c",
            "#f1c40f", "#e67e22", "#1abc9c", "#34495e"
        ])
        ax.set_ylabel("Puntuación")
        ax.set_title("Puntuación por Inteligencia")
        ax.bar_label(barras, labels=[f"{valor}" for valor in subtotales.values()], padding=3)
        plt.xticks(rotation=45, ha='right')  # Inclinar etiquetas a 45 grados
        st.pyplot(fig)

        ###########################################################################################
        # Gráfico general radar chart
        # Gráfico de radar
        fig2 = plt.figure(figsize=(8, 8))
        ax2 = fig2.add_subplot(111, polar=True)

        # Configurar los ángulos para cada inteligencia
        categorias = list(subtotales.keys())
        N = len(categorias)
        angulos = [n / float(N) * 2 * 3.14159 for n in range(N)]
        angulos += angulos[:1]  # Cerrar el gráfico

        # Valores de las puntuaciones
        valores = list(subtotales.values())
        valores += valores[:1]  # Cerrar el gráfico

        # Dibujar el radar chart
        ax2.plot(angulos, valores, color='blue', linewidth=2, linestyle='solid', label='Puntuación')
        ax2.fill(angulos, valores, color='blue', alpha=0.25)

        # Configurar las etiquetas y título
        ax2.set_thetagrids([a * 180/3.14159 for a in angulos[:-1]], categorias)
        ax2.set_title("Perfil de Inteligencias Múltiples", size=15, y=1.1)
        ax2.grid(True)

        # Establecer el límite del eje radial
        ax2.set_ylim(0, 12)

        st.pyplot(fig2)
        ###########################################################################################

        # Deshabilitar botón
        st.success("Gracias por ser parte de la familia Bebe Genial.\nTu opinión nos ayuda a crear experiencias cada vez más significativas para ti y tu familia.")
        st.button("Procesar resultados", disabled=True, key="boton_procesar_deshabilitado")

        ###########################################################################################
        # Crear el dato en GHL
        #agregar_contacto(nombre, correo, telefono, nombre_comercial, resultado_test)
        # Guardar en Google Sheets
        # Convertimos a minúsculas para que la validación sea insensible a mayúsculas
        if "prueba" in nombre.lower():
            print(f"Registro omitido: El nombre '{nombre}' contiene la palabra de control 'prueba'.")
            console_log(f"Registro omitido: El nombre '{nombre}' contiene la palabra de control 'prueba'.")
        else:
            print(f"Validación exitosa. Procediendo a crear cliente: {nombre}")
            console_log(f"Validación exitosa. Procediendo a crear cliente: {nombre}")
            # Ejecutamos la función que ya definiste anteriormente
            try:
                guardar_en_google_sheets(nombre, correo, telefono, nombre_nino, str(fecha_nacimiento), "Organico", respuestas, resultado_test) 
            except Exception as e:
                print(f"Error al crear el cliente en Google Sheets: {str(e)}")
                console_log(f"Error al crear el cliente en Google Sheets: {str(e)}")
                