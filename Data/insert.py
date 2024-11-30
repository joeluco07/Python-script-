import json
import psycopg2
from django.contrib.auth.hashers import PBKDF2PasswordHasher
import random

# Configura conexión a la base de datos PostgreSQL
DATABASE_CONFIG = {
    'dbname': 'db',
    'user': 'postgres',
    'password': 'an822237',
    'host': 'localhost',
    'port': '5432'
}

# Ruta del archivo JSON
FILE_PATH = 'D:\\ProyectoDjango\\ScriptPos\\Python-script-\\Data\\data.json' # Ruta en Windows
# FILE_PATH = '/home/user/Data/data.json' # Ruta en Linux  o la que corresponda en su sistema


def encriptar_password(password):
    """Encripta la contraseña usando PBKDF2 y SHA-256."""
    hasher = PBKDF2PasswordHasher()
    return hasher.encode(password, hasher.salt())
    
def validar_username(card_no):
    # Asegurarse de que solo contenga dígitos y agregar ceros si es necesario
    if not card_no.isdigit():
        raise ValueError("El valor de 'CardNo' debe contener solo dígitos.")
    
    username = card_no.zfill(10)  # Completar con ceros al inicio hasta 10 dígitos
    return username

def guardar_usuario_en_bd(usuario):
    """Guarda el usuario en la base de datos PostgreSQL."""
    password_encriptado = encriptar_password(usuario['CardNo'])
    full_name = f"{usuario['name']} {usuario['lastname']}"
    username = validar_username(usuario['CardNo'])
    # username = usuario['CardNo']
    date_joined = usuario['acc_startdate'][:10] # Extraer solo la fecha sin los últimos 9 caracteres    
    last_login = None
    is_superuser = False
    image = ''
    is_active = True
    is_staff = False
    is_change_password = False
    email_reset_token = None
    email = usuario['email']
    mobile = str(random.randint(1000000000, 9999999999))
    birthdate = usuario['BIRTHDAY']
    address = usuario['street']
    identication_type = '07'
    send_email_invoice = False
    memenddate = usuario['acc_enddate']
    memstartdate = usuario['acc_startdate']
    useridacces = usuario['USERID']
    group_id = 2

    connection = None  # Definir la conexión antes del bloque try

    try:
        connection = psycopg2.connect(**DATABASE_CONFIG)
        # connection = getConnection()
        cursor = connection.cursor()

        # Insertar en la tabla `atlasfitnessclub.user_user`
        query_table_user_user = """
        INSERT INTO atlasfitnessclub.user_user (
            password, last_login, is_superuser, names, username,image, email,
            is_active, is_staff, date_joined, is_change_password, email_reset_token
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query_table_user_user, (
            password_encriptado,
            last_login,     
            is_superuser,
            full_name,
            username,
            image,
            email,
            is_active,
            is_staff,
            date_joined,
            is_change_password,
            email_reset_token
        ))        

        # Buscar el ID del usuario recién insertado
        select_query = "SELECT id FROM atlasfitnessclub.user_user WHERE username = %s"
        cursor.execute(select_query, (username,))
        user_id = cursor.fetchone()  # Obtener el resultado de la consulta

        if user_id:
            user_id = user_id[0]  # Extraer el ID de la tupla     

               
        query_table_pos_client = """
        INSERT INTO atlasfitnessclub.pos_client (
            dni, mobile, birthdate, address, identification_type, send_email_invoice,
            user_id, memenddate, memstartdate, useridacces
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query_table_pos_client, (
            username,
            mobile,     
            birthdate,
            address,
            identication_type,
            send_email_invoice,
            user_id,
            memenddate,
            memstartdate,
            useridacces
        ))

        query_table_user_user_groups = """
        INSERT INTO atlasfitnessclub.user_user_groups (
            user_id, group_id
        ) VALUES (%s, %s)
        """
        cursor.execute(query_table_user_user_groups, (            
            user_id,
            group_id
        ))

        connection.commit()        
        print(f"Usuario {full_name} guardado exitosamente con ID: {user_id}.")

    except Exception as e:
        print(f"Error al guardar el usuario {full_name}: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def leer_archivo_json(file_path):
    try:
        with open(file_path, 'rb') as f:
            contenido = f.read().decode('ascii', errors='ignore')
        data = json.loads(contenido)
        return data
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
        return None


def main():

    usuario = leer_archivo_json(FILE_PATH)
    """Carga el archivo JSON y guarda cada usuario en la base de datos."""
    try:
        usuarios = leer_archivo_json(FILE_PATH)
        for usuario in usuarios:
            guardar_usuario_en_bd(usuario)
    except FileNotFoundError:
        print(f"No se encontró el archivo JSON en la ruta: {FILE_PATH}")
    except json.JSONDecodeError:
        print("Error al decodificar el archivo JSON.")

if __name__ == "__main__":
    main()
