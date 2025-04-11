"""
El objetivo del siguiente script, es poblar la tabla de tramite asociando el ID del usuario interno que genero dicho tramite, 
ya que esto nos servirá para generar endpoints de consultas de rendimientos y mejorar la trazabilidad del proceso de tramites
"""
import psycopg2
from psycopg2 import sql
from datetime import datetime
import sys

# ========== VARIABLES DE CONEXIÓN Y CONFIGURACIÓN ==========

DB_AUTH_CONFIG = {
    'dbname': 'mpio_auth',
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

DB_TRAMITE_CONFIG = {
    'dbname': 'Tu_Catastro_Backup_26032025',
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

# Tabla y campos
AUTH_TABLE = 'auth.auth_user'
HISTORICO_TABLE = 'data.historico_estado_tramite'
TRAMITE_TABLE = 'data.tramite'

# ========== FUNCIONES ==========
# Conexión a una de las DB
def conectar_db(config):
    try:
        conn = psycopg2.connect(**config)
        print("✅ Conexión exitosa a la base de datos:", config['dbname'])
        return conn
    except Exception as e:
        print("❌ Error al conectar a la base de datos:", config['dbname'])
        print(e)
        sys.exit(1)

# Extraer id y nombre de los usuarios internos de mpio auth
def obtener_usuarios(conn):
    try:
        with conn.cursor() as cur:
            query = sql.SQL("""
                SELECT id, TRIM(first_name || ' ' || last_name) AS name_user
                FROM {table}
            """).format(table=sql.Identifier(*AUTH_TABLE.split('.')))
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        print("❌ Error al obtener usuarios de auth_user")
        print(e)
        return []
    
# Extraer los datos asociados al historico estado de tramites
def obtener_datos_historico(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH tramite_con_fechas AS (
                    SELECT 
                        tramite,
                        fecha_inicio,
                        responsable_cambio_estado,
                        estado_tipo,
                        TRIM(SPLIT_PART(responsable_cambio_estado, '-', 2)) AS nombre_responsable,
                        ROW_NUMBER() OVER (PARTITION BY tramite ORDER BY fecha_inicio ASC) AS rn
                    FROM data.historico_estado_tramite
                ),
                conteo_tramites AS (
                    SELECT 
                        tramite, 
                        COUNT(*) AS cantidad
                    FROM data.historico_estado_tramite
                    GROUP BY tramite
                )
                SELECT 
                    c.tramite,
                    t.nombre_responsable
                FROM 
                    conteo_tramites c
                JOIN 
                    tramite_con_fechas t ON c.tramite = t.tramite AND t.rn = 1
            """)
            return cur.fetchall()
    except Exception as e:
        print("❌ Error al obtener datos de historico_estado_tramite")
        print(e)
        return []
    
# Crear diccionario con los atributos necesarios para actualizar el id del tramitador en tramite
def crear_diccionario_tramites(data_historico, usuarios):
    lista_tramites = []
    user_map = {name: uid for uid, name in usuarios}
    no_encontrados = set()

    for tramite, nombre_responsable in data_historico:
        id_tramitador = user_map.get(nombre_responsable)
        if id_tramitador:
            lista_tramites.append({
                'id_tramite': tramite,
                'id_tramitador': id_tramitador,
                'nombre_tramitador': nombre_responsable
            })
        else:
            if nombre_responsable:
                print("⚠️ No se encontró usuario para:", nombre_responsable)
                no_encontrados.add(nombre_responsable)
            else:
                print("⚠️ Nombre de responsable es None para trámite:", tramite)


    print("\n📌 Usuarios no encontrados (sin duplicados):")
    for nombre in sorted(filter(None, no_encontrados)):
        print("-", nombre)

    return lista_tramites, no_encontrados

# Inicializa un diccionario con los usuarios no encontrados
def inicializar_diccionario_usuarios(no_encontrados):
    return {nombre: None for nombre in no_encontrados}

# De los usuarios no encontrados buscamos correo en resolución
def buscar_correos_en_resolucion(conn, usuarios_dict):
    try:
        with conn.cursor() as cur:
            for nombre in usuarios_dict:
                if usuarios_dict[nombre] is None:
                    cur.execute("""
                        SELECT user_res_email 
                        FROM data.resolucion 
                        WHERE usuario_int = %s
                        LIMIT 1
                    """, (nombre,))
                    result = cur.fetchone()
                    if result and result[0]:
                        usuarios_dict[nombre] = result[0]
    except Exception as e:
        print("❌ Error al buscar correos en resolución")
        print(e)
# De los usuarios internos no encontrados buscamos su correo en certificado
def buscar_correos_en_certificado(conn, usuarios_dict):
    try:
        with conn.cursor() as cur:
            for nombre in usuarios_dict:
                if usuarios_dict[nombre] is None:
                    cur.execute("""
                        SELECT user_res_email 
                        FROM data.certificado 
                        WHERE usuario_int = %s
                        LIMIT 1
                    """, (nombre,))
                    result = cur.fetchone()
                    if result and result[0]:
                        usuarios_dict[nombre] = result[0]
    except Exception as e:
        print("❌ Error al buscar correos en certificado")
        print(e)
    
# ESTA FUNCION ES PARA INSERTAR LOS REGISTROS DE USUARIOS INTERNOS FALTANTES INICIALMENTE EN LA DB DE AUTH USER 
def insertar_usuarios_auth(conn, usuarios_dict):
    try:
        with conn.cursor() as cur:
            for nombre, correo in usuarios_dict.items():
                nombres = nombre.strip().split()
                if len(nombres) < 2:
                    continue  # No se puede construir last_name correctamente
                first_name = nombres[0]
                last_name = nombres[1] if len(nombres) == 2 else nombres[2]
                username = (first_name + '.' + last_name).lower()

                # Correo vacío si no viene
                correo = correo.strip() if correo else ''

                password = 'pbkdf2_sha256$216000$fakeSalt$fakeHashGeneratedPassword=='  # Contraseña de ejemplo
                last_login = None
                is_superuser = False
                is_staff = False
                is_active = False
                date_joined = datetime(2022, 1, 1, 12, 0, 0)

                cur.execute("""
                    INSERT INTO auth.auth_user (
                        password, last_login, is_superuser, username, 
                        first_name, last_name, email, is_staff, 
                        is_active, date_joined
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s
                    ) ON CONFLICT (username) DO NOTHING
                """, (
                    password, last_login, is_superuser, username,
                    first_name, last_name, correo, is_staff,
                    is_active, date_joined
                ))
        conn.commit()
        print("✅ Usuarios insertados correctamente en auth_user.")
    except Exception as e:
        print("❌ Error al insertar usuarios nuevos en auth_user")
        print(e)
        conn.rollback()

# Realizar el UPDATE de los atributos de usuario_tramite y de resposable con los datos extraídos en el paso anterior
def actualizar_tabla_tramite(conn, lista_tramites):
    try:
        with conn.cursor() as cur:
            for tramite in lista_tramites:
                query = sql.SQL("""
                    UPDATE {table}
                    SET usuario_tramite = %s,
                        responsable_cambio_estado = 
                            CASE 
                                WHEN responsable_cambio_estado IS NULL THEN %s 
                                ELSE responsable_cambio_estado 
                            END
                    WHERE id = %s
                """).format(table=sql.Identifier(*TRAMITE_TABLE.split('.')))
                
                cur.execute(query, (
                    tramite['id_tramitador'],
                    tramite['nombre_tramitador'],
                    tramite['id_tramite']
                ))
            conn.commit()
            print("✅ Se actualizaron", len(lista_tramites), "registros en la tabla tramite.")
    except Exception as e:
        print("❌ Error al actualizar la tabla tramite")
        print(e)
        conn.rollback()

# ========== FUNCIÓN PRINCIPAL ==========

def main():
    # 1. Conectar a las bases
    conn_auth = conectar_db(DB_AUTH_CONFIG)
    conn_tramite = conectar_db(DB_TRAMITE_CONFIG)

    try:
        # 1. Conectar a las bases
        conn_auth = conectar_db(DB_AUTH_CONFIG)
        conn_tramite = conectar_db(DB_TRAMITE_CONFIG)

        # 2. Obtener usuarios actuales en auth y tramites
        usuarios = obtener_usuarios(conn_auth)
        data_historico = obtener_datos_historico(conn_tramite)

        # 3. Crear lista de tramites y lista de usuarios no encontrados
        lista_tramites, no_encontrados = crear_diccionario_tramites(data_historico, usuarios)

        if lista_tramites:
            if no_encontrados:
                # 4. Inicializar diccionario de correos
                usuarios_dict = inicializar_diccionario_usuarios(no_encontrados)

                # 5. Buscar correos en resolucion y certificado
                buscar_correos_en_resolucion(conn_tramite, usuarios_dict)
                buscar_correos_en_certificado(conn_tramite, usuarios_dict)

                # 6. Insertar usuarios nuevos en auth_user
                insertar_usuarios_auth(conn_auth, usuarios_dict)

                # 7. Obtener nuevamente usuarios actualizados
                usuarios_actualizados = obtener_usuarios(conn_auth)

                # 8. Rehacer asociación ID tramitador y actualizar la tabla tramite
                lista_tramites_actualizada, _ = crear_diccionario_tramites(data_historico, usuarios_actualizados)
                actualizar_tabla_tramite(conn_tramite, lista_tramites_actualizada)
            else:
                print("⚠️ No hay registros de usuarios internos sin relación.")

        else:
            print("⚠️ No hay registros para actualizar.")
    finally:
        conn_auth.close()
        conn_tramite.close()
        print("🔒 Conexiones cerradas.")

if __name__ == '__main__':
    main()