"""
El objetivo del siguiente script, es poblar la tabla de tramite asociando el ID del usuario interno que genero dicho tramite, 
ya que esto nos servirá para generar endpoints de consultas de rendimientos y mejorar la trazabilidad del proceso de tramites
"""
import psycopg2
from psycopg2 import sql
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

    for tramite, nombre_responsable in data_historico:
        id_tramitador = user_map.get(nombre_responsable)
        if id_tramitador:
            lista_tramites.append({
                'id_tramite': tramite,
                'id_tramitador': id_tramitador,
                'nombre_tramitador': nombre_responsable
            })
        else:
            print("⚠️ No se encontró usuario para:", nombre_responsable)
    return lista_tramites

# Realizar el UPDATE de los atributos de usuario_tramite y de resposable con los datos extraídos en el paso anterior
def actualizar_tabla_tramite(conn, lista_tramites):
    try:
        with conn.cursor() as cur:
            for tramite in lista_tramites:
                query = sql.SQL("""
                    UPDATE {table}
                    SET usuario_tramite = %s,
                        responsable = %s
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
    conn_auth = conectar_db(DB_AUTH_CONFIG)
    conn_tramite = conectar_db(DB_TRAMITE_CONFIG)

    try:
        usuarios = obtener_usuarios(conn_auth)
        data_historico = obtener_datos_historico(conn_tramite)

        lista_tramites = crear_diccionario_tramites(data_historico, usuarios)

        if lista_tramites:
            actualizar_tabla_tramite(conn_tramite, lista_tramites)
        else:
            print("⚠️ No hay registros para actualizar.")
    finally:
        conn_auth.close()
        conn_tramite.close()
        print("🔒 Conexiones cerradas.")

if __name__ == '__main__':
    main()