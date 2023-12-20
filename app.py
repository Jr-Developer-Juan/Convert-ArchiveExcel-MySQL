import pandas as pd
import mysql.connector
from mysql.connector import errorcode

def crear_tabla_desde_excel(archivo_excel, host='', usuario='', contrasena='', nombre_base_datos=''):
    try:
        conn = mysql.connector.connect(host=host, user=usuario, password=contrasena)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {nombre_base_datos}")
        cursor.execute(f"USE {nombre_base_datos}")

        df = pd.read_excel(archivo_excel, sheet_name=None)

        for sheet_name, sheet_data in df.items():
            # Corrige los nombres de las columnas
            sheet_data = corregir_nombres_columnas(sheet_data)

            # Añade la columna 'id' con valores consecutivos
            sheet_data.insert(0, 'id', range(1, 1 + len(sheet_data)))

            # Crea la tabla en la base de datos MySQL
            crear_tabla_mysql(cursor, sheet_name, sheet_data, conn)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Acceso denegado. Verifica el usuario y la contraseña.")
        else:
            print(f"Error: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def corregir_nombres_columnas(sheet_data):
    sheet_data.columns = [corregir_nombre(col) for col in sheet_data.columns]
    return sheet_data

def corregir_nombre(nombre):
    # Reemplaza espacios y guiones con guiones bajos y convierte a minúsculas
    nombre_corregido = nombre.replace(' ', '_').replace('-', '_').lower()
    # Elimina caracteres especiales
    nombre_corregido = ''.join(e for e in nombre_corregido if e.isalnum() or e == '_')
    return nombre_corregido

def crear_tabla_mysql(cursor, tabla_nombre, sheet_data, conn):
    columnas = sheet_data.columns

    # Crear la lista de columnas y tipos de datos para la creación de la tabla
    columnas_sql = [f"{col} VARCHAR(255)" for col in columnas]
    create_table_query = f"CREATE TABLE IF NOT EXISTS {tabla_nombre} ({', '.join(columnas_sql)}, PRIMARY KEY (id))"
    
    # Ejecutar la consulta para crear la tabla
    cursor.execute(create_table_query)

    # Iterar sobre las filas del DataFrame y realizar inserciones parametrizadas
    for _, row in sheet_data.iterrows():
        # Reemplazar valores NaN con None antes de insertar en la base de datos
        row = row.where(pd.notna(row), None)

        # Crear la consulta parametrizada para la inserción
        insert_query = f"INSERT INTO {tabla_nombre} ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})"
        
        # Ejecutar la consulta de inserción con los valores de la fila
        cursor.execute(insert_query, tuple(row))

    # Confirmar los cambios en la base de datos
    conn.commit()

if __name__ == "__main__":
    archivo_excel = 'archivo.xlsx'  
    usuario_mysql = '' 
    contrasena_mysql = ''
    nombre_base_datos_mysql = ''
    crear_tabla_desde_excel(archivo_excel, usuario=usuario_mysql, contrasena=contrasena_mysql, nombre_base_datos=nombre_base_datos_mysql)
