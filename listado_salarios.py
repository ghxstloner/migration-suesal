import pandas as pd
import re
import pymysql
import numpy as np

# Leer el archivo Excel
def actualizar_sueldos_desde_excel(ruta_excel, conexion_db):
    # Cargar el archivo Excel
    df = pd.read_excel(ruta_excel)
    
    # Crear cursor para operaciones en la base de datos
    cursor = conexion_db.cursor()
    
    # Contador para seguimiento
    actualizados = 0
    errores = 0
    
    # Procesar cada fila del Excel
    for index, fila in df.iterrows():
        try:
            # Extraer el número de ficha (eliminar 'E' y ceros iniciales)
            personal_txt = str(fila['Personal']).strip()
            ficha = re.search(r'E0*(\d+)', personal_txt).group(1)
            
            # Valores a actualizar, con manejo de NaN
            # Reemplazar NaN con None (que se convierte en NULL en MySQL)
            sueldo_mensual = fila['SueldoMensual'] if not pd.isna(fila['SueldoMensual']) else None
            gasto_rep = fila['Gasto Rep'] if not pd.isna(fila['Gasto Rep']) else None
            sueldo_diario = fila['SueldoDiario'] if not pd.isna(fila['SueldoDiario']) else None
            rata_hora = fila['RataHora'] if not pd.isna(fila['RataHora']) else None
            
            # Preparar y ejecutar consulta SQL para actualizar
            sql = """
            UPDATE nompersonal 
            SET suesal = %s, 
                sueldopro = %s, 
                gastos_representacion = %s, 
                salario_diario = %s, 
                rata_x_hr = %s 
            WHERE ficha = %s
            """
            
            # Ejecutar consulta
            cursor.execute(sql, (
                sueldo_mensual,  # suesal
                sueldo_mensual,  # sueldopro
                gasto_rep,
                sueldo_diario,
                rata_hora,
                ficha
            ))
            
            actualizados += 1
            print(f"Actualizado registro con ficha {ficha}")
            
        except Exception as e:
            print(f"Error procesando fila {index+1} ({personal_txt}): {str(e)}")
            errores += 1
    
    # Confirmar cambios
    conexion_db.commit()
    cursor.close()
    
    return actualizados, errores

# Función principal
def main():
    # Configuración de la conexión a la base de datos
    config_db = {
        'host': 'localhost',
        'user': 'root',
        'password': 'S3l3ctr4$',
        'database': 'aitsa_rrhh'
    }
    
    try:
        # Establecer conexión con la base de datos
        conexion = pymysql.connect(**config_db)
        
        # Ruta del archivo Excel
        ruta_excel = 'listado_salarios.xlsx'
        
        # Actualizar datos
        actualizados, errores = actualizar_sueldos_desde_excel(ruta_excel, conexion)
        
        print(f"Proceso completado:")
        print(f"- Registros actualizados: {actualizados}")
        print(f"- Errores encontrados: {errores}")
        
        # Cerrar conexión
        conexion.close()
        
    except Exception as e:
        print(f"Error general: {str(e)}")

if __name__ == "__main__":
    main()