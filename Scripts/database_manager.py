import pandas as pd
import os
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', 'data'))

def gestionar_base_datos(nombre_db='streaming_analytics.db'):
    # Construimos la ruta dinámica a la base de datos
    ruta_db = os.path.join(DATA_DIR, nombre_db)
    engine = create_engine(f'sqlite:///{ruta_db}')
    
    print(f"Estableciendo conexión con la base de datos en: {ruta_db}")

    try:
        # Carga de grupos procesados desde la carpeta /data
        usuarios = pd.read_csv(os.path.join(DATA_DIR, 'dim_usuarios.csv'))
        suscripciones = pd.read_csv(os.path.join(DATA_DIR, 'dim_suscripciones.csv'))
        interacciones = pd.read_csv(os.path.join(DATA_DIR, 'fact_interacciones.csv'))

        # Migración de datos a tablas SQL
        usuarios.to_sql('dim_usuarios', con=engine, if_exists='replace', index=False)
        suscripciones.to_sql('dim_suscripciones', con=engine, if_exists='replace', index=False)
        interacciones.to_sql('fact_interacciones', con=engine, if_exists='replace', index=False)

        print("Arquitectura de datos desplegada correctamente en SQL (Carpeta /data).")
        
    except Exception as e:
        print(f"Error durante la migración de datos: {e}")

if __name__ == "__main__":
    gestionar_base_datos()