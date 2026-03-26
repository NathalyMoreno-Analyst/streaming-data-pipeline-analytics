import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', 'data'))

def ejecutar_todo_el_proceso():
    nombre_db = 'streaming_analytics.db'
    ruta_db = os.path.join(DATA_DIR, nombre_db)
    engine = create_engine(f'sqlite:///{ruta_db}')
    
    print("1. Cargando datos desde /data y actualizando base de datos SQL...")
    try:
        # Carga de archivos base usando rutas dinámicas
        usuarios = pd.read_csv(os.path.join(DATA_DIR, 'dim_usuarios.csv'))
        suscripciones = pd.read_csv(os.path.join(DATA_DIR, 'dim_suscripciones.csv'))
        interacciones = pd.read_csv(os.path.join(DATA_DIR, 'fact_interacciones.csv'))

        # Guardar en SQL
        usuarios.to_sql('dim_usuarios', con=engine, if_exists='replace', index=False)
        suscripciones.to_sql('dim_suscripciones', con=engine, if_exists='replace', index=False)
        interacciones.to_sql('fact_interacciones', con=engine, if_exists='replace', index=False)
        print(f"--- Base de datos SQL lista en: {ruta_db}")
    except Exception as e:
        print(f"--- Error al crear la base de datos: {e}")
        return

    print("2. Generando archivos CSV finales para Power BI en /data...")
    conn = sqlite3.connect(ruta_db)

    # Tabla 1: Churn con user_id
    query_churn = '''
        SELECT s.user_id, s.primary_device, i.churned_id
        FROM dim_suscripciones s
        JOIN fact_interacciones i ON s.user_id = i.user_id
    '''
    pd.read_sql_query(query_churn, conn).to_csv(os.path.join(DATA_DIR, 'bi_churn_dispositivo.csv'), index=False)

    # Tabla 2: Engagement con user_id
    query_engagement = '''
        SELECT u.user_id, u.country, i.avg_watch_time_minutes
        FROM dim_usuarios u
        JOIN fact_interacciones i ON u.user_id = i.user_id
    '''
    pd.read_sql_query(query_engagement, conn).to_csv(os.path.join(DATA_DIR, 'bi_engagement_pais.csv'), index=False)

    # Tabla 3: Rentabilidad con user_id
    query_rentabilidad = '''
        SELECT user_id, subscription_type, monthly_fee, account_age_months
        FROM dim_suscripciones
    '''
    pd.read_sql_query(query_rentabilidad, conn).to_csv(os.path.join(DATA_DIR, 'bi_rentabilidad_plan.csv'), index=False)

    conn.close()
    print("--- Archivos bi_*.csv generados correctamente en /data.")

if __name__ == "__main__":
    ejecutar_todo_el_proceso()