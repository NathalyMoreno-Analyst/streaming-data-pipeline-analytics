import pandas as pd
import os

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
# Detecta la ubicación real de este archivo .py (dentro de /scripts)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Define la ruta a la carpeta 'data' subiendo un nivel desde 'scripts'
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', 'data'))

def procesar_datos_streaming(nombre_archivo):
    # Construimos la ruta completa al archivo de entrada de forma dinámica
    ruta_entrada = os.path.join(DATA_DIR, nombre_archivo)
    
    print(f"Iniciando fase de ETL. Buscando archivo en: {ruta_entrada}")
    
    # Verificación de existencia del archivo
    if not os.path.exists(ruta_entrada):
        print(f"Error: No se encontró el archivo en {ruta_entrada}")
        return

    df = pd.read_csv(ruta_entrada)
    
    # Estandarización de métricas
    df['churned_id'] = df['churned'].map({'Yes': 1, 'No': 0})
    
    # GRUPO 1: Dimensión de Usuarios
    dim_usuarios = df[['user_id', 'age', 'gender', 'country']].copy()
    dim_usuarios.drop_duplicates(subset=['user_id'], inplace=True)

    # GRUPO 2: Dimensión de Suscripciones
    dim_suscripciones = df[['user_id', 'subscription_type', 'monthly_fee', 'account_age_months', 'primary_device']].copy()
    
    # GRUPO 3: Tabla de Hechos
    fact_interacciones = df[['user_id', 'favorite_genre', 'avg_watch_time_minutes', 'churned_id']].copy()

    # Guardamos los archivos resultantes en la carpeta /data usando rutas dinámicas
    dim_usuarios.to_csv(os.path.join(DATA_DIR, 'dim_usuarios.csv'), index=False)
    dim_suscripciones.to_csv(os.path.join(DATA_DIR, 'dim_suscripciones.csv'), index=False)
    fact_interacciones.to_csv(os.path.join(DATA_DIR, 'fact_interacciones.csv'), index=False)
    
    print("Fase de procesamiento completada. Archivos guardados exitosamente en /data.")

if __name__ == "__main__":
    # Ahora solo pasas el nombre del archivo, el código ya sabe dónde buscarlo
    procesar_datos_streaming('netflix_user_data.csv')