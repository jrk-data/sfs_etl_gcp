import functions_framework
import os
import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime
from ScraperFC import sofascore as sfc
import pyarrow

@functions_framework.http
def fetch_data():
    """Consulta la API y devuelve los datos en formato JSON."""
    # Creo objeto con la clase de sofascore
    ss = sfc.Sofascore()

    # Pruebo el metodo de scrape_player_league_stats, puedo obtener estadísticas de las posiciones: 
    # 'Defenders', 'Midfielders', 'Forwards', 'Goalkeepers'

    league = 'Argentina Liga Profesional'
    season = '2025'
    stats_type = 'total'
    positions = ['Defenders', 'Midfielders', 'Forwards', 'Goalkeepers']

    players_stats_list = []
    
    for position in positions:
        df = ss.scrape_player_league_stats(season, league, stats_type, [position])
        # Registrar posición
        df['position'] = position
        # Crear hash de fila
        df['hashRow']=pd.util.hash_pandas_object(df, index=False).astype(str) # hash de fila
        # Obtener el timestamp actual
        timestamp_now =  datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        df["dateTimestamp"] = timestamp_now # Creo campo con timestamp
        #players_stats[position] = ss.scrape_player_league_stats(season, league, stats_type, [position])    
        players_stats_list.append(df)
    
    players_stats_dict = {}
    
    players_stats_dict['all_players_stats'] =   pd.concat(players_stats_list)
    return players_stats_dict    

def save_to_parquet(data_players):
    """Convierte datos a Parquet y guarda en un archivo temporal. 
    Se realiza hash de la cada registro para poder comparar a posterior modificaciones en los campos.
    Además guarda los paths en un arreglo."""
    filenames = []
    for position, data in data_players.items():
        df = data
        filename = f"/tmp/{position}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(filename, index=False, engine='pyarrow')
        filenames.append(filename)
    return filenames

def upload_to_gcs(source_file, destination_blob):
    """Sube un archivo a Google Cloud Storage."""
    BUCKET_NAME = "datos-sfs"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Archivo subido a gs://{BUCKET_NAME}/{destination_blob}")


def copy_to_bigquery(source_blob_name, destination_blob_name):
    """Copia un archivo de un bucket a otro."""
    storage_client = storage.Client()
    
    # Defino bucket source y destino
    BUCKET_NAME = "datos-sfs"
    source_bucket = storage_client.bucket(BUCKET_NAME)
    destination_bucket = storage_client.bucket(BUCKET_NAME)

    # Defino blob fuente
    source_blob = source_bucket.blob(source_blob_name)
    
    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    print(f"Archivo copiado de gs://{BUCKET_NAME}/{source_blob_name} a gs://{BUCKET_NAME}/{destination_blob_name}")



def main_function(request):
    """Función principal que se ejecuta en la Cloud Function.
    1) Busca los datos en la api de sofascore.}
    2) Los guarda en archivos parquet.
    3) Sube los archivos a Google Cloud Storage.
    4) Retorna un mensaje con los archivos subidos.
    """    
    try:
        data_players = fetch_data()
        parquet_file = save_to_parquet(data_players)
        destination_paths = []
        for file in parquet_file:
            destination = f"parquets/{os.path.basename(file)}"
            destination_paths.append(destination)
            
            # Subo el archivo al bucket
            upload_to_gcs(file, destination)
            
            # Creo una copia de cada archivo ya subido al bucket y lo guardo en carpeta llamada bigquery. Pisando el archivo si ya existe.
            destination_bigquery = f"bigquery/{os.path.basename(file).split('_')[0]}.parquet"
            
            # Copio el archivo creado desde el bucket parquet al nuevo destination_bigquery
            copy_to_bigquery(destination, destination_bigquery)
        return f"Archivos procesados: \n- {"\n- ".join(destination_paths)}", 200
    except Exception as e:
        return f"Error: {str(e)}", 500