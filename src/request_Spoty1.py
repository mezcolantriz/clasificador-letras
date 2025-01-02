import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time
from urllib.parse import quote

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET
))

def get_spotify_data(artist, song):
    query = f"{artist} {song}"
    encoded_query = quote(query)
    try:
        results = spotify.search(q=encoded_query, type='track', limit=1)
        tracks = results.get('tracks', {}).get('items', [])
        if tracks:
            track = tracks[0]
            return {
                'SPOTIFY_URL': track['external_urls']['spotify'],
                'ALBUM_NAME': track['album']['name'],
                'ALBUM_RELEASE_DATE': track['album']['release_date'],
                'DURATION_MS': track['duration_ms'],
                'POPULARITY': track['popularity'],
                'PREVIEW_URL': track['preview_url']
            }
    except Exception as e:
        print(f"Error obteniendo datos para {artist} - {song}: {e}")
    return {
        'SPOTIFY_URL': None,
        'ALBUM_NAME': None,
        'ALBUM_RELEASE_DATE': None,
        'DURATION_MS': None,
        'POPULARITY': None,
        'PREVIEW_URL': None
    }

def add_missing_rows(dataset_file, output_file, wait_time=0.1, save_interval=500, max_requests=35000):
    try:
        # Leer los datos originales
        dataset_df = pd.read_csv(dataset_file)

        # Leer el archivo procesado o inicializarlo si no existe
        if os.path.exists(output_file):
            processed_df = pd.read_csv(output_file)
            print(f"Archivo de salida encontrado. Verificando filas faltantes...")
        else:
            print(f"Archivo de salida no encontrado. Creando uno nuevo...")
            processed_df = pd.DataFrame(columns=[
                'ARTIST_NAME', 'SONG_NAME', 'LYRICS', 'SPOTIFY_URL',
                'ALBUM_NAME', 'ALBUM_RELEASE_DATE', 'DURATION_MS',
                'POPULARITY', 'PREVIEW_URL'
            ])

        # Identificar filas faltantes en `combined_with_spotify`
        existing_keys = set(processed_df[['ARTIST_NAME', 'SONG_NAME']].dropna().apply(tuple, axis=1))
        dataset_keys = set(dataset_df[['ARTIST_NAME', 'SONG_NAME']].apply(tuple, axis=1))
        missing_keys = dataset_keys - existing_keys

        # Filtrar filas faltantes desde `combined_dataset`
        missing_rows = dataset_df[dataset_df[['ARTIST_NAME', 'SONG_NAME']].apply(tuple, axis=1).isin(missing_keys)]

        if missing_rows.empty:
            print("No hay filas faltantes en combined_dataset para procesar.")
            return

        print(f"Filas faltantes para procesar: {len(missing_rows)}")

        request_count = 0

        for i, row in missing_rows.iterrows():
            # Obtener datos de Spotify
            spotify_data = get_spotify_data(row['ARTIST_NAME'], row['SONG_NAME'])

            # Crear nueva fila completa
            updated_row = {
                'ARTIST_NAME': row['ARTIST_NAME'],
                'SONG_NAME': row['SONG_NAME'],
                'LYRICS': row['LYRICS'],
                **spotify_data
            }
            processed_df = pd.concat([processed_df, pd.DataFrame([updated_row])], ignore_index=True)

            print(f"[{i+1}/{len(missing_rows)}] Añadido: {spotify_data}")
            request_count += 1

            # Guardar cada `save_interval` filas procesadas
            if request_count % save_interval == 0:
                print(f"Guardando progreso después de {request_count} filas procesadas...")
                processed_df.drop_duplicates(subset=['ARTIST_NAME', 'SONG_NAME'], keep='last', inplace=True)
                processed_df.to_csv(output_file, index=False)

            # Detener si se alcanza el límite de solicitudes
            if request_count >= max_requests:
                print("Límite de solicitudes alcanzado. Deteniendo el proceso temporalmente.")
                break

            # Pausa para respetar la política de la API
            time.sleep(wait_time)

        # Guardar archivo final
        processed_df.drop_duplicates(subset=['ARTIST_NAME', 'SONG_NAME'], keep='last', inplace=True)
        print(f"Guardando archivo final en {output_file}...")
        processed_df.to_csv(output_file, index=False)
        print("Proceso completo.")
    except FileNotFoundError:
        print(f"No se encontró el archivo {dataset_file}. Verifica la ruta.")
    except Exception as e:
        print(f"Se produjo un error: {e}")

if __name__ == "__main__":
    # Directorio base para los archivos
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dataset_file = os.path.join(base_dir, "data", "combined_dataset.csv")
    output_file = os.path.join(base_dir, "data", "combined_with_spotify.csv")

    # Ejecutar el procesamiento
    add_missing_rows(dataset_file, output_file, wait_time=0.1, save_interval=500, max_requests=35000)
