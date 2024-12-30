import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las claves de Spotify desde el .env
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Autenticación con Spotify
spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id=SPOTIFY_CLIENT_ID,
    client_secret=SPOTIFY_CLIENT_SECRET
))

# Función para buscar la información de Spotify
def get_spotify_data(artist, song):
    """
    Busca la información de Spotify para una canción específica.
    :param artist: Nombre del artista
    :param song: Nombre de la canción
    :return: Diccionario con la información relevante o valores None si no se encuentra
    """
    query = f"{artist} {song}"
    try:
        results = spotify.search(q=query, type='track', limit=1)
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
        print(f"Error buscando {query}: {e}")
    return {
        'SPOTIFY_URL': None,
        'ALBUM_NAME': None,
        'ALBUM_RELEASE_DATE': None,
        'DURATION_MS': None,
        'POPULARITY': None,
        'PREVIEW_URL': None
    }

# Procesar el dataset y añadir información de Spotify
def process_dataset(input_file, output_file, wait_time=0.1, save_interval=500, max_requests=35000):
    """
    Procesa un dataset para añadir información adicional de Spotify.
    :param input_file: Ruta del archivo CSV de entrada
    :param output_file: Ruta del archivo CSV de salida
    :param wait_time: Tiempo de espera entre solicitudes (en segundos)
    :param save_interval: Número de filas procesadas antes de guardar el archivo
    :param max_requests: Número máximo de solicitudes antes de pausar
    """
    # Cargar el dataset original
    try:
        original_df = pd.read_csv(input_file)
        if os.path.exists(output_file):
            processed_df = pd.read_csv(output_file)
            print(f"Archivo de salida encontrado. Verificando filas faltantes.")
            # Identificar las filas que faltan en el archivo de salida
            merged_df = original_df.merge(processed_df, on=['ARTIST_NAME', 'SONG_NAME'], how='left', indicator=True)
            rows_to_process = merged_df[merged_df['_merge'] == 'left_only']
            rows_to_process = rows_to_process.drop(columns=['_merge'])
            print(f"Filas restantes por procesar: {len(rows_to_process)}")
            # Usar el archivo procesado como base
            df = processed_df
        else:
            print(f"Archivo de salida no encontrado. Comenzando desde cero.")
            rows_to_process = original_df
            rows_to_process['SPOTIFY_URL'] = None
            rows_to_process['ALBUM_NAME'] = None
            rows_to_process['ALBUM_RELEASE_DATE'] = None
            rows_to_process['DURATION_MS'] = None
            rows_to_process['POPULARITY'] = None
            rows_to_process['PREVIEW_URL'] = None
            df = rows_to_process.copy()
    except FileNotFoundError:
        print(f"El archivo {input_file} no se encontró. Verifica la ruta.")
        return

    if rows_to_process.empty:
        print("No hay filas para procesar.")
        return

    request_count = 0

    for index, row in rows_to_process.iterrows():
        # Obtener la información de Spotify
        spotify_data = get_spotify_data(row['ARTIST_NAME'], row['SONG_NAME'])

        # Actualizar el DataFrame
        for key, value in spotify_data.items():
            df.at[index, key] = value

        # Imprimir progreso
        print(f"[{index+1}/{len(original_df)}] Datos obtenidos: {spotify_data}")

        # Incrementar el contador de solicitudes
        request_count += 1

        # Guardar cada `save_interval` filas procesadas
        if (index + 1) % save_interval == 0:
            print(f"Guardando progreso en {output_file}...")
            df.to_csv(output_file, index=False)

        # Pausar si se alcanza el número máximo de solicitudes
        if request_count >= max_requests:
            print("Se alcanzó el límite de solicitudes. Deteniendo el proceso para evitar exceder el límite de la API.")
            break

        # Esperar antes de la siguiente solicitud
        time.sleep(wait_time)

    # Guardar el dataset actualizado al finalizar
    print(f"Guardando archivo final en {output_file}...")
    df.to_csv(output_file, index=False)
    print("Proceso completo.")

# Prueba inicial
if __name__ == "__main__":
    # Ajusta las rutas relativas desde la ubicación del script
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    input_file = os.path.join(base_dir, "data", "combined_dataset.csv")
    output_file = os.path.join(base_dir, "data", "combined_with_spotify.csv")

    # Procesar el dataset
    process_dataset(input_file, output_file, wait_time=0.1, save_interval=500, max_requests=35000)
