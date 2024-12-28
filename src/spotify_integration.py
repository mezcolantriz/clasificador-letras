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

# Función para buscar la URL de Spotify
def get_spotify_url(artist, song):
    query = f"{artist} {song}"
    try:
        results = spotify.search(q=query, type='track', limit=1)
        tracks = results.get('tracks', {}).get('items', [])
        if tracks:
            return tracks[0]['external_urls']['spotify']
    except Exception as e:
        print(f"Error buscando {query}: {e}")
        return None
    return None

# Procesar el dataset y añadir URLs de Spotify
def process_dataset(input_file, output_file, wait_time=0.2):
    """
    Continúa procesando un dataset, ignorando las filas ya procesadas.
    :param input_file: Ruta del archivo CSV de entrada.
    :param output_file: Ruta del archivo CSV de salida.
    :param wait_time: Tiempo de espera entre solicitudes (en segundos).
    """
    # Cargar el dataset actual
    try:
        df = pd.read_csv(output_file)
        print("Archivo existente cargado. Continuando...")
    except FileNotFoundError:
        # Si el archivo de salida no existe, empezar desde el original
        df = pd.read_csv(input_file)
        print("Archivo de salida no encontrado. Empezando desde el original.")

    # Filtrar filas sin procesar
    df_to_process = df[df['SPOTIFY_URL'].isnull()]

    print(f"Filas restantes por procesar: {len(df_to_process)}")

    # Iterar sobre las filas no procesadas
    for index, row in df_to_process.iterrows():
        # Obtener la URL de Spotify
        url = get_spotify_url(row['ARTIST_NAME'], row['SONG_NAME'])
        df.at[index, 'SPOTIFY_URL'] = url  # Actualizar en el DataFrame original
        print(f"[{index+1}/{len(df)}] URL obtenida: {url}")

        # Esperar antes de la siguiente solicitud
        time.sleep(wait_time)

    # Guardar el dataset actualizado
    df.to_csv(output_file, index=False)
    print(f"Dataset actualizado con URLs de Spotify guardado en {output_file}")

# Prueba inicial
if __name__ == "__main__":
    # Ajusta las rutas según tu estructura
    input_file = r"C:\Users\solan\Downloads\clasificador-letras\data\combined_dataset.csv"
    output_file = r"C:\Users\solan\Downloads\clasificador-letras\data\combined_with_spotify.csv"

    # Procesar el dataset
    process_dataset(input_file, output_file, wait_time=0.2)
