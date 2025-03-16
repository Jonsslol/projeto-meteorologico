import os
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileCreatedEvent
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"), override= True)
print("BUCKET_NAME carregado:", os.getenv("BUCKET_NAME"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATA_FOLDER = Path(
    os.path.join(os.path.dirname(__file__), "..", "..", os.getenv("DATA_FOLDER"))
).resolve()


class MinIOUploader(FileSystemEventHandler):
    def __init__(self, client):
        self.client = client
        self.uploaded_files = set()

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            file_path = Path(event.src_path)
            object_name = file_path.name
            
            if object_name not in self.uploaded_files:
                try:
                    self.client.fput_object(
                        BUCKET_NAME,
                        object_name,
                        str(file_path),
                        content_type="text/csv"
                    )
                    print(f"Arquivo '{object_name}' enviado!")
                    self.uploaded_files.add(object_name)
                except S3Error as e:
                    print(f"Erro: {e}")

def initialize_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
        region='us-east-1'
    )
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    return client

def main():
    
    if not DATA_FOLDER.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {DATA_FOLDER}")
    
    print(f"Pasta monitorada: {DATA_FOLDER}")

    if not DATA_FOLDER.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {DATA_FOLDER}")
    
    client = initialize_minio()
    event_handler = MinIOUploader(client)
    
    for file in DATA_FOLDER.glob("*.csv"):
        event = FileCreatedEvent(str(file))
        event_handler.on_created(event)
    
    observer = Observer()
    observer.schedule(event_handler, str(DATA_FOLDER), recursive=True) 
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

print(f"Caminho absoluto da pasta: {DATA_FOLDER}")


if __name__ == "__main__":
    main()