import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

DATA_FOLDER = os.path.join(os.path.dirname(__file__), "..", os.getenv("DATA_FOLDER"))

def upload_csv_to_minio():

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  
    )

    if not client.bucket_exists(BUCKET_NAME):
        print(f"Bucket '{BUCKET_NAME}' não existe. Criando...")
        client.make_bucket(BUCKET_NAME)
    else:
        print(f"Bucket '{BUCKET_NAME}' já existe.")

    for root, _, files in os.walk(DATA_FOLDER):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                object_name = file  

                try:
                    client.fput_object(
                        BUCKET_NAME,
                        object_name,
                        file_path,
                        content_type="text/csv"
                    )
                    print(f"Arquivo '{file}' enviado com sucesso para o bucket '{BUCKET_NAME}'.")
                except S3Error as e:
                    print(f"Erro ao enviar o arquivo '{file}': {e}")
if __name__ == "__main__":
    upload_csv_to_minio()

#ADD UMA FUNÇÃO PARA ESPERAR DADOS NOVOS DA PASTA 
# FLINK E KAFKA SERÃO NESCESSARIOS?