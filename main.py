from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os








def main():
    
    load_dotenv(".env")

    ORIGEM_ACCOUNT = os.environ["ORIGEM_ACCOUNT"]
    ORIGEM_CONTAINER = os.environ["ORIGEM_CONTAINER"]
    ORIGEM_KEY = os.environ["ORIGEM_KEY"]
    
    DESTINO_ACCOUNT = os.environ["DESTINO_ACCOUNT"]
    DESTINO_CONTAINER = os.environ["DESTINO_CONTAINER"]
    DESTINO_KEY = os.environ["DESTINO_KEY"]



    origem_client = BlobServiceClient(
        account_url=f"https://{ORIGEM_ACCOUNT}.blob.core.windows.net",
        credential=ORIGEM_KEY
    )

    destino_client = BlobServiceClient(
        account_url=f"https://{DESTINO_ACCOUNT}.blob.core.windows.net",
        credential=DESTINO_KEY
    )


    origem_container_client = origem_client.get_container_client(ORIGEM_CONTAINER)
    destino_container_client = destino_client.get_container_client(DESTINO_CONTAINER)

    blobs_origem_lista = origem_container_client.list_blobs()


    blobs_copiados: int = 0
    for blob in blobs_origem_lista:
        origem_blob_client = origem_container_client.get_blob_client(blob.name)
        
        download_stream = origem_blob_client.download_blob()
        blob_data = download_stream.readall()
        
        destino_blob_client = destino_container_client.get_blob_client(blob.name)
        destino_blob_client.upload_blob(blob_data, overwrite=True)
        
        # blob metadata
        origem_properties = origem_blob_client.get_blob_properties()
        destino_blob_client.set_blob_metadata(origem_properties.metadata)
        
        blobs_copiados += 1
        print(f"({blobs_copiados}) Blob {blob.name} copiado.")
    
    print(f"{blobs_copiados} cópias feitas")










if __name__ == "__main__":
    main()