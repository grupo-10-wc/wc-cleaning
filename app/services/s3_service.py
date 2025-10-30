import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

from app.config import AWS_REGION


class S3Service:
    """Thin wrapper around boto3 S3 client with helpful logging."""

    def __init__(self, region_name: str = AWS_REGION) -> None:
        self._client = None
        try:
            self._client = boto3.client("s3", region_name=region_name)
        except Exception as e:
            logging.warning("Não foi possível criar cliente S3: %s", e)
            self._client = None

    @property
    def available(self) -> bool:
        return self._client is not None

    def download_file(self, bucket: str, key: str, local_path: Path) -> bool:
        if not self.available:
            logging.info("S3 indisponível — pulando download: s3://%s/%s", bucket, key)
            return False
        try:
            logging.info("Baixando s3://%s/%s -> %s", bucket, key, local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._client.download_file(bucket, key, str(local_path))
            logging.info("Baixado: %s", local_path)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                logging.warning("Arquivo não encontrado no S3: s3://%s/%s", bucket, key)
            else:
                logging.error("Erro ao baixar s3://%s/%s: %s", bucket, key, e)
            return False
        except Exception as e:
            logging.error("Erro inesperado ao baixar %s: %s", key, e)
            return False

    def upload_file(self, local_path: Path, bucket: str, key: str) -> bool:
        if not self.available:
            logging.info("S3 indisponível — pulando upload: %s -> s3://%s/%s", local_path, bucket, key)
            return False
        try:
            logging.info("Enviando %s -> s3://%s/%s", local_path, bucket, key)
            self._client.upload_file(str(local_path), bucket, key)
            logging.info("Enviado: s3://%s/%s", bucket, key)
            return True
        except Exception as e:
            logging.error("Falha ao enviar %s para s3://%s/%s: %s", local_path, bucket, key, e)
            return False
