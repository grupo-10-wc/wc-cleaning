import os
from pathlib import Path
from datetime import date, timedelta

# Buckets and region (overridable via environment variables)
BUCKET_RAW = os.environ.get("BUCKET_RAW", "raw-wattech10")
BUCKET_TRUSTED = os.environ.get("BUCKET_TRUSTED", "trusted-wattech10")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
SENSORS_LOCATION_SALT = os.environ.get("SENSORS_LOCATION_SALT", "change-me-salt")

# Local folders used by the existing cleaning functions (overridable via env)
INPUT_FOLDER = Path(os.environ.get("INPUT_FOLDER", "./sendToRaw/files/"))
INPUT_FOLDER.mkdir(parents=True, exist_ok=True)
PROCESSED_FOLDER = Path("./processed_data")
PROCESSED_FOLDER.mkdir(parents=True, exist_ok=True)

# Constants for expected input and output filenames
RAW_FILENAMES = (
    "consumoAparelho.pdf",
    "horarioPrecoDiff.csv",
    "clima.csv",
    "dados.csv",  # sensores
)

PROCESSED_FILENAMES = (
    "consumo_aparelho.csv",
    "pld_normalizado.csv",
    "dados_clima.csv",
    "dados.csv",
)


def yesterday_folder() -> str:
    """Return folder name for yesterday in format YYYY-MM-DD."""
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def today_folder() -> str:
    """Return folder name for today in format YYYY-MM-DD."""
    return date.today().strftime("%Y-%m-%d")
