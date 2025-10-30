import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


BUCKET_RAW = os.environ.get("BUCKET_RAW", "raw-wattech10")
BUCKET_TRUSTED = os.environ.get("BUCKET_TRUSTED", "trusted-wattech10")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

INPUT_FOLDER = Path("./sendToRaw/files/")
from dotenv import load_dotenv
load_dotenv()

from app.pipeline import run


if __name__ == "__main__":
    run()