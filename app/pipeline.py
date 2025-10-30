import logging
from pathlib import Path

from app.config import (
    BUCKET_RAW,
    BUCKET_TRUSTED,
    INPUT_FOLDER,
    PROCESSED_FOLDER,
    RAW_FILENAMES,
    PROCESSED_FILENAMES,
    yesterday_folder,
    today_folder,
)
from app.services.s3_service import S3Service

# Import existing processing functions (keep behavior and outputs unchanged)
from app.local_processing import (
    process_consumo_aparelho,
    process_pld,
    process_dado_clima,
    process_dados_sensores,
)


def _download_raw_files(s3: S3Service) -> None:
    """Download yesterday's RAW files to local INPUT_FOLDER."""
    y_folder = yesterday_folder()
    mapping = {
        f"{y_folder}/consumoAparelho.pdf": INPUT_FOLDER / "consumoAparelho.pdf",
        f"{y_folder}/horarioPrecoDiff.csv": INPUT_FOLDER / "horarioPrecoDiff.csv",
        f"{y_folder}/clima.csv": INPUT_FOLDER / "clima.csv",
        f"{y_folder}/dados.csv": INPUT_FOLDER / "dados.csv",
    }
    logging.info("Iniciando download dos arquivos RAW do bucket %s (pasta %s)", BUCKET_RAW, y_folder)
    for key, local_path in mapping.items():
        s3.download_file(BUCKET_RAW, key, local_path)


essential_processors = (
    process_consumo_aparelho,
    process_pld,
    process_dado_clima,
    process_dados_sensores,
)


def _process_local_files() -> None:
    logging.info("Iniciando processamento local dos arquivos")
    for fn in essential_processors:
        try:
            fn()
        except Exception as e:
            logging.error("Falha ao processar com %s: %s — continuando com as próximas etapas", getattr(fn, "__name__", str(fn)), e)
            # continuidade: não interrompe o pipeline inteiro


def _upload_processed_files(s3: S3Service) -> None:
    t_folder = today_folder()
    outputs = [
        (PROCESSED_FOLDER / "consumo_aparelho.csv", f"{t_folder}/consumo_aparelho.csv"),
        (PROCESSED_FOLDER / "pld_normalizado.csv", f"{t_folder}/pld_normalizado.csv"),
        (PROCESSED_FOLDER / "dados_clima.csv", f"{t_folder}/dados_clima.csv"),
        (PROCESSED_FOLDER / "dados.csv", f"{t_folder}/dados.csv"),
    ]
    logging.info("Enviando arquivos processados para o bucket %s (pasta %s)", BUCKET_TRUSTED, t_folder)
    for local_path, key in outputs:
        if local_path.exists():
            s3.upload_file(local_path, BUCKET_TRUSTED, key)
        else:
            logging.warning("Arquivo processado não encontrado, pulando upload: %s", local_path)


def run() -> None:
    """End-to-end pipeline: RAW (D-1) -> local processing -> TRUSTED (D)."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    s3 = S3Service()
    _download_raw_files(s3)
    _process_local_files()
    _upload_processed_files(s3)
