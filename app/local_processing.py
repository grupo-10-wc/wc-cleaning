from pathlib import Path
import unicodedata
import pdfplumber
import csv
import datetime
import logging
import hashlib
import os
import pandas as pd
import numpy as np

# Import folders from centralized config to keep a single source of truth
from app.config import (
    INPUT_FOLDER as input_folder,
    PROCESSED_FOLDER as output_folder,
    SENSORS_LOCATION_SALT,
)

output_folder.mkdir(parents=True, exist_ok=True)


def remove_accents(text: str) -> str:
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

def pdf_to_csv(input_file: Path, output_file: Path) -> None:
    logging.info("Processando PDF -> CSV: %s", input_file)
    if not Path(input_file).exists():
        logging.error("❌ Arquivo PDF não encontrado: %s", input_file)
        return

    with pdfplumber.open(input_file) as pdf, open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['aparelho', 'potencia', 'dias', 'utilizacao', 'medida', 'consumo'])
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = text.split("\n")
            lines = lines[8:]
            for line in lines:
                line = line.replace("*", "")
                line = line.replace('"', "")
                parts = line.split()
                if len(parts) < 6:
                    continue
                aparelho = remove_accents(" ".join(parts[:-5]))

                potencia = parts[-5]

                dias = parts[-4]
                utilizacao = "1" if parts[-3] == "-" else parts[-3]
                medida = "h" if parts[-2] == "-" else parts[-2]
                consumo = parts[-1]
                writer.writerow([aparelho, potencia, dias, utilizacao, medida, consumo])
    logging.info("✔ Arquivo salvo em %s", output_file)


def tratar_csv(input_file: Path, output_file: Path) -> None:
    logging.info("Processando CSV: %s", input_file)
    if not Path(input_file).exists():
        logging.error("❌ Arquivo não encontrado: %s", input_file)
        return

    try:
        df = pd.read_csv(input_file, encoding='latin1', sep=';')
    except Exception as e:
        logging.error("❌ Erro ao ler %s: %s", input_file, e)
        return
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    nomes_padronizados = [
        'DATA',
        'HORA_UTC',
        'TEMP_MAX_HORA_ANT_C',
        'TEMP_MIN_HORA_ANT_C',
        'UMID_REL_AR_PCT',
        'DIRECAO_VENTO_GRAUS',
        'VELOC_VENTO_MS'
    ]
    if len(df.columns) == len(nomes_padronizados):
        df.columns = nomes_padronizados

    def tratar_numero(valor):
        if isinstance(valor, str):
            valor = valor.strip().replace(',', '.')
            if valor.startswith('.'):
                valor = '0' + valor
            if valor.endswith('.'):
                valor = valor + '0'
            if valor == '':
                return np.nan
            try:
                return str(float(valor))
            except ValueError:
                return valor
        return valor

    df = df.map(tratar_numero)

    # Adiciona DATAHORA unificado se houver DATA e HORA_UTC (para correlação)
    if 'DATA' in df.columns and 'HORA_UTC' in df.columns:
        def _mk_dt(row):
            try:
                day = str(row['DATA']).strip()
                hour = int(float(row['HORA_UTC']))
                return datetime.datetime.strptime(f"{day} {hour:02d}", "%Y-%m-%d %H")\
                    .strftime("%Y-%m-%d %H:00:00")
            except Exception:
                return np.nan
        df['DATAHORA'] = df.apply(_mk_dt, axis=1)

    df = df.dropna()
    df.to_csv(output_file, index=False, header=True, sep=';')


def normalizar_datahora_pld(input_file: Path, output_file: Path) -> None:
    logging.info("Normalizando datas em %s", input_file)
    df = pd.read_csv(input_file, delimiter=';')
    df['DIA'] = df['DIA'].apply(lambda x: f"{int(x):02d}")
    df['HORA'] = df['HORA'].apply(lambda x: f"{int(x):02d}")
    df['DATAHORA'] = df.apply(
        lambda row: datetime.datetime.strptime(
            f"{row['MES_REFERENCIA']}{row['DIA']}{row['HORA']}", "%Y%m%d%H"
        ),
        axis=1
    )
    df.drop(columns=['MES_REFERENCIA', 'DIA', 'HORA'], inplace=True)
    df.to_csv(output_file, index=False, header=True, sep=';')
    logging.info("✔ Arquivo salvo em %s", output_file)


def process_consumo_aparelho() -> None:
    input_file = input_folder / "consumoAparelho.pdf"
    output_file = output_folder / "consumo_aparelho.csv"
    pdf_to_csv(input_file, output_file)


def process_pld() -> None:
    input_file = input_folder / "horarioPrecoDiff.csv"
    output_file = output_folder / "pld_normalizado.csv"
    if not input_file.exists():
        logging.error("❌ Arquivo não encontrado: %s", input_file)
        return
    normalizar_datahora_pld(input_file, output_file)


def process_dado_clima() -> None:
    input_file = input_folder / "clima.csv"
    output_file = output_folder / "dados_clima.csv"
    tratar_csv(input_file, output_file)


# -------------------- Sensores (dados.csv) -------------------- #

def _normalize_text(value: str) -> str:
    if isinstance(value, str):
        return value.strip()
    return value


def _anonymize_location(location: str) -> str:
    """Anonymize location using a salted SHA-256 hash (stable, non-reversible)."""
    if not isinstance(location, str) or location.strip() == "":
        return ""
    data = (SENSORS_LOCATION_SALT + "::" + location.strip()).encode("utf-8", errors="ignore")
    return hashlib.sha256(data).hexdigest()


def _parse_float(value) -> float | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    if isinstance(value, str):
        v = value.strip().replace(",", ".")
        if v == "":
            return None
        try:
            return float(v)
        except ValueError:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value) -> str | None:
    """Parse to ISO-like 'YYYY-MM-DD HH:MM:SS'. Accepts common formats."""
    if not isinstance(value, str):
        return None
    v = value.strip()
    if v == "":
        return None
    # Try several common formats
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            dt = datetime.datetime.strptime(v, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def tratar_dados_sensores(input_file: Path, output_file: Path) -> None:
    """Normalize and clean sensors data from dados.csv according to data dictionary.

    Columns expected (case-insensitive, trimmed):
      - sensor_model (text)
      - measure_unit (text)
      - device (text)
      - location (text) -> anonymized
      - data_type (text)
      - data (numeric)
      - created_at (datetime)
    Output CSV uses ';' as delimiter (consistent with other outputs).
    """
    logging.info("Processando sensores: %s", input_file)
    if not Path(input_file).exists():
        logging.error("❌ Arquivo não encontrado: %s", input_file)
        return

    # Read with tolerant settings
    try:
        df = pd.read_csv(input_file, encoding="latin1")
    except Exception:
        df = pd.read_csv(input_file, encoding="utf-8", engine="python", sep=None)

    # Normalize column names (lowercase, underscores)
    df.columns = [
        _normalize_text(c).lower().replace(" ", "_").replace("-", "_") for c in df.columns
    ]

    # Map potential variants to expected names
    col_map = {
        "sensor_model": "sensor_model",
        "measure_unit": "measure_unit",
        "device": "device",
        "location": "location",
        "data_type": "data_type",
        "tipo_dado": "data_type",
        "data": "data",
        "valor": "data",
        "created_at": "created_at",
        "timestamp": "created_at",
    }
    df.rename(columns={c: col_map.get(c, c) for c in df.columns}, inplace=True)

    expected = [
        "sensor_model",
        "measure_unit",
        "device",
        "location",
        "data_type",
        "data",
        "created_at",
    ]

    # Keep only expected columns (if present)
    present = [c for c in expected if c in df.columns]
    df = df[present].copy()

    # Clean textual columns
    for col in ["sensor_model", "measure_unit", "device", "data_type"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: _normalize_text(x))

    # Anonymize location
    if "location" in df.columns:
        df["location"] = df["location"].map(lambda x: _anonymize_location(x))

    # Numeric parsing for 'data'
    if "data" in df.columns:
        df["data"] = df["data"].map(_parse_float)

    # Datetime parsing for 'created_at'
    if "created_at" in df.columns:
        df["created_at"] = df["created_at"].map(_parse_datetime)

    # Drop rows with missing critical fields
    critical = [c for c in ["data", "created_at", "data_type"] if c in df.columns]
    if critical:
        df.dropna(subset=critical, inplace=True)

    # Output as ';' separated CSV
    df.to_csv(output_file, index=False, header=True, sep=';')
    logging.info("✔ Arquivo salvo em %s", output_file)


def process_dados_sensores() -> None:
    input_file = input_folder / "dados.csv"
    output_file = output_folder / "dados.csv"
    tratar_dados_sensores(input_file, output_file)
    # Integra normalização diretamente no arquivo principal (sem criar commons)
    try:
        df = pd.read_csv(output_file, sep=';')

        # DATAHORA para sensores = created_at (já normalizado anteriormente)
        if 'created_at' in df.columns:
            df['DATAHORA'] = df['created_at']

        # Unidades canônicas
        unit_map = {
            'kwh': 'kWh',
            'wh': 'Wh',
            'w': 'W',
            '%': 'pct',
            'v': 'V',
            '°c': 'C',
            'c': 'C',
            'a': 'A',
        }
        def _canon_unit(u):
            if not isinstance(u, str):
                return u
            key = u.strip().lower()
            return unit_map.get(key, u.strip())
        if 'measure_unit' in df.columns:
            df['unit'] = df['measure_unit'].map(_canon_unit)

        # Métricas canônicas
        metric_map = {
            'energia': 'energy',
            'energy': 'energy',
            'potência': 'power',
            'potencia': 'power',
            'power': 'power',
            'fator de potência': 'power_factor',
            'fator de potencia': 'power_factor',
            'power factor': 'power_factor',
            'tensão': 'voltage',
            'tensao': 'voltage',
            'voltage': 'voltage',
            'temperatura': 'temperature',
            'temperature': 'temperature',
            'corrente': 'current',
            'current': 'current',
        }
        def _canon_metric(m):
            if not isinstance(m, str):
                return m
            key = m.strip().lower()
            return metric_map.get(key, m.strip())
        if 'data_type' in df.columns:
            df['metric'] = df['data_type'].map(_canon_metric)

        # Regrava o arquivo principal com campos canônicos
        df.to_csv(output_file, index=False, header=True, sep=';')
        logging.info("✔ Arquivo atualizado com normalização integrada: %s", output_file)
    except Exception as e:
        logging.warning("Falha ao integrar normalização no arquivo de sensores: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    #process_consumo_aparelho()
    #process_pld()
    process_dado_clima()
    logging.info("All files processed and saved.")