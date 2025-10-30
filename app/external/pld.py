import datetime
import requests
import pandas as pd
from io import StringIO


def read_from_raw(save_date: datetime.datetime = datetime.datetime.now()):
    res = requests.get("https://pda-download.ccee.org.br/korJMXwpSLGyVlpRMQWduA/content")
    res.raise_for_status()
    texto = res.text
    return texto


def normalizar_pld(texto_csv: str = None):
    if texto_csv is None:
        texto_csv = read_from_raw()
    df = pd.read_csv(StringIO(texto_csv), delimiter=';')
    df['DIA'] = df['DIA'].apply(lambda x: f"{x:02.0f}")
    df['HORA'] = df['HORA'].apply(lambda x: f"{x:02.0f}")
    df['DATAHORA'] = df.apply(
        lambda row: datetime.datetime.strptime(f"{row['MES_REFERENCIA']}{row['DIA']}{row['HORA']}", "%Y%m%d%H"),
        axis=1
    )
    df.drop(columns=['MES_REFERENCIA', 'DIA', 'HORA', 'PERIODO_COMERCIALIZACAO'], inplace=True)
    return df


if __name__ == "__main__":
    normalizar_pld()
