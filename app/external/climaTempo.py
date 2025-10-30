import pandas as pd
import os


def tratar_csv(nome_arquivo, pasta_saida='trusted'):
    os.makedirs(pasta_saida, exist_ok=True)
    df = pd.read_csv(nome_arquivo, skiprows=8, encoding='latin1', sep=';')

    indices_remover = list(range(2, 8)) + [8, 11, 12, 13, 14, 17]
    cols_to_drop = [df.columns[i] for i in indices_remover if i < len(df.columns)]
    df = df.drop(cols_to_drop, axis=1)
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
    else:
        pass

    def tratar_numero(valor):
        if isinstance(valor, str):
            valor = valor.strip()
            valor = valor.replace(',', '.')
            if valor.startswith('.'):  
                valor = '0' + valor
            if valor.endswith('.'):
                valor = valor + '0'
            if valor == '':
                return float('nan')
            try:
                return str(float(valor))
            except ValueError:
                return valor
        return valor

    df = df.applymap(tratar_numero)
    df = df.dropna()

    nome_base = os.path.basename(nome_arquivo)
    nome_saida = nome_base.replace('.CSV', '_trusted.csv')
    caminho_saida = os.path.join(pasta_saida, nome_saida)

    df.to_csv(caminho_saida, index=False, header=True, sep=';')
    return caminho_saida
