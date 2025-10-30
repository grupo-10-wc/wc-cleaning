WC Cleaning - Raw to Trusted via S3

Visão geral
- Este projeto baixa arquivos do bucket RAW na AWS S3, processa/limpa com as funções existentes e publica os arquivos tratados no bucket TRUSTED.
- Convenção de pastas no S3:
	- RAW: as pastas são no formato YYYY-MM-DD e contêm quatro arquivos: `consumoAparelho.pdf`, `horarioPrecoDiff.csv`, `clima.csv` e `dados.csv` (sensores).
	- TRUSTED: segue a mesma convenção de pastas. As saídas do processamento são `consumo_aparelho.csv`, `pld_normalizado.csv`, `dados_clima.csv`, `dados.csv` (sensores).
- Regra de negócio:
	- Sempre baixar do dia de ontem (D-1) no RAW: `YYYY-MM-DD` de ontem.
	- Sempre salvar no dia de hoje (D) no TRUSTED: `YYYY-MM-DD` de hoje.

Requisitos
- Python 3.10+ (recomendado)
- Dependências Python:
	- listadas em `requirements.txt` (boto3, pandas, numpy, pdfplumber, etc.)

Instalação
```zsh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Configuração AWS
- Configure as credenciais via variáveis de ambiente (recomendado para desenvolvimento):
```zsh
export AWS_ACCESS_KEY_ID=SEU_KEY_ID
export AWS_SECRET_ACCESS_KEY=SEU_SECRET
export AWS_SESSION_TOKEN=SEU_SESSION_TOKEN   # se usar credenciais temporárias
export AWS_REGION=us-east-1
```
- Buckets podem ser sobrescritos via variáveis de ambiente:
```zsh
export BUCKET_RAW=raw-wattech10
export BUCKET_TRUSTED=trusted-wattech10
```

Importante: não commitar arquivos com segredos (como `.env`). Prefira perfis do AWS CLI/STS/roles em produção.

Execução
```zsh
python3 main.py
```
O fluxo executado será:
1. Baixar do RAW os arquivos de `YYYY-MM-DD` de ontem: consumoAparelho.pdf, horarioPrecoDiff.csv, clima.csv e dados.csv (sensores).
2. Processar localmente os dados usando as funções existentes (saídas em `./processed_data`).
3. Enviar os CSVs tratados para o TRUSTED em `YYYY-MM-DD` de hoje.

Layout de arquivos
- RAW (input):
	- `s3://raw-wattech10/YYYY-MM-DD/consumoAparelho.pdf`
	- `s3://raw-wattech10/YYYY-MM-DD/horarioPrecoDiff.csv`
	- `s3://raw-wattech10/YYYY-MM-DD/clima.csv`
	- `s3://raw-wattech10/YYYY-MM-DD/dados.csv`
- TRUSTED (output):
	- `s3://trusted-wattech10/YYYY-MM-DD/consumo_aparelho.csv`
	- `s3://trusted-wattech10/YYYY-MM-DD/pld_normalizado.csv`
	- `s3://trusted-wattech10/YYYY-MM-DD/dados_clima.csv`
	- `s3://trusted-wattech10/YYYY-MM-DD/dados.csv`

Arquitetura (pasta `app/`)
- `app/config.py`: configurações e convenções (buckets, região, pastas locais, datas D-1/D).
- `app/services/s3_service.py`: cliente S3 encapsulado (download/upload com logs e tolerância a falhas).
- `app/local_processing.py`: funções de processamento local (PDF->CSV, normalização PLD, tratamento clima) que leem de `INPUT_FOLDER` e escrevem em `PROCESSED_FOLDER`.
	- Inclui limpeza/normalização de sensores (`dados.csv` -> `dados_sensores.csv`):
		- Padronização de colunas: sensor_model, measure_unit, device, location (anonimizada), data_type, data (float), created_at (datetime ISO).
		- Anonimização de location via hash SHA-256 salgado (variável `SENSORS_LOCATION_SALT`).
		- Conversão robusta de números (vírgula -> ponto), parsing de datas em formatos comuns, remoção de inválidos.
- `app/pipeline.py`: orquestra o fluxo RAW (D-1) -> processamento -> TRUSTED (D).
- `app/external/`:
	- `consumoAparelho.py`: utilitário para baixar PDF público e convertê-lo para CSV.
	- `climaTempo.py`: rotina de tratamento alternativa para arquivos de clima (legado/apoio).
	- `pld.py`: rotina de leitura e normalização do PLD a partir de fonte pública.

Compatibilidade
- Arquivos sob `cleaning/` foram mantidos como “shims” que reexportam as funções dos novos módulos em `app/`.
- Isso permite a migração gradual sem quebrar imports existentes; novos desenvolvimentos devem importar de `app/`.

Notas
- Se não houver credenciais AWS no ambiente, o script apenas processa localmente (sem download/upload S3). Coloque manualmente os arquivos de entrada em `./sendToRaw/files/` se necessário.
- Os arquivos de saída sempre são gravados localmente em `./processed_data/` além do envio opcional ao S3.

Próximos passos sugeridos
- Adicionar parametrização do deslocamento de data (ex.: `DATE_OFFSET_DAYS`) para testes e reprocessamentos.
- Adicionar testes automatizados para validar os CSVs de saída em `./processed_data/`.
