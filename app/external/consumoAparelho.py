import requests
import pdfplumber
import csv
import unicodedata


def download_pdf(url, filename="document.pdf"):
    response = requests.get(url)
    with open(filename, "wb") as f:
        f.write(response.content)
    return filename


def remove_accents(text):
    normalized = unicodedata.normalize('NFD', text)
    return ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')


def pdf_to_csv(pdf_path, csv_path="consumoAparelho.csv"):
    with pdfplumber.open(pdf_path) as pdf, open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
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


if __name__ == "__main__":
    url = "https://igce.rc.unesp.br/Home/ComissaoSupervisora-old/ConservacaodeEnergiaCICE/tabela_consumo.pdf"
    pdf_file = download_pdf(url)
    pdf_to_csv(pdf_file)
