import os
import pandas as pd
from datetime import datetime

def processar_arquivos():
    raw_dir = os.path.join('raw')
    data_dir = os.path.join('data')
    os.makedirs(data_dir, exist_ok=True)

    todos_metadados = []
    todos_dados = []

    for arquivo in os.listdir(raw_dir):
        if not arquivo.endswith('.csv'):
            continue

        caminho_arquivo = os.path.join(raw_dir, arquivo)
        
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            linhas = f.readlines()

        inicio_dados = next(i for i, linha in enumerate(linhas) 
                          if linha.startswith('Data Medicao;'))

        metadados = {}
        for linha in linhas[:inicio_dados]:
            if ':' in linha:
                chave, valor = linha.split(':', 1)
                chave = chave.strip().lower().replace(' ', '_').replace('ç', 'c')
                valor = valor.strip()

                if chave in ['latitude', 'longitude', 'altitude']:
                    valor = float(valor.replace(',', '.'))
                elif chave in ['data_inicial', 'data_final']:
                    valor = datetime.strptime(valor, '%Y-%m-%d').date()
                
                metadados[chave] = valor

        todos_metadados.append(metadados)

        dados = pd.read_csv(
            caminho_arquivo,
            skiprows=inicio_dados,
            sep=';',
            decimal=',',
            parse_dates=['Data Medicao'],
            dayfirst=True,
            na_values=['null', ''],
            keep_default_na=False
        )

        dados.columns = (
            dados.columns.str.lower()
            .str.replace(' ', '_')
            .str.replace(',', '')
            .str.replace('(', '')
            .str.replace(')', '')
            .str.replace('°', '')
        )
        dados['codigo_estacao'] = metadados.get('codigo_estacao', '')
        
        todos_dados.append(dados)

    if todos_metadados:
        df_metadados = pd.DataFrame(todos_metadados)
        df_metadados.to_csv(
            os.path.join(data_dir, 'metadados.csv'),
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

    if todos_dados:
        df_dados = pd.concat(todos_dados, ignore_index=True)
        df_dados.to_csv(
            os.path.join(data_dir, 'dados.csv'),
            index=False,
            encoding='utf-8-sig',
            date_format='%Y-%m-%d'
        )

    print(f"Processamento concluído! Arquivos salvos em: {data_dir}")

if __name__ == "__main__":
    processar_arquivos()