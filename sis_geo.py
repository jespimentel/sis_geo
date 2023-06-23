#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 17 15:54:49 2023

@author: pimentel
"""
#%% Importações necessárias

import pandas as pd
import requests

#%% Configuraçõeos do usuário

caminho_dados = 'PJ Piracicaba - DrPimentel.xlsx'
api_key = 'sua-chave-aqui'

# Registros que serão georreferenciados
registro_inicial = 0
registro_final = 2000

# Definir coordenadas mínimas e máximas
lat_min, lat_max = -22.9348, -22.5374
lon_min, lon_max = -48.1953, -47.4423

#%% Leitura da base de dados e criação do dataframe principal

df = pd.read_excel(caminho_dados, header = 9)

#%% Delimitação do dataframe à localidade de interesse

df = df[df['Município'] == 'PIRACICABA']

#%% Criação do dataframe de partes

df_partes = df[['Número MP','Partes']]
df_partes = df_partes.drop_duplicates(subset=['Número MP','Partes'])
df_partes = df_partes.dropna()

# Grava o arquivo de partes
df_partes.to_csv('partes.csv', index=False)

#%% Limpeza dos dados

# Exclui registros duplicados
df = df.drop_duplicates(subset='Número MP', keep='first')
df = df.drop_duplicates(subset='Número TJ', keep='first')

# Colunas de interesse
df = df[['Número TJ', 'Número MP', 'Tipo de Procedimento','Situação', 'Delegacia',
         'Assunto','DtFatoInicial', 'Violência Doméstica', 'Município', 'Logradouro',
         'Num_Logradouro','Bairro']]

df['Latitude'] = None
df['Longitude'] = None

# Ordenação do df
df = df.sort_values(by='DtFatoInicial', ascending=False)
n_total = len(df)
print(f'N. de registros: {n_total}')

# Exclusão dos registros sem endereço
df = df.dropna(subset = 'Logradouro')
n_com_enderecos = len(df)

print(f'N. de registros com endereços: {n_com_enderecos}')

porcentagem = n_com_enderecos/n_total * 100
print(f'Aproveitamento: {porcentagem:.2f}%')

# Edição do campo "Assunto"

def edita_assunto(assunto):
    """Devolve a última parte do assunto"""
    return assunto.split('>')[-1]

# Transformações
df['Assunto'] = df['Assunto'].apply(edita_assunto)

# Substituir as ocorrências "Simples" por "Injúria" na coluna "Assunto"
df['Assunto'] = df['Assunto'].replace(' Simples', ' Injúria')


#%% Georreferenciamento dos registros selecionados

def obter_coordenadas(endereco, chave_api = api_key):
    # Formate o endereço para ser enviado na URL
    endereco_formatado = f"{endereco}".replace(' ', '+')

    # Defina a URL da API de Geocodificação do TomTom e a chave de API
    url = f'https://api.tomtom.com/search/2/geocode/{endereco_formatado}.json?key={chave_api}'

    # Faça a requisição à API do TomTom
    response = requests.get(url)

    # Analise a resposta JSON e obtenha as coordenadas geográficas
    if response.status_code == 200:
        data = response.json()
        lat = data['results'][0]['position']['lat']
        lng = data['results'][0]['position']['lon']
        return lat, lng
    else:
        return None, None

for i, row in df[registro_inicial:registro_final].iterrows():
  try:
    endereco = str(row['Logradouro']) + ', '+ str(row['Num_Logradouro']) + ', ' + \
      str(row['Bairro']) + ', ' + str(row['Município']) + ',SP'
    lat, lng = obter_coordenadas(endereco)
    df.at[i, 'Latitude'] = lat
    df.at[i, 'Longitude'] = lng
  except:
    print(f'Verifique a linha {i}')

#%% Registros dentro dos limites estabelecidos

condicao = (df['Latitude'] >= lat_min) & (df['Latitude'] <= lat_max) & \
    (df['Longitude'] >= lon_min) & (df['Longitude'] <= lon_max)

df[condicao].to_csv(f'registros_nos_limites_{registro_inicial}_{registro_final}.csv', index=False)
df.to_csv(f'registros_georreferenciados_{registro_inicial}_{registro_final}.csv', index=False)

#%% Top ocorrências

print(df['Assunto'].value_counts().sort_values(ascending = False).head(20))
