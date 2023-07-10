import requests  # biblioteca para realizar requisições à uma API
import json  # biblioteca para tratar arquivos do tipo 'json'
import pandas as pd  # biblioteca para tratar os dados
import psycopg2  # para conectar com o banco de dados
import sqlalchemy  # para inserir os dados
from configuracoes import token  # importa o token
from conexao import *

# Criação da API
tipo = 'tickets'
select = 'id,category,subject,urgency,baseStatus,ownerTeam,lifetimeWorkingTime,chatTalkTime,chatWaitingTime,' \
         'stoppedTime,stoppedTimeWorkingTime,createdDate,resolvedIn,closedIn,reopenedIn'
filters = "(chatWidget eq  'Unidade - VR Fortaleza')"
expand = 'owner($select=id,businessName),createdBy($select=id,businessName)'
skip = 2000
top = 1000
orderBy = 'id asc'


# Função para dropar colunas desnecessárias do dataframe caso existam
def drop_column_if_exists(df, nome_coluna):
    if nome_coluna in df.columns:
        df.drop(nome_coluna, axis=1)


while True:
    try:
        api_url = f'https://api.movidesk.com/public/v1/' \
                  f'{tipo}?token={token}&$select={select}&$filter={filters}&$expand={expand}&' \
                  f'$skip={skip}&$top={top}&$orderby={orderBy}'
        respostas = requests.get(api_url, timeout=3)
        if respostas.status_code == 200:
            json = respostas.json()
            # normaliza a semi-estrutura do arquivo JSON em uma única tabela
            tickets = pd.json_normalize(json)
            drop_column_if_exists(tickets, 'owner')
            tickets = tickets.rename(
                columns={'reopenedIn': 'dataabertura', 'closedIn': 'datafechamento', 'resolvedIn': 'dataresolucao',
                         'createdDate': 'datacriacao', 'stoppedTimeWorkingTime': 'pausautil',
                         'stoppedTime': 'pausacorrida',
                         'chatWaitingTime': 'duracaofila', 'chatTalkTime': 'duracaochat',
                         'lifeTimeWorkingTime': 'duracaoticket', 'ownerTeam': 'modulo', 'baseStatus': 'status',
                         'urgency': 'urgencia', 'subject': 'assunto', 'category': 'categoria',
                         'createdBy.businessName': 'cliente', 'createdBy.id': 'id_cliente',
                         'owner.businessName': 'analista', 'owner.id': 'id_analista'})
            # Insere dataframe  na tabela tickets
            tabela = 'tickets'
            tickets.to_sql(tabela, engine, if_exists='append', index=False)
            # Fecha a conexão
            engine.dispose()
            skip = skip + 1000
        else:
            erro = respostas.raise_for_status()
            print(f'Ocorreu o seguinte erro no acesso da API: {erro}')

    except Exception as erro:
        print(f'Ocorreu o seguinte erro na execução do código: {erro}')
