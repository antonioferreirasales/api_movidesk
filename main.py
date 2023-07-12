import requests  # biblioteca para realizar requisições à uma API
import json  # biblioteca para tratar arquivos do tipo 'json'
import pandas as pd  # biblioteca para tratar os dados
from psycopg2 import sql  # para conectar com o banco de dados
from psycopg2.errors import UniqueViolation, IntegrityError
import sqlalchemy  # para inserir os dados
from configuracoes import token  # importa o token
from conexao import *
import time  # para lidar com timing
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)
session = Session()

# Criação da API
tipo = 'tickets'
select = 'id,category,subject,urgency,baseStatus,ownerTeam,lifetimeWorkingTime,chatTalkTime,chatWaitingTime,' \
         'stoppedTime,stoppedTimeWorkingTime,createdDate,resolvedIn,closedIn,reopenedIn'
filters = "(chatWidget eq  'Unidade - VR Fortaleza')"
expand = 'owner($select=id,businessName),createdBy($select=id,businessName)'
skip = 0
top = 10
orderBy = 'id asc'

tabela = 'tickets'  # define nome da tabela no banco de dados

# Função para dropar colunas desnecessárias do dataframe caso existam
def drop_column_if_exists(df, nome_coluna):
    if nome_coluna in df.columns:
        df = df.drop(nome_coluna, axis=1)
    return df


while True:
    try:
        api_url = f'https://api.movidesk.com/public/v1/' \
                  f'{tipo}?token={token}&$select={select}&$filter={filters}&$expand={expand}&' \
                  f'$skip={skip}&$top={top}&$orderby={orderBy}'
        respostas = requests.get(api_url, timeout=3)
        if respostas.status_code == 200:
            print(f'O valor pulado é de {skip}')
            json = respostas.json()
            # normaliza a semi-estrutura do arquivo JSON em uma única tabela
            tickets = pd.json_normalize(json)
            # dropa coluna 'owner'
            tickets = drop_column_if_exists(tickets, 'owner')
            # noinspection SpellCheckingInspection
            tickets = tickets.rename(
                columns={'reopenedIn': 'dataabertura', 'closedIn': 'datafechamento', 'resolvedIn': 'dataresolucao',
                         'createdDate': 'datacriacao', 'stoppedTimeWorkingTime': 'pausautil',
                         'stoppedTime': 'pausacorrida',
                         'chatWaitingTime': 'duracaofila', 'chatTalkTime': 'duracaochat',
                         'lifeTimeWorkingTime': 'duracaoticket', 'ownerTeam': 'modulo', 'baseStatus': 'status',
                         'urgency': 'urgencia', 'subject': 'assunto', 'category': 'categoria',
                         'createdBy.businessName': 'cliente', 'createdBy.id': 'id_cliente',
                         'owner.businessName': 'analista', 'owner.id': 'id_analista'})
            # Consulta os id existentes no banco
            query = text(f"SELECT COUNT(id) FROM tickets")
            with engine.connect() as conn:
                resultado = conn.execute(query)
            # Armazena resultado na variável skip
            skip = resultado.fetchone()[0]
            # Verifica os id duplicados no dataframe e os remove
            id_existentes = pd.read_sql_query('SELECT id FROM tickets', engine)
            tickets = tickets[~tickets['id'].isin(id_existentes['id'])]
            # Insere dataframe  na tabela tickets
            tickets.to_sql(tabela, engine, if_exists='append', index=False)
            # Fecha a conexão
            engine.dispose()
            session.close()
            time.sleep(60)  # espera 60 segundos
        else:
            erro = respostas.raise_for_status()
            print(f'Ocorreu o seguinte erro no acesso da API: {erro}')
    except UniqueViolation as e:
        print("Erro de violação de chave única:", e)
        skip += skip
        continue
    except IntegrityError as e:
        print("Erro de violação de chave única:", e)
        skip += skip
        continue
