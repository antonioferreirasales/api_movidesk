from sqlalchemy import create_engine
from configuracoes import*  # importa as variáveis do banco

# Cria engine para se conectar ao banco
engine = create_engine(f'postgresql://{db_usuario}:{db_senha}@{db_ip}:{db_porta}/{db_nome}')
