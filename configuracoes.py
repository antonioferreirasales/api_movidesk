import configparser

config = configparser.RawConfigParser()
config.read('config.properties')
token = config.get('API', 'token')
db_usuario = config.get('BANCO', 'db.usuario')
db_senha = config.get('BANCO', 'db.senha')
db_ip = config.get('BANCO', 'db.ip')
db_porta = config.get('BANCO', 'db.porta')
db_nome = config.get('BANCO', 'db.nome')
