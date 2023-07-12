-- Cria a tabela para guardar informações de "analistas"
CREATE TABLE IF NOT EXISTS analistas (
	id INT NOT NULL UNIQUE PRIMARY KEY,
	nome VARCHAR(100),
	ativo BOOLEAN DEFAULT TRUE
);

-- Cria a tabela tickets
CREATE TABLE tickets (
	id INT NOT NULL UNIQUE PRIMARY KEY,
	datacriacao TIMESTAMP WITHOUT TIME ZONE,
	dataresolucao TIMESTAMP WITHOUT TIME ZONE,
	datafechamento TIMESTAMP WITHOUT TIME ZONE,
	dataabertura TIMESTAMP WITHOUT TIME ZONE,
	id_cliente INT,
	cliente VARCHAR(100),
	modulo VARCHAR(100),
	assunto VARCHAR(150),
	categoria VARCHAR(70),
	urgencia VARCHAR(50),
	status VARCHAR(70),
	duracaofila int,
	duracaochat int,
	duracaoticket int,
	pausautil int,
	pausacorrida int,
	analista VARCHAR(100),
	id_analista INT
);