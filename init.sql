CREATE TABLE IF NOT EXISTS fato_vendas (
    id_transacao VARCHAR(50) PRIMARY KEY,
    cpf_aluno VARCHAR(14),
    data_transacao DATE,
    valor DECIMAL(10, 2),
    cidade VARCHAR(100),
    estado VARCHAR(2),
    bairro VARCHAR(100),
    venda_em_feriado BOOLEAN,
    horas_assistidas DECIMAL(10, 2),
    tickets_suporte INT,
    nps_score INT
);