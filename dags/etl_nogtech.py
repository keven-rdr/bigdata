from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import requests
import re
import numpy as np

# ==========================================
# FUNÇÕES AUXILIARES DE TRANSFORMAÇÃO
# ==========================================

def limpar_mascarar_cpf(cpf):
    """Remove pontuação e aplica máscara LGPD"""
    cpf_limpo = re.sub(r'\D', '', str(cpf)).zfill(11)
    return f"*.{cpf_limpo[3:6]}.{cpf_limpo[6:9]}-**"

def buscar_cep(cep, cache_cep):
    """Busca CEP na BrasilAPI utilizando cache local"""
    cep_limpo = re.sub(r'\D', '', str(cep))
    if not cep_limpo or cep_limpo == 'nan':
        return {'cidade': None, 'estado': None, 'bairro': None}
        
    if cep_limpo in cache_cep:
        return cache_cep[cep_limpo]
    
    try:
        response = requests.get(f"https://brasilapi.com.br/api/cep/v2/{cep_limpo}", timeout=5)
        if response.status_code == 200:
            data = response.json()
            resultado = {
                'cidade': data.get('city'),
                'estado': data.get('state'),
                'bairro': data.get('neighborhood')
            }
            cache_cep[cep_limpo] = resultado
            return resultado
    except Exception as e:
        print(f"Erro ao buscar CEP {cep_limpo}: {e}")
    
    return {'cidade': None, 'estado': None, 'bairro': None}

def verificar_feriado(data_str, cache_feriados):
    """Verifica se a data é feriado consultando a BrasilAPI por ano"""
    if pd.isna(data_str):
        return False
        
    data_obj = pd.to_datetime(data_str)
    ano = str(data_obj.year)
    data_formatada = data_obj.strftime('%Y-%m-%d')

    if ano not in cache_feriados:
        try:
            response = requests.get(f"https://brasilapi.com.br/api/feriados/v1/{ano}", timeout=5)
            if response.status_code == 200:
                cache_feriados[ano] = [f['date'] for f in response.json()]
            else:
                cache_feriados[ano] = []
        except Exception as e:
            print(f"Erro ao buscar feriados de {ano}: {e}")
            cache_feriados[ano] = []

    return data_formatada in cache_feriados[ano]

# ==========================================
# TASKS DA DAG
# ==========================================

def extract_and_transform(**kwargs):
    # 1. Extração Local
    df_transacoes = pd.read_csv('/opt/airflow/data/transacoes_nogtech.csv', sep=';', encoding='latin-1')
    df_engajamento = pd.read_json('/opt/airflow/data/engajamento_alunos.json', encoding='utf-8')

    # Correção do valor: 49,90 -> 49.90 (Float) e renomeia a coluna
    df_transacoes['valor'] = df_transacoes['valor_brl'].astype(str).str.replace(',', '.').astype(float)

    # Tratamento MISTO de datas: O Pandas vai descobrir sozinho qual padrão é em cada linha
    data_convertida = pd.to_datetime(df_transacoes['data_transacao'], format='mixed', dayfirst=True)
    df_transacoes['mes_ref'] = data_convertida.dt.strftime('%Y-%m')
    df_transacoes['data_transacao'] = data_convertida.dt.strftime('%Y-%m-%d') 

    # Converte a data do JSON
    df_engajamento['mes_ref'] = pd.to_datetime(df_engajamento['mes_referencia']).dt.strftime('%Y-%m')

    # Limpa CPF em ambos para garantir que o JOIN funcione 100%
    df_transacoes['cpf_limpo'] = df_transacoes['cpf_aluno'].apply(lambda x: re.sub(r'\D', '', str(x)).zfill(11))
    df_engajamento['cpf_limpo'] = df_engajamento['cpf_aluno'].apply(lambda x: re.sub(r'\D', '', str(x)).zfill(11))

    # 2. Transformação: Left Join
    df_final = pd.merge(
        df_transacoes, 
        df_engajamento, 
        how='left', 
        on=['cpf_limpo', 'mes_ref']
    )

    # 3. LGPD: Mascarar CPF e remover Nome
    df_final['cpf_aluno'] = df_final['cpf_limpo'].apply(limpar_mascarar_cpf)
    if 'nome_aluno' in df_final.columns:
        df_final = df_final.drop(columns=['nome_aluno'])

    # 4. Enriquecimento: BrasilAPI (Cache em Memória)
    cache_cep = {}
    cache_feriados = {}
    
    ceps_enriquecidos = df_final['cep_cobranca'].apply(lambda x: buscar_cep(x, cache_cep))
    df_final['cidade'] = ceps_enriquecidos.apply(lambda x: x['cidade'])
    df_final['estado'] = ceps_enriquecidos.apply(lambda x: x['estado'])
    df_final['bairro'] = ceps_enriquecidos.apply(lambda x: x['bairro'])

    df_final['venda_em_feriado'] = df_final['data_transacao'].apply(lambda x: verificar_feriado(x, cache_feriados))

    # Selecionar colunas finais baseadas no JSON real
    colunas_finais = [
        'id_transacao', 'cpf_aluno', 'data_transacao', 'valor', 'cidade', 'estado', 
        'bairro', 'venda_em_feriado', 'horas_assistidas', 'tickets_suporte', 'nps_score'
    ]
    
    # Garantir que colunas faltantes (por causa do left join) sejam criadas
    for col in ['horas_assistidas', 'tickets_suporte', 'nps_score']:
        if col not in df_final.columns:
            df_final[col] = np.nan

    df_final = df_final[colunas_finais]

    # Substituir NaN do Pandas por None para o PostgreSQL aceitar nulos perfeitamente
    df_final = df_final.replace({np.nan: None})

    df_final.to_csv('/opt/airflow/data/dados_transformados.csv', index=False)


def load_data(**kwargs):
    df = pd.read_csv('/opt/airflow/data/dados_transformados.csv')
    df = df.replace({np.nan: None})

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # UPSERT atualizado com as colunas reais do JSON
    upsert_query = """
        INSERT INTO fato_vendas (
            id_transacao, cpf_aluno, data_transacao, valor, cidade, estado, bairro, 
            venda_em_feriado, horas_assistidas, tickets_suporte, nps_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id_transacao) DO UPDATE SET
            cidade = EXCLUDED.cidade,
            estado = EXCLUDED.estado,
            bairro = EXCLUDED.bairro,
            venda_em_feriado = EXCLUDED.venda_em_feriado,
            horas_assistidas = EXCLUDED.horas_assistidas,
            tickets_suporte = EXCLUDED.tickets_suporte,
            nps_score = EXCLUDED.nps_score;
    """

    for _, row in df.iterrows():
        cursor.execute(upsert_query, (
            row['id_transacao'], row['cpf_aluno'], row['data_transacao'], 
            row['valor'], row['cidade'], row['estado'], row['bairro'], 
            row['venda_em_feriado'], row['horas_assistidas'], row['tickets_suporte'], row['nps_score']
        ))

    conn.commit()
    cursor.close()
    conn.close()

# ==========================================
# DEFINIÇÃO DA DAG
# ==========================================

default_args = {
    'owner': 'engenharia',
    'start_date': datetime(2026, 6, 8),
    'retries': 1, 
}

with DAG(
    'nogtech_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Pipeline ETL da NogTech com integrações BrasilAPI'
) as dag:

    task_transform = PythonOperator(
        task_id='extract_and_transform_data',
        python_callable=extract_and_transform
    )

    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data
    )

    task_transform >> task_load