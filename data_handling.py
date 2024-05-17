import calendar
from datetime import datetime
import os
import zipfile
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import os.path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1jwyftpIbYTMenHjipS5DYT-inigDHTry0SCRvynTaGs"
nome_tabela = 'Página1'
SAMPLE_RANGE_NAME = f"{nome_tabela}!A1:B12"


def get_clean_clean_erp_tables(zip_file_path):
    # Caminho para o arquivo ZIP

    # Caminho temporário para extrair os arquivos CSV
    extract_dir = 'temp_extract'

    # Criar diretório temporário
    os.makedirs(extract_dir, exist_ok=True)

    # Descompactar o arquivo ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zipf:
        zipf.extractall(extract_dir)

    # Listar os arquivos extraídos
    extracted_files = os.listdir(extract_dir)

    # Ler cada arquivo CSV em um DataFrame
    erp = pd.read_csv(os.path.join(extract_dir, extracted_files[0]), low_memory=False)

    # Remover os arquivos CSV temporários e o diretório temporário
    for file in extracted_files:
        os.remove(os.path.join(extract_dir, file))
    os.rmdir(extract_dir)

    erp['Created At'] = erp['Created At'].fillna(erp['Updated At'])
    erp['Canceled At'] = erp['Canceled At'].fillna(erp['Suspended At'])
    erp = erp.sort_values(by='Created At', ascending=False)
    erp = erp[['Plan ID', 'Shop ID', 'Created At', 'Active', 'Canceled At']]
    erp['Canceled At'] = pd.to_datetime(erp['Canceled At'], utc=True)
    erp['Canceled At'] = erp['Canceled At'].dt.tz_convert(None)
    erp['Canceled At'] = erp['Canceled At'].dt.date
    erp.drop_duplicates(subset=['Shop ID'], keep='first', inplace=True)
    erp.drop(columns=['Created At'], inplace=True)
    return erp


def get_clean_orders_table(zip_file_path='tabelas.zip'):
    extract_dir = 'temp_extract'

    # Criar diretório temporário
    os.makedirs(extract_dir, exist_ok=True)

    # Descompactar o arquivo ZIP
    with zipfile.ZipFile(zip_file_path, 'r') as zipf:
        zipf.extractall(extract_dir)

    # Listar os arquivos extraídos
    extracted_files = os.listdir(extract_dir)

    # Ler cada arquivo CSV em um DataFrame
    orders = pd.read_csv(os.path.join(extract_dir, extracted_files[1]), low_memory=False)

    # Remover os arquivos CSV temporários e o diretório temporário
    for file in extracted_files:
        os.remove(os.path.join(extract_dir, file))
    os.rmdir(extract_dir)

    orders = orders[['ID', 'Shop ID', 'Customer ID', 'Created At',
                     'Quantity', 'Total', 'Subtotal', 'Discount', ]]
    orders['Created At'] = pd.to_datetime(orders['Created At'], utc=True)
    orders.dropna(subset=['Created At'], inplace=True)
    orders['Created At'] = orders['Created At'].dt.tz_convert(None)
    orders['Created At'] = orders['Created At'].dt.date
    lojas = orders['Shop ID'].unique()
    return orders, lojas


def generate_summary(zip_file_path='tabelas.zip'):
    orders, lojas = get_clean_orders_table(zip_file_path)
    erp = get_clean_clean_erp_tables(zip_file_path)
    erp = erp.loc[erp['Shop ID'].isin(lojas)]
    summary = pd.merge(orders, erp, on='Shop ID', how='left')
    return summary


SUMMARY = generate_summary()


def safe_sum(df, column):
    if column in df.columns:
        return df[column].sum()
    else:
        return None


def safe_len(df):
    if isinstance(df, pd.DataFrame):
        return len(df)
    else:
        return None


def safe_mean(df, column):
    if column in df.columns:
        return df[column].mean()
    else:
        return None


def safe_max(df, column):
    if column in df.columns:
        return df[column].max()
    else:
        return None


def safe_min(df, column):
    if column in df.columns:
        return df[column].min()
    else:
        return None


def safe_get_by_index(df, index, column):
    if index in df.index and column in df.columns:
        return df.at[index, column]
    else:
        return None


def carregar_dados_loja(summary, shop_id):
    loja = summary[summary['Shop ID'] == shop_id].reset_index(drop=True)
    PLANO_LOJA = loja.iloc[0]['Plan ID']
    LOJA_ATIVA = loja.iloc[0]['Active']
    DATA_CANCELAMENTO = loja.iloc[0]['Canceled At']
    loja.drop(columns=['Shop ID', 'Plan ID', 'Active', 'Canceled At'], inplace=True)
    return loja, PLANO_LOJA, LOJA_ATIVA, DATA_CANCELAMENTO


def calcular_info_vendas(loja, loja_ativa, data_cancelamento):
    primeira_venda = pd.to_datetime(loja['Created At'].min())
    ultimo_dia_ativo = pd.to_datetime(data_cancelamento) if not loja_ativa else pd.to_datetime('today')
    dias_de_atividade = (ultimo_dia_ativo - primeira_venda).days
    return primeira_venda, ultimo_dia_ativo, dias_de_atividade


def gera_lojadoc(shop_id, summary=SUMMARY):
    ultimo_dia_de_venda = None

    loja = summary[summary['Shop ID'] == shop_id].reset_index(drop=True)
    loja.drop(columns=['Shop ID'], inplace=True)

    PLANO_LOJA = loja.iloc[0]['Plan ID']
    LOJA_ATIVA = loja.iloc[0]['Active']
    DATA_CANCELAMENTO = loja.iloc[0]['Canceled At']

    loja.drop(columns=['Plan ID', 'Active', 'Canceled At'], inplace=True)
    primeira_venda = pd.to_datetime(loja['Created At'].min())
    if not LOJA_ATIVA:
        ultimo_dia_ativo = pd.to_datetime(DATA_CANCELAMENTO)

    else:
        ultimo_dia_ativo = pd.to_datetime('today')

    dias_de_atividade = ultimo_dia_ativo - primeira_venda
    dias_vendendo = dias_de_atividade.days

    loja['Created At'] = pd.to_datetime(loja['Created At'])

    # Definindo o intervalo de datas
    data_inicio = primeira_venda
    data_fim = ultimo_dia_ativo

    # Criando um DataFrame que representa todos os meses no intervalo
    all_months = pd.period_range(start=data_inicio, end=data_fim, freq='M')
    df_months = pd.DataFrame(all_months, columns=['Month'])
    df_months['Month'] = df_months['Month'].astype(str)  # Convertendo de Period para string se necessário

    # Agrupando por mês e ano na coluna 'Created At' e somando a coluna 'Total'
    gmv_mensal = loja.groupby(loja['Created At'].dt.to_period('M'))[['Total', 'Quantity']].sum().reset_index()
    gmv_mensal['Created At'] = gmv_mensal['Created At'].astype(str)  # Convertendo de Period para string

    # Juntando os dados agrupados com o DataFrame de todos os meses
    gmv_mensal_completo = pd.merge(df_months, gmv_mensal, left_on='Month', right_on='Created At', how='left').fillna(0)
    gmv_mensal_completo.drop('Created At', axis=1, inplace=True)
    gmv_mensal_completo.columns = ['Mês', 'GMV', 'Produtos Vendidos']

    gmv_mensal_completo['Mês'] = pd.to_datetime(gmv_mensal_completo['Mês']).dt.to_period('M')
    gmv_mensal_completo['MoM %'] = gmv_mensal_completo['GMV'].pct_change()
    gmv_mensal_completo['MoM % - Produtos'] = gmv_mensal_completo['Produtos Vendidos'].pct_change()
    gmv_mensal_completo['Mês'] = gmv_mensal_completo['Mês'].astype(str)
    # Encontrar índices onde GMV é zero
    zero_gmv_indices = gmv_mensal_completo.index[gmv_mensal_completo['GMV'] == 0]

    # Filtrar fora os meses com GMV zero que não têm meses subsequentes com geração de receita
    to_drop = []
    for idx in zero_gmv_indices:
        subsequent_data = gmv_mensal_completo.loc[idx + 1:]  # Dados após o mês corrente
        if not subsequent_data.empty and (subsequent_data['GMV'] > 0).any():
            continue
        to_drop.append(idx)

    MESES_INATIVOS = len(to_drop)

    # Remover os meses identificados
    gmv_mensal_completo_recorte = gmv_mensal_completo.drop(to_drop)

    # Verificar o último mês com GMV zero que foi removido
    if to_drop:
        last_removed_index = to_drop[0]
        mes_cancelamento = gmv_mensal_completo.loc[last_removed_index, 'Mês']
    else:
        mes_cancelamento = None

    gmv_transposto = gmv_mensal_completo_recorte.T
    gmv_transposto.columns = gmv_transposto.iloc[0]
    gmv_transposto.drop('Mês', inplace=True)
    calculo_std = gmv_mensal_completo_recorte.copy()
    calculo_std['MoM %'] = calculo_std['MoM %'].replace([np.inf, -np.inf], 1).fillna(0)

    if not LOJA_ATIVA:
        data_cancelamento = pd.to_datetime(DATA_CANCELAMENTO)
        data_offset = data_cancelamento - pd.DateOffset(months=3)

    elif LOJA_ATIVA and MESES_INATIVOS >= 3:
        data = str(mes_cancelamento)

        # Converte a string para um objeto datetime (usando o primeiro dia do mês)
        data_obj = datetime.strptime(data, '%Y-%m')

        # Subtrai um mês
        data_modificada = data_obj - relativedelta(months=1)

        # Formata de volta para o formato 'YYYY-MM'
        data_modificada_str = data_modificada.strftime('%Y-%m')
        # Converte a string para um objeto datetime
        data_obj = datetime.strptime(data_modificada_str, '%Y-%m')
        # Obtém o primeiro dia do mês
        # Suponha que data_obj já tenha sido definido como um objeto datetime para o mês desejado
        ano = data_obj.year
        mes = data_obj.month

        # Obtém o último dia do mês
        ultimo_dia = calendar.monthrange(ano, mes)[1]
        ultimo_dia_de_venda = data_obj.replace(day=ultimo_dia)

        data_offset = pd.to_datetime(ultimo_dia_de_venda) - pd.DateOffset(months=3)
    else:
        ultimo_dia_de_venda = None
        data_offset = pd.to_datetime('today') - pd.DateOffset(months=3)

    # Remova .date() para manter data_offset como datetime64[ns]
    loja_3meses = loja[loja['Created At'] >= data_offset]

    if not ultimo_dia_de_venda:
        ultimo_dia_de_venda = ultimo_dia_ativo

    # Calcula a diferença entre as datas
    dias_de_atividade = abs((ultimo_dia_de_venda - primeira_venda).days)

    primeira_venda_d = np.datetime64(primeira_venda, 'D')
    ultimo_dia_ativo_d = np.datetime64(ultimo_dia_de_venda, 'D')

    # Agora calcule os dias úteis
    dias_uteis_vendendo = np.busday_count(primeira_venda_d, ultimo_dia_ativo_d)

    sazonal = loja.copy()
    sazonal['Dia da Semana'] = sazonal['Created At'].dt.day_name()
    sazonal_v1 = sazonal[['Total', 'Quantity', 'Dia da Semana']].groupby('Dia da Semana').sum().sort_values(by='Total',
                                                                                                            ascending=False)
    # Calculando o total da receita
    total_receita = sazonal_v1['Total'].sum()
    total_produtos = sazonal_v1['Quantity'].sum()

    # Calculando a porcentagem de receita para cada dia
    sazonal_v1['Fatia GMV'] = (sazonal_v1['Total'] / total_receita)
    sazonal_v1['Fatia Produtos'] = (sazonal_v1['Quantity'] / total_produtos)

    loja_clientes = loja.copy()
    loja_clientes['Created At'] = pd.to_datetime(loja_clientes['Created At'])
    loja_clientes.sort_values(by='Created At', ascending=True, inplace=True)
    primeiras_compras = loja_clientes.drop_duplicates(subset=['Customer ID'], keep='first')[
        ['Customer ID', 'Created At']]
    primeiras_compras.columns = ['Customer ID', 'Data da Primeira Compra']

    loja_clientes['Transcações'] = 1
    clientes = loja_clientes[['Customer ID', 'Subtotal', 'Total', 'Discount', 'Quantity', 'Transcações']].groupby(
        'Customer ID').sum()
    clientes.reset_index(inplace=True)
    clientes = pd.merge(clientes, primeiras_compras, on='Customer ID', how='left')
    clientes.sort_values(by='Subtotal', ascending=False, inplace=True)
    clientes['Tempo de Compra (Dias)'] = pd.to_datetime(ultimo_dia_de_venda) - pd.to_datetime(
        clientes['Data da Primeira Compra'])
    clientes['Tempo de Compra (Dias)'] = clientes['Tempo de Compra (Dias)'].dt.days
    clientes.drop(columns=['Data da Primeira Compra'], inplace=True)
    clientes['Ticket Médio'] = clientes['Total'] / clientes['Transcações']
    clientes['Ticket Médio - Produtos'] = clientes['Total'] / clientes['Quantity']
    clientes['Receita por Dia'] = clientes['Total'] / clientes['Tempo de Compra (Dias)']
    clientes['Período - Transações'] = clientes['Tempo de Compra (Dias)'] / clientes['Transcações']
    clientes['Período - Produtos'] = clientes['Tempo de Compra (Dias)'] / clientes['Quantity']
    melhores_8_clientes = clientes.reset_index(drop=True)[:8]

    # Calculando valores de GMV
    GMV_TOTAL = safe_sum(loja, 'Total')
    GMV_3MESES = safe_sum(loja_3meses, 'Total')
    RELACAO_TOTAL_3MESES = (GMV_3MESES + 1) / (GMV_TOTAL + 1) if GMV_TOTAL is not None else None

    # Calculando médias diárias de GMV
    GMV_MEDIO_DIARIO = GMV_TOTAL / dias_de_atividade if dias_de_atividade else None
    GMV_MEDIO_DIARIO_3_MESES = GMV_3MESES / 90 if GMV_3MESES is not None else None
    GMV_MEDIO_PER_BUSY_DAY = GMV_3MESES / dias_uteis_vendendo if dias_uteis_vendendo and GMV_3MESES is not None else None
    ULTIMA_VARIACAO_GMV = gmv_transposto.iloc[2][-1] if not gmv_transposto.empty else None

    # Calculando valores de produtos
    TOTAL_PRODUTOS = safe_sum(loja, 'Quantity')
    PRODUTOS_3MESES = safe_sum(loja_3meses, 'Quantity')
    RELACAO_PRODUTO_3MESES = (PRODUTOS_3MESES + 1) / (TOTAL_PRODUTOS + 1) if TOTAL_PRODUTOS is not None else None

    # Calculando médias de produtos
    PRODUTOS_MEDIA = TOTAL_PRODUTOS / dias_de_atividade if dias_de_atividade else None
    PRODUTOS_MEDIA_3MESES = PRODUTOS_3MESES / 90 if PRODUTOS_3MESES is not None else None
    PRODUTOS_MEDIA_PER_BUSY_DAY = PRODUTOS_3MESES / dias_uteis_vendendo if dias_uteis_vendendo and PRODUTOS_3MESES is not None else None
    ULTIMA_VARIACAO_PRODUTOS = gmv_transposto.iloc[3][-1] if not gmv_transposto.empty else None

    # Descontos e valores de clientes
    DESCONTO_TOTAL = safe_sum(loja, 'Discount')
    DESCONTO_8_CLIENTES = safe_sum(melhores_8_clientes, 'Discount')
    LTV_MEDIO = safe_mean(clientes, 'Total')
    LTV_MEDIO_8MELHORES = safe_mean(melhores_8_clientes, 'Total')
    RAZAO_8TOTAL = safe_sum(melhores_8_clientes, 'Total') / GMV_TOTAL if GMV_TOTAL else None
    STD_LTV = np.std(clientes['Total']) if 'Total' in clientes.columns else None

    # Transações e tickets médios
    TRANSACTIONS = safe_len(loja)
    TRANSACTIONS_3MESES = safe_len(loja_3meses)
    TICKET_MEDIO = GMV_TOTAL / TRANSACTIONS if TRANSACTIONS and GMV_TOTAL is not None else None
    TICKET_MEDIO_MEDIO = safe_mean(clientes, 'Ticket Médio')
    TICKET_MEDIO_PRODUTOS = GMV_TOTAL / TOTAL_PRODUTOS if TOTAL_PRODUTOS and GMV_TOTAL is not None else None
    TICKET_MEDIO_3_MESES = GMV_3MESES / TRANSACTIONS_3MESES if TRANSACTIONS_3MESES and GMV_3MESES is not None else None
    TICKET_MEDIO_PRODUTOS_3_MESES = GMV_3MESES / PRODUTOS_3MESES if PRODUTOS_3MESES and GMV_3MESES is not None else None

    # Usando valores seguros para períodos e desvios padrões
    PERIODO_MEDIO_TRANSACOES_8CLIENTES = safe_mean(melhores_8_clientes, 'Período - Transações')
    PERIODO_MEDIO_PRODUTOS_8CLIENTES = safe_mean(melhores_8_clientes, 'Período - Produtos')
    STD_PERIODO_TRANSCOES_8CLIENTES = np.std(
        melhores_8_clientes['Período - Transações']) if 'Período - Transações' in melhores_8_clientes.columns else None
    STD_PERIODO_PRODUTOS_8CLIENTES = np.std(
        melhores_8_clientes['Período - Produtos']) if 'Período - Produtos' in melhores_8_clientes.columns else None

    PERIODO_MEDIO_TRANSACOES_CLIENTES = safe_mean(clientes, 'Período - Transações')
    PERIODO_MEDIO_PRODUTOS_CLIENTES = safe_mean(clientes, 'Período - Produtos')
    STD_PERIODO_TRANSCOES_CLIENTES = np.std(
        clientes['Período - Transações']) if 'Período - Transações' in clientes.columns else None
    STD_PERIODO_PRODUTOS_CLIENTES = np.std(
        clientes['Período - Produtos']) if 'Período - Produtos' in clientes.columns else None

    # Valores diários e sazonais de GMV
    GMV_MONDAY = safe_get_by_index(sazonal_v1, 'Monday', 'Total')
    GMV_TUESDAY = safe_get_by_index(sazonal_v1, 'Tuesday', 'Total')
    GMV_WEDNESDAY = safe_get_by_index(sazonal_v1, 'Wednesday', 'Total')
    GMV_THURSDAY = safe_get_by_index(sazonal_v1, 'Thursday', 'Total')
    GMV_FRIDAY = safe_get_by_index(sazonal_v1, 'Friday', 'Total')
    GMV_SATURDAY = safe_get_by_index(sazonal_v1, 'Saturday', 'Total')
    GMV_SUNDAY = safe_get_by_index(sazonal_v1, 'Sunday', 'Total')

    # GMV relativo por dia da semana
    RELATIVE_GMV_MONDAY = safe_get_by_index(sazonal_v1, 'Monday', 'Fatia GMV')
    RELATIVE_GMV_TUESDAY = safe_get_by_index(sazonal_v1, 'Tuesday', 'Fatia GMV')
    RELATIVE_GMV_WEDNESDAY = safe_get_by_index(sazonal_v1, 'Wednesday', 'Fatia GMV')
    RELATIVE_GMV_THURSDAY = safe_get_by_index(sazonal_v1, 'Thursday', 'Fatia GMV')
    RELATIVE_GMV_FRIDAY = safe_get_by_index(sazonal_v1, 'Friday', 'Fatia GMV')
    RELATIVE_GMV_SATURDAY = safe_get_by_index(sazonal_v1, 'Saturday', 'Fatia GMV')
    RELATIVE_GMV_SUNDAY = safe_get_by_index(sazonal_v1, 'Sunday', 'Fatia GMV')

    STD_GMV_WEEKDAY = np.std(sazonal_v1['Total']) if 'Total' in sazonal_v1.columns else None

    # Outros indicadores estatísticos e ratios
    MESES_COM_GMV_ZERADO = safe_len(gmv_mensal_completo_recorte.loc[gmv_mensal_completo_recorte['GMV'] == 0])
    QUANTIDADE_MESES = len(gmv_transposto.columns)
    ALTAS_GMV_SEM_IMPULSO = safe_len(gmv_mensal_completo_recorte.loc[(gmv_mensal_completo_recorte['MoM %'] > 0) & (
            gmv_mensal_completo_recorte['MoM %'] != np.inf)])
    IMPULSOS_GMV = safe_len(gmv_mensal_completo_recorte.loc[gmv_mensal_completo_recorte['MoM %'] == np.inf])
    ALTAS_GMV = safe_len(gmv_mensal_completo_recorte.loc[gmv_mensal_completo_recorte['MoM %'] > 0])
    QUEDAS_GMV_SEM_ZERAR = safe_len(gmv_mensal_completo_recorte.loc[(gmv_mensal_completo_recorte['MoM %'] < 0) & (
            gmv_mensal_completo_recorte['MoM %'] != -1)])
    ZERAGENS_GMV = safe_len(gmv_mensal_completo_recorte.loc[gmv_mensal_completo_recorte['MoM %'] == -1])
    QUEDAS_GMV = safe_len(gmv_mensal_completo_recorte.loc[gmv_mensal_completo_recorte['MoM %'] < 0])

    RELACAO_ZERADO_TOTAL = MESES_COM_GMV_ZERADO / QUANTIDADE_MESES if QUANTIDADE_MESES else None
    RELACAO_ALTAS_TOTAL = ALTAS_GMV / QUANTIDADE_MESES if QUANTIDADE_MESES else None
    RELACAO_QUEDAS_TOTAL = QUEDAS_GMV / QUANTIDADE_MESES if QUANTIDADE_MESES else None
    RELACAO_IMPULSOS_TOTAL = IMPULSOS_GMV / QUANTIDADE_MESES if QUANTIDADE_MESES else None
    RELACAO_ALTAS_SEM_IMPULSO = ALTAS_GMV_SEM_IMPULSO / QUANTIDADE_MESES if QUANTIDADE_MESES else None
    RELACAO_QUEDAS_SEM_ZERAR = QUEDAS_GMV_SEM_ZERAR / QUANTIDADE_MESES if QUANTIDADE_MESES else None

    MAIOR_ALTA_GMV = safe_max(calculo_std, 'MoM %')
    MAIOR_QUEDA_GMV = safe_min(calculo_std, 'MoM %')
    STD_GMV_MOM = np.std(calculo_std['MoM %']) if 'MoM %' in calculo_std.columns else None
    GMV_MAXIMO_MENSAL = safe_max(gmv_mensal_completo_recorte, 'GMV')

    COMPRAS_MEDIAS = PERIODO_MEDIO_PRODUTOS_8CLIENTES / dias_de_atividade if dias_de_atividade and PERIODO_MEDIO_PRODUTOS_8CLIENTES is not None else None
    COMPRAS_MEDIAS_8MELHORES = PERIODO_MEDIO_PRODUTOS_8CLIENTES / dias_de_atividade if dias_de_atividade and PERIODO_MEDIO_PRODUTOS_8CLIENTES is not None else None

    loja_doc = {
        'Shop ID': shop_id,
        'Plano Loja': PLANO_LOJA,
        'GMV Total': GMV_TOTAL,
        'Transações': TRANSACTIONS,
        'Ticket Médio': TICKET_MEDIO,
        'Tcket Médio - Produtos': TICKET_MEDIO_PRODUTOS,
        'GMV 3meses': GMV_3MESES,
        'RELACAO Total_3meses': RELACAO_TOTAL_3MESES,
        'Última Variação Percentual de GMV': ULTIMA_VARIACAO_GMV,
        'Produtos Vendidos - Total': TOTAL_PRODUTOS,
        'Produtos Vendidos - 3 meses': PRODUTOS_3MESES,
        'Relacao Produtos_3meses': RELACAO_PRODUTO_3MESES,
        'Média - Produtos Vendidos': PRODUTOS_MEDIA,
        'Média - Produtos Vendidos 3 meses': PRODUTOS_MEDIA_3MESES,
        'Média - Produtos Vendidos por Dia Útil': PRODUTOS_MEDIA_PER_BUSY_DAY,
        'Última Variação Percentual de Produtos': ULTIMA_VARIACAO_PRODUTOS,
        'GMV Medio Diario': GMV_MEDIO_DIARIO,
        'GMV Medio Diario 3 meses': GMV_MEDIO_DIARIO_3_MESES,
        'GMV_Medio Dias Úteis': GMV_MEDIO_PER_BUSY_DAY,
        'Relação Zerado Total': RELACAO_ZERADO_TOTAL,
        'Relação Altas Total': RELACAO_ALTAS_TOTAL,
        'Relação Quedas Total': RELACAO_QUEDAS_TOTAL,
        'Relação Impulsos Total': RELACAO_IMPULSOS_TOTAL,
        'Relação Altas Sem Impulso': RELACAO_ALTAS_SEM_IMPULSO,
        'Relação Quedas Sem Zerar': RELACAO_QUEDAS_SEM_ZERAR,
        'Dias de Atividade': dias_de_atividade,
        'Dias Úteis Vendendo': dias_uteis_vendendo,
        'Meses na Base': QUANTIDADE_MESES,
        'Desconto Total': DESCONTO_TOTAL,
        'Desconto 8 Clientes': DESCONTO_8_CLIENTES,
        'LTV Medio': LTV_MEDIO,
        'LTV Medio 8 Melhores': LTV_MEDIO_8MELHORES,
        'RAZAO 8 Total': RAZAO_8TOTAL,
        'STD LTV': STD_LTV,
        'MESES COM GMV ZERADO': MESES_COM_GMV_ZERADO,
        'Ticket Médio Médio': TICKET_MEDIO_MEDIO,
        'Ticket Médio 3 meses': TICKET_MEDIO_3_MESES,
        'Ticket Médio Produtos 3 meses': TICKET_MEDIO_PRODUTOS_3_MESES,
        'Compras Médias': COMPRAS_MEDIAS,
        'Compras Médias 8 Melhores': COMPRAS_MEDIAS_8MELHORES,
        'Período Médio Transações 8 Clientes': PERIODO_MEDIO_TRANSACOES_8CLIENTES,
        'Período Médio Produtos 8 Clientes': PERIODO_MEDIO_PRODUTOS_8CLIENTES,
        'STD Período Transações 8 Clientes': STD_PERIODO_TRANSCOES_8CLIENTES,
        'STD Período Produtos 8 Clientes': STD_PERIODO_PRODUTOS_8CLIENTES,
        'Período Médio Transações Clientes': PERIODO_MEDIO_TRANSACOES_CLIENTES,
        'Período Médio Produtos Clientes': PERIODO_MEDIO_PRODUTOS_CLIENTES,
        'STD Período Transações Clientes': STD_PERIODO_TRANSCOES_CLIENTES,
        'STD Período Produtos Clientes': STD_PERIODO_PRODUTOS_CLIENTES,
        'GMV Monday': GMV_MONDAY,
        'GMV Tuesday': GMV_TUESDAY,
        'GMV Wednesday': GMV_WEDNESDAY,
        'GMV Thursday': GMV_THURSDAY,
        'GMV Friday': GMV_FRIDAY,
        'GMV Saturday': GMV_SATURDAY,
        'GMV Sunday': GMV_SUNDAY,
        'Relative GMV Monday': RELATIVE_GMV_MONDAY,
        'Relative GMV Tuesday': RELATIVE_GMV_TUESDAY,
        'Relative GMV Wednesday': RELATIVE_GMV_WEDNESDAY,
        'Relative GMV Thursday': RELATIVE_GMV_THURSDAY,
        'Relative GMV Friday': RELATIVE_GMV_FRIDAY,
        'Relative GMV Saturday': RELATIVE_GMV_SATURDAY,
        'Relative GMV Sunday': RELATIVE_GMV_SUNDAY,
        'STD GMV Weekday': STD_GMV_WEEKDAY,
        'Altas GMV Sem Impulso': ALTAS_GMV_SEM_IMPULSO,
        'Impulsos GMV': IMPULSOS_GMV,
        'Altas GMV': ALTAS_GMV,
        'Quedas GMV Sem Zerar': QUEDAS_GMV_SEM_ZERAR,
        'Zeragens GMV': ZERAGENS_GMV,
        'Quedas GMV': QUEDAS_GMV,
        'Maior Alta GMV': MAIOR_ALTA_GMV,
        'Maior Queda GMV': MAIOR_QUEDA_GMV,
        'STD GMV MoM': STD_GMV_MOM,
        'GMV Máximo Mensal': GMV_MAXIMO_MENSAL

    }

    if LOJA_ATIVA:
        loja_doc['Status'] = 'Ativa'
    else:
        loja_doc['Status'] = 'Cancelada'

    loja_doc['Data de Cancelamento'] = DATA_CANCELAMENTO

    if MESES_INATIVOS >= 3 and LOJA_ATIVA:
        loja_doc['Status'] = 'Quarentena'
        loja_doc['Data de Cancelamento - Inatividade'] = mes_cancelamento

    doc_loja_formatado = {k: round(v, 2) if isinstance(v, float) else v for k, v in loja_doc.items()}
    return doc_loja_formatado


def convert_value(value):
    if isinstance(value, np.int64):
        return int(value)
    elif isinstance(value, np.float64):
        return float(value)
    return value


def upload_to_Google_Sheets(listas_Pedrinho):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if isinstance(listas_Pedrinho, pd.DataFrame):
        # Convertendo os tipos int64 para int
        listas_Pedrinho = listas_Pedrinho.astype(int).to_dict(orient='records')
    elif isinstance(listas_Pedrinho, np.int64):
        listas_Pedrinho = int(listas_Pedrinho)

    with open("token.json", "w") as token:
        token.write(creds.to_json())

    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()

    sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                          range="A1",
                          valueInputOption='USER_ENTERED',
                          body={'values': listas_Pedrinho}).execute()
