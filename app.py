from data_handling import *
import warnings
import joblib

warnings.filterwarnings("ignore")


def handler():
    COLUNAS_DROPAR_MODELO = [
        'Plano Loja',
        'Data de Cancelamento',
        'Data de Cancelamento - Inatividade',
    ]

    COLUNAS_PARA_ZERO_INPUT = ['STD Período Produtos Clientes',
                               'LTV Medio',
                               'LTV Medio 8 Melhores',
                               'STD LTV',
                               'Ticket Médio Médio',
                               'Compras Médias',
                               'Compras Médias 8 Melhores',
                               'STD Período Transações 8 Clientes',
                               'STD Período Produtos 8 Clientes',
                               'Período Médio Transações Clientes',
                               'Período Médio Produtos Clientes',
                               'STD Período Transações Clientes',
                               'Última Variação Percentual de GMV',
                               'Última Variação Percentual de Produtos',
                               'Ticket Médio 3 meses',
                               'Ticket Médio Produtos 3 meses',
                               ]
    COLUNAS_GMV_SAZONAL = ['GMV Sunday',
                           'Relative GMV Sunday',
                           'GMV Saturday',
                           'Relative GMV Saturday',
                           'GMV Friday',
                           'Relative GMV Friday',
                           'GMV Monday',
                           'Relative GMV Monday',
                           'GMV Tuesday',
                           'Relative GMV Tuesday',
                           'GMV Thursday',
                           'Relative GMV Thursday',
                           'GMV Wednesday',
                           'Relative GMV Wednesday']

    SUMMARY = generate_summary()
    shop_ids = list(SUMMARY['Shop ID'].unique())
    erp = get_clean_clean_erp_tables()

    lojas = {}
    for i in range(len(shop_ids)):
        lojas[i] = gera_lojadoc(shop_ids[i])
    db = pd.DataFrame(lojas).T
    db.index = db['Shop ID']
    db.drop(columns=['Shop ID'], inplace=True)

    for_model = db.drop(columns=COLUNAS_DROPAR_MODELO)
    for_model[COLUNAS_PARA_ZERO_INPUT] = for_model[COLUNAS_PARA_ZERO_INPUT].fillna(0)
    for_model['Período Médio Transações 8 Clientes'].fillna(for_model['Período Médio Transações 8 Clientes'].max(),
                                                            inplace=True)
    for_model['Período Médio Produtos 8 Clientes'].fillna(for_model['Período Médio Produtos 8 Clientes'].max(),
                                                          inplace=True)
    for_model[COLUNAS_GMV_SAZONAL] = for_model[COLUNAS_GMV_SAZONAL].fillna(0)
    for_model = pd.merge(for_model, erp[['Shop ID', 'Active']], on='Shop ID', how='left')
    for_model.drop(columns=['Status'], inplace=True)
    for_model['Active'] = for_model['Active'].astype(int)
    # Ensure 'GMV Total' is in float format
    for_model['GMV Total'] = for_model['GMV Total'].astype(float)
    # Apply the logarithmic transformation
    for_model['GMV_ln'] = np.log(for_model['GMV Total'] + 1)
    # Create bins and categorize
    ln_bins = np.linspace(for_model['GMV_ln'].min(), for_model['GMV_ln'].max() + 1, 6)
    for_model['GMV_Categorico'] = pd.cut(for_model['GMV_ln'], bins=ln_bins, labels=False)
    for_model['GMV_Categorico'].fillna(0, inplace=True)
    for_model.reset_index(inplace=True)
    for_model.replace(np.inf, 1, inplace=True)

    for_model.index = for_model['Shop ID']
    for_model.drop(columns=['index', 'Shop ID', 'GMV_Categorico', 'GMV_ln'], inplace=True)
    for_model_X = for_model.drop(columns=['Active'])
    for_model.reset_index(inplace=True)

    rnd_clf = joblib.load('random_forest_model.joblib')
    probas = pd.DataFrame(rnd_clf.predict_proba(for_model_X.values),
                          columns=['Probabilidade de Cancelamento', 'Probabilidade de Ativação'])

    Pedrinho = pd.concat([for_model, probas], axis=1)

    to_upload = Pedrinho.values.tolist()
    converted_data = [[convert_value(item) for item in sublist] for sublist in to_upload]
    colunas = list(Pedrinho.columns)
    converted_data.insert(0, colunas)
    upload_to_Google_Sheets(converted_data)

    return True


handler()
