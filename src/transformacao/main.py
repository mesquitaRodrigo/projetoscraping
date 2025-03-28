# Importar bibliotecas
import pandas as pd
import sqlite3
from datetime import datetime

# Ler o arquivo JSONL
df = pd.read_json('/home/rodrigo/projetos/projetoscraping_mercado_livre_1/data/data.jsonl', lines=True)

# Mostrar todas as colunas
pd.options.display.max_columns = None

# Adicionar colunas auxiliares
df['_source'] = 'https://lista.mercadolivre.com.br/tenis-corrida-masculino'
df['_data_coleta'] = datetime.now()

# Tratar valores nulos
df['old_price_reais'] = df['old_price_reais'].fillna(0).astype(float)
df['old_price_centavos'] = df['old_price_centavos'].fillna(0).astype(float)
df['new_price_reais'] = df['new_price_reais'].fillna(0).astype(float)
df['new_price_centavos'] = df['new_price_centavos'].fillna(0).astype(float)
df['reviews_rating_number'] = df['reviews_rating_number'].fillna(0).astype(float)

# Remover parênteses da coluna `reviews_amount`
df['reviews_amount'] = df['reviews_amount'].fillna("0").str.replace(r'[()]', '', regex=True).astype(int)

# Calcular os valores totais dos preços
df['old_price'] = df['old_price_reais'] + df['old_price_centavos'] / 100
df['new_price'] = df['new_price_reais'] + df['new_price_centavos'] / 100

# Remover colunas antigas de preços
df.drop(columns=['old_price_reais', 'old_price_centavos', 'new_price_reais', 'new_price_centavos'], inplace=True)

# Conectar ao banco de dados SQLite
conn = sqlite3.connect('/home/rodrigo/projetos/projetoscraping_mercado_livre_1/data/quotes.db')

# Salvar no banco de dados
df.to_sql('mercadolivre_items', conn, if_exists='replace', index=False)

# Fechar conexão
conn.close()

# Mostrar resultado
print(df.head())
