# Этот файл переносит данные из гуглдока по профитности майнинга в MySQL
import pandas as pd
import pygsheets
import pymysql

import config

path = 'credentials.json'
gc = pygsheets.authorize(service_account_file=path)
sh = gc.open('mining_profitability')
wk1 = sh[0]
df = wk1.get_as_df(index_column=0)
df.columns = ['A', 'B', 'C']

# Устанавливаем соединение с базой данных MySQL
db_connection = pymysql.connect(host=config.host, database=config.database,
                                user=config.user, password=config.password)
cursor = db_connection.cursor()

# Эта часть кода переносит данные из гуглдока по профитности майнинга (mining_profitability) в MySQL

# Очистка таблицы
query = f"TRUNCATE TABLE mining_profitability;"
cursor.execute(query)

# Подтверждение изменений
db_connection.commit()

# Записываем данные из DataFrame в таблицу в базе данных MySQL
for index, row in df.iterrows():
    insert_query = (f"INSERT INTO mining_profitability (date, currency, profitability) "
                    f"VALUES ('{row['A']}', '{row['B']}', {row['C']});")
    cursor.execute(insert_query)

# Фиксируем изменения и закрываем соединение с базой данных
db_connection.commit()
print('mining_profitability - успех.....1/9')

# Эта часть кода переносит данные из MySQL по курсу валют (currency_exchange) в Google Docs
# Запрос данных из базы данных MySQL
query = "SELECT * FROM currency_exchange"
df = pd.read_sql(query, db_connection)
df['value'] = df['value'].astype(str).str.replace('.', ',')
df = df.sort_values(by=['currency', 'date'], ascending=[True, False])
df['LEAD_price'] = df.groupby('currency')['value'].shift(-1)
max_date_indices = df.groupby('currency')['date'].idxmax()
df = df.loc[max_date_indices]
df['LEAD_price'] = df['LEAD_price'].astype(str).str.replace('.', ',')

# Открываем файл Google Sheets
sh = gc.open('currency_exchange')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('currency_exchange - успех.....2/9')


# Эта часть кода переносит данные из вьюхи asic_general_info_12_view в Google Docs

# Запрос данных из базы данных MySQL
query = "SELECT * FROM asic_general_info_12_view"
df = pd.read_sql(query, db_connection)
df['price_category'] = df.groupby(['coin', 'cool_type', 'date'])['price'].transform(lambda x: pd.cut(x, labels=False, bins=3))
df['correct_hash_rate'] = df['correct_hash_rate'].astype(str).str.replace('.', ',')
df['energy_consumption'] = df['energy_consumption'].astype(str).str.replace('.', ',')
df['price'] = df['price'].astype(str).str.replace('.', ',')
df['fiat_profitability'] = df['fiat_profitability'].astype(str).str.replace('.', ',')
df['price_category'] = df['price_category'].astype(str).str.replace('.', ',')
# Открываем файл Google Sheets
sh = gc.open('asic_general_info_12_view')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('asic_general_info_12_view - успех!!!!..3/9')

# Эта часть кода переносит данные из вьюхи asic_profitability_in_coins_5_view в Google Docs

# Запрос данных из базы данных MySQL
query = "SELECT * FROM asic_profitability_in_coins_5_view"
df = pd.read_sql(query, db_connection)
df['price_category'] = df.groupby(['coin', 'cool_type', 'date'])['price'].transform(lambda x: pd.cut(x, labels=False, bins=3))
df['count_name'] = df.groupby('name')['name'].transform('count')
df['correct_hash_rate'] = df['correct_hash_rate'].astype(str).str.replace('.', ',')
df['energy_consumption'] = df['energy_consumption'].astype(str).str.replace('.', ',')
df['fiat_profitability'] = df['fiat_profitability'].astype(str).str.replace('.', ',')
df['price_category'] = df['price_category'].astype(str).str.replace('.', ',')
df['count_name'] = df['count_name'].astype(str).str.replace('.', ',')
# Открываем файл Google Sheets
sh = gc.open('asic_profitability_in_coins_5_view')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('asic_profitability_in_coins_5_view - успех!!!!..4/9')


# Эта часть кода переносит данные из вьюхи asic_profitability_in_fiat_8_view в Google Docs

# Запрос данных из базы данных MySQL
query = "SELECT * FROM asic_profitability_in_fiat_8_view"
df = pd.read_sql(query, db_connection)
df['price_category'] = df.groupby(['coin', 'cool_type'])['price'].transform(lambda x: pd.cut(x, labels=False, bins=3))
df['correct_hash_rate'] = df['correct_hash_rate'].astype(str).str.replace('.', ',')
df['energy_consumption'] = df['energy_consumption'].astype(str).str.replace('.', ',')
df['price'] = df['price'].astype(str).str.replace('.', ',')
df['fiat_profitability'] = df['fiat_profitability'].astype(str).str.replace('.', ',')
df['price_category'] = df['price_category'].astype(str).str.replace('.', ',')
# Открываем файл Google Sheets
sh = gc.open('asic_profitability_in_fiat_8_view')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('asic_profitability_in_fiat_8_view - успех!!!!..5/9')


# Эта часть кода переносит данные из вьюхи coin_profitability_trend_9_view в Google Docs

# Запрос данных из базы данных MySQL
query = "SELECT * FROM coin_profitability_trend_9_view"
df = pd.read_sql(query, db_connection)
df['profitability'] = df['profitability'].astype(str).str.replace('.', ',')
df['value'] = df['value'].astype(str).str.replace('.', ',')
df['fiat_profitability'] = df['fiat_profitability'].astype(str).str.replace('.', ',')
df['average_hash_rate'] = df['average_hash_rate'].astype(str).str.replace('.', ',')

# Открываем файл Google Sheets
sh = gc.open('coin_profitability_trend_9_view')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('coin_profitability_trend_9_view - успех!!!!..6/9')


# Эта часть кода переносит данные из вьюхи asic_profitability_in_coins_calculator_10_view в Google Docs

# Запрос данных из базы данных MySQL
query = "SELECT * FROM asic_profitability_in_coins_calculator_10_view"
df = pd.read_sql(query, db_connection)
df['price_category'] = df.groupby(['coin', 'cool_type'])['price'].transform(lambda x: pd.cut(x, labels=False, bins=3))
df['correct_hash_rate'] = df['correct_hash_rate'].astype(str).str.replace('.', ',')
df['profitability'] = df['profitability'].astype(str).str.replace('.', ',')
df['price_category'] = df['price_category'].astype(str).str.replace('.', ',')
# Открываем файл Google Sheets
sh = gc.open('asic_profitability_in_coins_calculator_10_view')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('asic_profitability_in_coins_calculator_10_view - успех!!!!..7/9')


# Эта часть кода переносит данные из вьюхи asic_general_info_47_view в Google Docs

# Запрос данных из базы данных MySQL
query = "SELECT * FROM asic_general_info_47_view"
df = pd.read_sql(query, db_connection)
df['price_category'] = df.groupby(['coin', 'cool_type', 'date'])['price'].transform(lambda x: pd.cut(x, labels=False, bins=3))
df['count_name'] = df.groupby('name')['name'].transform('count')
df['correct_hash_rate'] = df['correct_hash_rate'].astype(str).str.replace('.', ',')
df['energy_consumption'] = df['energy_consumption'].astype(str).str.replace('.', ',')
df['price'] = df['price'].astype(str).str.replace('.', ',')
df['fiat_profitability'] = df['fiat_profitability'].astype(str).str.replace('.', ',')
df['price_category'] = df['price_category'].astype(str).str.replace('.', ',')
df['count_name'] = df['count_name'].astype(str).str.replace('.', ',')
# Открываем файл Google Sheets
sh = gc.open('asic_general_info_47_view')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
print('asic_general_info_47_view - успех!!!!..8/9')


# Эта часть кода переносит данные из MySQL по профитности майнинга (mining_profitability) в Google Docs
# Запрос данных из базы данных MySQL
query = "SELECT * FROM mining_profitability"
df = pd.read_sql(query, db_connection)
del df['profitability']
df = df.sort_values(by=['currency', 'date'], ascending=[True, False])
df['lead_date'] = df.groupby('currency')['date'].shift(1)

# Открываем файл Google Sheets
sh = gc.open('mining_profitability_bot')

# Выбираем лист, на который будем добавлять данные
wks = sh[0]
wks.clear()
# Загружаем данные на лист Google Sheets
wks.set_dataframe(df, (1, 1))
db_connection.commit()
sh = gc.open('mining_profitability_bot')
wk1 = sh[0]
df = wk1.get_as_df(index_column=0)
df.columns = ['A', 'B', 'C']
print(df)

# Очистка таблицы
query = f"TRUNCATE TABLE mining_profitability_bot;"
cursor.execute(query)

# Подтверждение изменений
db_connection.commit()

# Записываем данные из DataFrame в таблицу в базе данных MySQL
for index, row in df.iterrows():
    insert_query = (f"INSERT INTO mining_profitability_bot (date, currency, lead_date) "
                    f"VALUES ('{row['A']}', '{row['B']}', '{row['C']}');")
    cursor.execute(insert_query)

# Фиксируем изменения и закрываем соединение с базой данных
db_connection.commit()
print('mining_profitability_bot - успех.....9/9')
cursor.close()
db_connection.close()
