# Этот файл собирает список из дат из файлов в папке 'files/', а затем идет в API Coinbase
# и вытаскивает оттуда курсы криптовалют на эти даты, затем записывает данные в MySQL

import csv
import datetime
import glob
import os

import requests
import pymysql
import pandas as pd
import time

import config
import data_parsing


def process_file(file_path):
    # Здесь можно выполнить любую необходимую обработку файла
    print(f"Обрабатывается файл: {file_path}")


# Путь к папке, в которой находятся файлы
folder_path = "files/"

# Ищем все файлы в указанной папке
files = glob.glob(os.path.join(folder_path, "*"))

final_list = []

# Проходимся по каждому файлу и применяем функцию process_file
for file in files:
    process_file(file)
    result = data_parsing.find_first_two_digits(file)
    final_list.append(result)

print(final_list)

list_of_dates = []

for date in final_list:
    # if int(date[1]) in (1, 2, 3):
    #     print(f'2024-{date[1]}-{date[0]}')
    #     result = f'2024-{date[1]}-{date[0]}'
    #     list_of_dates.append(result)
    # else:
    #     print(f'2023-{date[1]}-{date[0]}')
    #     result = f'2023-{date[1]}-{date[0]}'
    #     list_of_dates.append(result)
    current_year = datetime.datetime.now().year
    result = f'{str(current_year)}-{date[1]}-{date[0]}'
    list_of_dates.append(result)

print(list_of_dates)

user_agent = {'User-agent': 'Mozilla/5.0'}
currencies = ['BTC', 'ETH', 'ETC', 'LTC', 'DASH', 'DOGE', 'ZEC', 'KAS', 'KDA', 'HNS', 'CKB', 'ALPH']
# price_date = '2024-03-21'

# date = datetime.date(2024, 3, 21)

for price_date in list_of_dates:
    date = datetime.date(int(price_date.split('-')[0]), int(price_date.split('-')[1]), int(price_date.split('-')[2]),)
    list_ = []
    for currency in currencies:
        url = f'https://api.coinbase.com/v2/prices/{currency}-USD/spot?date={date}'
        response = requests.get(url, headers=user_agent)
        data = response.json()
        print(currency)
        print(data)
        try:
            cur = data['data']['base']
            value = data['data']['amount']
            list_ += price_date, cur, value
        except KeyError:
            continue
        time.sleep(2)
    list_new = [list_[i:i + 3] for i in range(0, len(list_), 3)]
    print(list_new)

    file_name = 'currency_new.csv'

    with open(file_name, mode='w', newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        for row in list_new:
            writer.writerow(row)

    print(f"Данные успешно записаны в файл {file_name}")

    df = pd.read_csv(file_name, header=None, encoding='utf-8')
    df.columns = ['A', 'B', 'C']

    # Устанавливаем соединение с базой данных MySQL
    db_connection = pymysql.connect(host=config.host, database=config.database,
                                    user=config.user, password=config.password,
                                    charset='utf8')
    cursor = db_connection.cursor()

    # Записываем данные из DataFrame в таблицу в базе данных MySQL
    for index, row in df.iterrows():
        insert_query = (f"INSERT INTO currency_exchange (date, currency, value) "
                        f"VALUES ('{row['A']}', '{row['B']}', {row['C']});")
        cursor.execute(insert_query)

    # Фиксируем изменения и закрываем соединение с базой данных
    db_connection.commit()
    cursor.close()
    db_connection.close()
    print(f'Данные за {price_date} успешно записаны в БД!')
