# Этот файл обрабатывает pdf-файлы в папке 'files/' и записывает данные в MySQL
import datetime
import difflib

import PyPDF2
import csv
import pymysql
import pandas as pd
import os
import glob
import re
from fuzzywuzzy import process

import config
import data_parsing


def find_closest_match(name, name_list):
    matches = difflib.get_close_matches(name, name_list, n=1, cutoff=0.1)
    if matches:
        return matches[0]
    else:
        return "Нет соответствий"


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
    current_year = datetime.datetime.now().year
    result = f'{str(current_year)}-{date[1]}-{date[0]}'
    list_of_dates.append(result)

print(list_of_dates)


def word_filter(letter):
    return ('USDT' in letter and '(USDT)' not in letter and '(ВОДЯНОЕ ОХЛАЖДЕНИЕ)' not in letter and
            '(ИММЕРСИОННОЕ ОХЛАЖДЕНИЕ)' not in letter and
            ('ГТД100' not in letter) and 'WHATSMINER M56S++ 248T 22W' not in letter and
            ('ANTMINER X5' not in letter) and ('SC5' not in letter) and ('DR7' not in letter)
            and ('BM-S3' not in letter) and ('RXD' not in letter) and ('DRAGONBALL' not in letter))


for file, price_date in zip(files, list_of_dates):
    # Открываем PDF-файл
    pdf_file = open(file, 'rb')

    # Создаем объект PdfFileReader
    pdf_reader = PyPDF2.PdfFileReader(pdf_file)

    # Получаем количество страниц в PDF-файле
    num_pages = pdf_reader.numPages

    summary_text = ''
    summary_list = []
    # Извлекаем текст из каждой страницы
    for page_num in range(num_pages):
        page = pdf_reader.getPage(page_num)
        text = page.extract_text()

        # Выводим текст на экран
        summary_text += text

    # Закрываем PDF-файл
    pdf_file.close()
    # print(summary_text)

    summary_list = summary_text.split('\n')
    # print(summary_list)

    price_list = []
    characters_list = []
    num_list = []
    asic_list = []

    # Фильтрация элементов
    for num, item in enumerate(summary_list):
        if '$' in item and '₽' in item and 'Цена' not in item:
            price_list.append(item)
            num_list.append(num - 1)
        if 'Вт' in item:
            characters_list.append(item)

    # print(price_list)
    # print(characters_list)

    for num, item in enumerate(summary_list):
        if num in num_list:
            asic_list.append(item)

    final_price_list = data_parsing.find_max_between_symbols(price_list)
    # print(final_price_list)

    # print(asic_list)

    concatenated_list = [f"{x} {y} {z}" for x, y, z in zip(asic_list, characters_list, final_price_list)]

    print(concatenated_list)

    final_asic_list = data_parsing.process_elements(concatenated_list)

    print(final_asic_list)
    asic_name_list = [sublist[0] for sublist in final_asic_list]
    asic_price_list = [sublist[1] for sublist in final_asic_list]

    # Исходные данные
    mapping = {'ANTMINER': ['e9', 'hs3', 'k7', 'ka3', 'l7', 'l9', 's19', 's21', 't19', 't21', 'z15', 'al1', 'ks5 '],
               'ICERIVER': ['ks0', 'ks3m', 'ks5l', 'ks5m', 'al0', 'al3'],
               'ELPHAPEX': ['dg'],
               'JASMINER': ['x4', 'x16'],
               'WHATSMINER': ['m'],
               'GOLDSHELL': ['al box', 'ka box', 'mini doge', 'e-']}

    # Результирующий список
    modified_elements = []

    # Проходим по каждому элементу списка
    for element in asic_name_list:
        lower_element = element.lower()  # Переводим в нижний регистр
        added_key = None  # Переменная для хранения добавленного ключа

        # Проверяем каждую пару ключ-значение в словаре
        for key, values in mapping.items():
            for value in values:
                if lower_element.startswith(value):  # Проверяем на совпадение
                    added_key = key  # Запоминаем ключ
                    break  # Выходим из внутреннего цикла, если нашли совпадение
            if added_key:  # Если ключ был найден, выходим из внешнего цикла
                break

        # Формируем новый элемент
        if added_key:
            modified_elements.append(f"{added_key} {element}")  # Добавляем ключ перед элементом
        else:
            modified_elements.append(element)  # Если совпадений нет, добавляем элемент без изменений

    # Вывод результата
    print(modified_elements)

    final_modified_elements = []

    for el in modified_elements:
        final_modified_elements.append(re.sub(r'\s*\(.*?\)\s*', ' ', el).strip())

    final_modified_elements_plus_price = zip(final_modified_elements, asic_price_list)
    print(final_modified_elements_plus_price)

    #
    # Устанавливаем соединение с базой данных MySQL
    db_connection = pymysql.connect(host=config.host, database=config.database,
                                    user=config.user, password=config.password,
                                    charset='utf8')
    cursor = db_connection.cursor()

    query = "SELECT * FROM miner_list"
    df_miner_list_full = pd.read_sql(query, db_connection)
    list_of_miners = df_miner_list_full['name'].tolist()
    print(list_of_miners)
    summary_asic_list = []
    # # Проходим по списку сотрудников и находим наиболее похожее имя
    # for miner in modified_elements:
    #     closest_match = find_closest_match(miner[0], list_of_miners)
    #     summary_asic_list.append((miner, closest_match))
    #     print(f'Для {miner} наиболее подходит: {closest_match}')
    #
    # df_for_merge = pd.DataFrame(summary_asic_list, columns=['source', 'name'])
    # print(df_for_merge)

    for input_element in final_modified_elements_plus_price:
        best_match = process.extractOne(input_element[0], list_of_miners)
        print(f"Для {input_element[0]} Наиболее похожий элемент: {best_match[0]}")
        answer = input("Вы согласны с этим сопоставлением? Y/N: ")
        if answer == 'Y':
            summary_asic_list.append((input_element[0], input_element[1], best_match[0]))
        elif answer == 'N':
            match = input(f"Введите корректное название для {input_element}: ")
            summary_asic_list.append((input_element[0], input_element[1], match))
        else:
            print("Переход к следующей итерации. Не забудьте потом скорректировать нужные строки в DBeaver")
            summary_asic_list.append((input_element[0], input_element[1], best_match[0]))
    print(summary_asic_list)
    df_for_merge = pd.DataFrame(summary_asic_list, columns=['source', 'price', 'name'])
    print(df_for_merge)

    asic_list_for_price_insert = [
        'ANTMINER KA3 166T', 'ANTMINER K7 63.5T', 'ANTMINER HS3 9T', 'ANTMINER D9 1770G', 'GOLDSHELL KA BOX PRO',
        'GOLDSHELL E-AL1M', 'GOLDSHELL AL BOX Ⅱ PLUS', 'GOLDSHELL AL BOX Ⅱ', 'GOLDSHELL E-KA1M',
        'GOLDSHELL E-DG1M', 'GOLDSHELL AL BOX', 'BOMBAX EZ 100 12500M', 'BOMBAX EZ 100-C 3200M',
        'BOMBAX EZ 100-C 3800M', 'JASMINER X16-P 5800M', 'JASMINER X16PE-5250M'
    ]

    usd_to_rub = data_parsing.get_usd_to_rub()

    for asic in asic_list_for_price_insert:
        asic_price = input(f"Введите актуальный прайс для {asic}: ")
        try:
            summary_asic_list.append((asic, round((int(asic_price)/usd_to_rub), 0), asic))
        except ValueError:
            summary_asic_list.append((asic, 0, asic))

    df_for_merge = pd.DataFrame(summary_asic_list, columns=['source', 'price', 'name'])
    print(df_for_merge)

    final_df = pd.merge(df_for_merge, df_miner_list_full, on='name', how='inner', validate="many_to_many")
    print(final_df.head(100))

    final_df['date'] = price_date
    print(final_df)
    # # Распаковка кортежей в разные колонки
    # pre_final_df[['old_name', 'price', 'date']] = pre_final_df['source'].apply(lambda x: pd.Series(x))
    #
    # # Удаление исходной колонки 'Кортеж'
    # final_df = pre_final_df.drop('source', axis=1)
    #
    # print(final_df)
    #
    # #
    # # # Запись данных в CSV файл
    # # with open(file_name, mode='w', newline='', encoding='utf-8') as file:
    # #     writer = csv.writer(file)
    # #     writer.writerows(summary_list)
    # #
    # # print(f"Таблица успешно создана в файле {file_name}")
    #
    # Записываем данные из DataFrame в таблицу в базе данных MySQL
    for index, row in final_df.iterrows():
        insert_query = (f"INSERT INTO price_from_shop_new (name, hash_rate, energy_consumption, price, "
                        f"date, old_name)"
                        f"VALUES ('{row['name']}', '{row['hash_rate']}', '{row['energy_consumption']}', '{row['price']}', "
                        f"'{row['date']}', '{row['source']}');")
        cursor.execute(insert_query)

    # Фиксируем изменения и закрываем соединение с базой данных
    db_connection.commit()
    cursor.close()
    db_connection.close()
    print(f"Данные за {price_date} записаны в БД!")
