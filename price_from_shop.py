# Этот файл обрабатывает pdf-файлы в папке 'files/' и записывает данные в MySQL
import datetime
import difflib

import PyPDF2
import csv
import pymysql
import pandas as pd
import os
import glob

import config
import data_parsing


def find_closest_match(name, name_list):
    matches = difflib.get_close_matches(name, name_list, n=1, cutoff=0.6)
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
    print(summary_text)

    summary_list = summary_text.split('\n')
    print(summary_list)
    summary_filtered_list = list(filter(word_filter, summary_list))

    summary_filtered_list_without_commas = [i.replace(',', '.') for i in summary_filtered_list]

    processed_strings = []
    for s in summary_filtered_list_without_commas:
        # Убираем лишний правый пробел
        s = s.rstrip()

        # Разрезаем строку по 'USD'
        s = s.rsplit('USDT', 1)

        # Разрезаем каждый элемент полученного списка по пробелу
        s[0] = s[0].split()

        processed_strings.append(s)
    print('1', processed_strings, sep='\n')
    processed_final_strings = []
    for sublist in processed_strings:
        processed_final_strings.append(sublist[0])
    print('2', processed_final_strings, sep='\n')
    final_list = []
    for sublist in processed_final_strings:
        counter = 0
        for i in reversed(sublist):
            try:
                float(i)
                counter += 1
            except ValueError:
                break
        if counter == 5:
            if sublist[2] == '100-C':
                price = f'{sublist[-2]}{sublist[-1]}'
                energy_consumption = f'{sublist[-3]}'
                hash_rate = f'{sublist[-5]}{sublist[-4]}'
                name = ' '.join(sublist[:-5])
                final_list += name, hash_rate, energy_consumption, price
            elif sublist[-4] == sublist[-5]:
                price = f'{sublist[-2]}{sublist[-1]}'
                energy_consumption = f'{sublist[-3]}'
                hash_rate = f'{sublist[-4]}'
                name = ' '.join(sublist[:-4])
                final_list += name, hash_rate, energy_consumption, price
            else:
                price = f'{sublist[-2]}{sublist[-1]}'
                energy_consumption = f'{sublist[-4]}{sublist[-3]}'
                hash_rate = f'{sublist[-5]}'
                name = ' '.join(sublist[:-5])
                final_list += name, hash_rate, energy_consumption, price
        elif counter == 4:
            if len(sublist[-1]) == 3 and len(sublist[-2]) < 3:
                price = f'{sublist[-2]}{sublist[-1]}'
                energy_consumption = f'{sublist[-3]}'
                hash_rate = f'{sublist[-4]}'
                name = ' '.join(sublist[:-4])
                final_list += name, hash_rate, energy_consumption, price
            elif len(sublist[-1]) == 3 and len(sublist[-2]) == 3:
                price = f'{sublist[-1]}'
                energy_consumption = f'{sublist[-3]}{sublist[-2]}'
                hash_rate = f'{sublist[-4]}'
                name = ' '.join(sublist[:-4])
                final_list += name, hash_rate, energy_consumption, price
        elif counter == 3:
            price = f'{sublist[-1]}'
            energy_consumption = f'{sublist[-2]}'
            hash_rate = f'{sublist[-3]}'
            name = ' '.join(sublist[:-3])
            final_list += name, hash_rate, energy_consumption, price
        elif counter == 6:
            if ((len(sublist[-2]) == 1 or len(sublist[-2]) == 2) and len(sublist[-4]) == 1 and
                    (len(sublist[-6]) == 1 or len(sublist[-6]) == 2)):
                price = f'{sublist[-2]}{sublist[-1]}'
                energy_consumption = f'{sublist[-4]}{sublist[-3]}'
                hash_rate = f'{sublist[-6]}{sublist[-5]}'
                name = ' '.join(sublist[:-6])
                final_list += name, hash_rate, energy_consumption, price
        elif counter == 7:
            if sublist[1] == "EZ":
                price = f'{sublist[7]}{sublist[8]}'
                energy_consumption = f'{sublist[5]}{sublist[6]}'
                hash_rate = f'{sublist[3]}{sublist[4]}'
                name = ' '.join(sublist[:-6])
                final_list += name, hash_rate, energy_consumption, price
            else:
                price = f'{sublist[-2]}{sublist[-1]}'
                energy_consumption = f'{sublist[-4]}{sublist[-3]}'
                hash_rate = f'{sublist[-5]}'
                name = ' '.join(sublist[:-5])
                final_list += name, hash_rate, energy_consumption, price

    final_list_new = [final_list[i:i + 4] for i in range(0, len(final_list), 4)]
    print(final_list_new)
    super_final_list = []
    for el in final_list_new:
        if len(el[-2]) == 1 or len(el[-2]) == 2:
            super_final_list.append((el[0], el[-2] + el[-1], price_date))
        else:
            super_final_list.append((el[0], el[-1], price_date))
    print(super_final_list)

    # Устанавливаем соединение с базой данных MySQL
    db_connection = pymysql.connect(host=config.host, database=config.database,
                                    user=config.user, password=config.password,
                                    charset='utf8')
    cursor = db_connection.cursor()

    query = "SELECT * FROM miner_list"
    df_miner_list_full = pd.read_sql(query, db_connection)
    list_of_miners = df_miner_list_full['name'].tolist()
    summary_list = []
    # Проходим по списку сотрудников и находим наиболее похожее имя
    for miner in super_final_list:
        closest_match = find_closest_match(miner[0], list_of_miners)
        summary_list.append((miner, closest_match))
        # print(f'Для {miner} наиболее подходит: {closest_match}')

    df_for_merge = pd.DataFrame(summary_list, columns=['source', 'name'])
    print(df_for_merge)

    pre_final_df = pd.merge(df_for_merge, df_miner_list_full, on='name', how='inner', validate="many_to_many")
    print(pre_final_df.head(100))

    # Распаковка кортежей в разные колонки
    pre_final_df[['old_name', 'price', 'date']] = pre_final_df['source'].apply(lambda x: pd.Series(x))

    # Удаление исходной колонки 'Кортеж'
    final_df = pre_final_df.drop('source', axis=1)

    print(final_df)

    #
    # # Запись данных в CSV файл
    # with open(file_name, mode='w', newline='', encoding='utf-8') as file:
    #     writer = csv.writer(file)
    #     writer.writerows(summary_list)
    #
    # print(f"Таблица успешно создана в файле {file_name}")

    # Записываем данные из DataFrame в таблицу в базе данных MySQL
    for index, row in final_df.iterrows():
        insert_query = (f"INSERT INTO price_from_shop_new (name, hash_rate, energy_consumption, price, "
                        f"date, old_name)"
                        f"VALUES ('{row['name']}', '{row['hash_rate']}', '{row['energy_consumption']}', '{row['price']}', "
                        f"'{row['date']}', '{row['old_name']}');")
        cursor.execute(insert_query)

    # Фиксируем изменения и закрываем соединение с базой данных
    db_connection.commit()
    cursor.close()
    db_connection.close()
    print(f"Данные за {price_date} записаны в БД!")
