import requests


def get_usd_to_rub():
    url = "https://api.exchangerate-api.com/v4/latest/USD"  # URL API
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        rub_rate = data['rates']['RUB']  # Получаем курс рубля
        return rub_rate
    else:
        print("Ошибка при получении данных:", response.status_code)
        return 100


def find_first_two_digits(input_string):
    first_two_digits = None
    next_two_digits_after_dot = None
    for i in range(len(input_string) - 1):
        if input_string[i].isdigit() and input_string[i + 1].isdigit():
            first_two_digits = input_string[i:i + 2]
            break

    dot_found = False
    for i in range(len(input_string)):
        if input_string[i] == '.':
            dot_found = True
        elif dot_found and input_string[i].isdigit() and i < len(input_string) - 1 and input_string[i + 1].isdigit():
            next_two_digits_after_dot = input_string[i:i + 2]
            break

    return first_two_digits, next_two_digits_after_dot


def find_max_between_symbols(prices):
    max_values = []

    for price in prices:
        # Убираем пробелы
        cleaned_price = price.replace(" ", "")

        # Ищем значения между '$' и 'Р'
        start = cleaned_price.find('₽')
        end = cleaned_price.find('$', start)

        if start != -1 and end != -1:
            # Извлекаем подстроку между '$' и 'Р'
            value_str = cleaned_price[start + 1:end]
            # Преобразуем в целое число и добавляем в список
            if value_str.isdigit():
                max_values.append(int(value_str))

    # Возвращаем максимальные значения
    return max_values


def process_elements(elements):
    result = []

    for element in elements:
        # Разделяем строку по символу '/'
        parts = element.split('/')

        if len(parts) >= 2:
            # Берем часть до '/' и часть после 'Вт'
            before_slash = parts[0].strip()  # Часть до '/'
            after_vt = parts[1].split('Вт')[1].strip()  # Часть между '/' и 'Вт'

            # Добавляем в результат
            result.append([before_slash, after_vt])

    return result
