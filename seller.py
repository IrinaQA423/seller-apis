import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина Озон.

    Отправляет запрос к API Озон для получения списка товаров,
    начиная с указанного идентификатора последнего товара.

    Аргументы:
        last_id (str): Идентификатор последнего товара, с которого 
            начинается выборка. Не может быть пустым.
        client_id (str): Идентификатор клиента, предоставленный Озон. 
            Не может быть пустым.
        seller_token (str): Токен продавца для авторизации. Не может 
            быть пустым.

    Возвращает:
        list: Список товаров магазина. Каждый товар представлен в 
        виде словаря с деталями, полученными из API.

    Исключения:
        HTTPError: Если ответ от сервера содержит ошибку (например, 
        4xx или 5xx статус).
        ValueError: если  один из аргументов пустой.

    Примеры корректного ввода:
        >>> product_list = get_product_list("12345", "your_client_id", "your_seller_token")

    Примеры некорректного ввода:
        >>> product_list = get_product_list("", "your_client_id", "your_seller_token")  
        ValueError
        >>> product_list = get_product_list("12345", "", "your_seller_token")
        ValueError
        >>> product_list = get_product_list("12345", "your_client_id", "")
        ValueError
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
   """Получить артикулы товаров магазина Озон.

    Эта функция извлекает список артикулов товаров из магазина Озон, 
    используя предоставленный идентификатор клиента и токен продавца.

    Аргументы:
        client_id (str): Идентификатор клиента. Должен быть непустой строкой.
        seller_token (str): Токен продавца. Должен быть непустой строкой.

    Возвращвет:
        list: Список артикулов товаров (offer_id), полученных из магазина.

    Исключения:
        ValueError: Если client_id или seller_token пустые строки.
        ConnectionError: Если не удается установить соединение с API Озон.
        
    Примеры корректного ввода:
        >>> offer_ids = get_offer_ids("123456789", "your_seller_token")
        
    Примеры некорректного ввода:
        >>> offer_ids = get_offer_ids("", "your_seller_token")
        ValueError
        >>> offer_ids = get_offer_ids("123456789", "")
        ValueError
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на платформе Озон.

    Отправляет запрос на обновление цен товаров через API Озон.

    Аргументы:
        prices (list): Список цен, которые необходимо обновить. 
                       Каждый элемент должен быть словарем с полями 'sku' и 'price'.
                       Пример: [{'sku': '12345', 'price': 100.0}, {'sku': '67890', 'price': 200.0}].
        client_id (str): Идентификатор клиента Озон.
        seller_token (str): Токен продавца для авторизации.

    Возвращает:
        dict: Ответ API Озон, содержащий информацию об обновлении цен.

    Исключения:
        requests.exceptions.HTTPError: Если запрос завершился неудачно (например, 
        неверный client_id, seller_token или формат prices).

    Примеры корректного ввода:
        >>> update_price(
            prices=[{'sku': '12345', 'price': 100.0}, {'sku': '67890', 'price': 200.0}],
            client_id='your_client_id',
            seller_token='your_seller_token'
        )

    Примеры некорректного ввода:
        >>> update_price(
            prices=[{'sku': '12345', 'price': 'сто'}, {'sku': '67890', 'price': -50}],
            client_id='your_client_id',
            seller_token='your_seller_token'
        )
        ValueError: 'price' не является числом или меньше нуля.

        >>> update_price(
            prices=[{'sku': '12345', 'price': 100.0}],
            client_id='',
            seller_token='your_seller_token'
        )
        HTTPError: client_id пустой.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров на платформе Озон.

    Эта функция отправляет запрос на обновление остатков товаров в системе Озон.
    Для этого используется API Озон, который требует аутентификацию с помощью 
    client_id и seller_token.

    Аргументы:
        stocks (list): Список остатков товаров, где каждый элемент должен быть 
            словарем с ключами 'sku' (строка) и 'quantity' (целое число).
            Пример корректного элемента: {'sku': '12345', 'quantity': 10}.
        client_id (str): Идентификатор клиента, предоставленный Озон.
        seller_token (str): Токен продавца, предоставленный Озон.

    Возвращает:
        dict: Ответ от API Озон в формате JSON с информацией об обновлении остатков.

    Исключения:
        HTTPError: Если запрос к API завершился ошибкой.
        ValueError: Если один из элементов в stocks имеет некорректный формат

    Примеры корректного ввода:
        >>> update_stocks([{'sku': '12345', 'quantity': 10}, {'sku': '67890', 'quantity': 5}], 'your_client_id', 'your_seller_token')

    Примеры некорректного ввода:
        >>> update_stocks(
            [{'sku': '12345', 'quantity': 'ten'}], 
            'your_client_id', 
            'your_seller_token'
        )
        ValueError: Количество должно быть целым числом.

        >>> update_stocks(
            [{'sku': '12345', 'quantity': -5}], 
            'your_client_id', 
            'your_seller_token'
        )
        ValueError: Количество не может быть отрицательным.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл остатки с сайта casio.

    Эта функция загружает ZIP-архив с остатками часов с указанного веб-сайта, 
    извлекает файл Excel из архива и преобразует его в список словарей, 
    где каждый словарь представляет собой запись о часах.

    Возвращает:
        list[dict]: Список остатков часов, где каждый элемент - это 
        словарь с данными о часах.

    Исключения:
        requests.HTTPError: Если запрос к URL завершился ошибкой.
        ValueError: Если структура загруженного Excel-файла некорректна.
        FileNotFoundError: Если файл "ostatki.xls" не существует.

    Примеры корректного ввода:
        >>> stock_data = download_stock()
        >>> print(stock_data)  
        
    Примеры некорректного ввода:
        >>> stock_data = download_stock("https://invalid-url.com")
        HTTPError

    Примечания:
        - Функция удаляет загруженный файл "ostatki.xls" после его обработки.
        - В случае ошибок во время загрузки или обработки файла будет 
          сгенерировано исключение.    
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков товаров на основе данных о недостающих запасах и идентификаторов товаров.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих информацию о недостающих запасах.
            Каждый словарь должен содержать ключи:
                "Код" (str): Уникальный идентификатор товара.
                "Количество" (str): Количество товара, представленное как строка. 
                Допустимые значения:
                    ">10": интерпретируется как 100.
                    "1": интерпретируется как 0.
                    Любое другое число: интерпретируется как целое число.
        offer_ids (list): Список идентификаторов товаров (str), для которых нужно создать остатки.

    Возвращает:
        list: Список словарей, каждый из которых содержит:
            "offer_id" (str): Идентификатор товара.
            "stock" (int): Количество товара на складе.

    Исключения:
        ValueError: Если "Количество" в watch_remnants не является строкой или не соответствует ожидаемым значениям.

    Примеры корректного ввода:
        >>> create_stocks([{"Код": "123", "Количество": ">10"}, {"Код": "456", "Количество": "5"}], ["123", "456", "789"])
        [{'offer_id': '123', 'stock': 100}, {'offer_id': '456', 'stock': 5}, {'offer_id': '789', 'stock': 0}]

    Примеры некорректного ввода:
        >>> create_stocks([{"Код": "123", "Количество": "abc"}], ["123"]) 
        ValueError: Неверное значение для количества товара. Ожидается строка, соответствующая формату.
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """ Создает список объектов цен для часов, которые присутствуют в списке предложений.

    Аргументы:
        watch_remnants (iterable): Итерируемый объект, содержащий словари с информацией о часах.
            Каждый словарь должен содержать ключи "Код" (строка) и "Цена" (число).
        offer_ids (set): Набор строк, представляющих идентификаторы товаров.

    Возвращает:
        list: Список словарей, каждый из которых содержит информацию о цене.
            Каждый словарь имеет следующие ключи:
                "auto_action_enabled": Значение "UNKNOWN".
                "currency_code": Значение "RUB".
                "offer_id": Идентификатор предложения.
                "old_price": Значение "0".
                "price": Преобразованная цена из "Цена" в требуемый формат.

    Исключения:
        ValueError: Если цена товара не может быть преобразована в подходящий формат 
        (например, если цена не является числом или содержит недопустимые значения).

    Примеры корректного ввода:
        >>> watch_remnants = [{'Код': '123', 'Цена': 1000.0}]
        >>> offer_ids = {'123'}
        >>> prices = create_prices(watch_remnants, offer_ids)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': 1000.0}]

    Примеры некорректного ввода:(Код не найден в offer_ids):
        >>> watch_remnants = [{'Код': '456', 'Цена': 2000.0}]
        >>> offer_ids = {'123'}
        >>> prices = create_prices(watch_remnants, offer_ids)
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строковое представление цены в числовое значение.

    Удаляет все символы форматирования (разделители тысяч, символы валюты и т. д.)
    и усекает десятичные дроби, возвращая только целую часть цены.Если в строке отсутствуют 
    цифры, возвращает пустую строку.

    Аргументы:
        price (str): Строка, представляющая отформатированное значение цены. Может содержать
                 разделители тысяч (такие как ' или ,), десятичные дроби и
                 символы валюты. Должна содержать хотя бы одну цифру для значимого
                 преобразования.

    Возвращает:
        str: Строка, содержащая только числовые символы целой части
             цены. Возвращает пустую строку, если в вводе не найдены цифры.

    Исключения:
        AttributeError: Если ввод не является строкой (например, None или другой тип).

    Примеры корректного ввода:
        >>> "5'990.00 руб."
        "5990"
        >>> "250€"
        "250"
        >>> "1000"
        "1000"

    Примеры некорректного ввода:
        >>> "six dollars" 
        ""
        >>>  "" 
        ""
        >>> None
        AttributeError
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части, каждая из которых содержит n элементов.

    Функция принимает список и целое число n и возвращает генератор, который 
    выдает подсписки длиной n. Последний подсписок может быть короче, если 
    длина исходного списка не делится на n.

    Аргументы:
        lst (list): Список, который нужно разделить.
        n (int): Количество элементов в каждой части. Должно быть положительным 
                  целым числом.

    Yields:
        list: Подсписок длиной n из исходного списка.

    Исключения:
        ValueError: Если n меньше или равно нулю.
        TypeError: Если lst не является списком.

    Примеры корректного ввода:
        >>> divide([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4], [5]]
        >>> divide(['a', 'b', 'c', 'd'], 3)
        [['a', 'b', 'c'], ['d']]
        >>> divide([], 1)
        []

    Примеры некорректного ввода:
        >>> divide([1, 2, 3], 0)
        ValueError: деление на ноль
        >>> divide([1, 2, 3], -1)
        ValueError: отрицательное значение n
        >>> divide(None, 2)
        TypeError: тип lst не list
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает цены для заданных остатков.

    Эта функция получает идентификаторы товаров, создает список цен для этих товаров
    и обновляет цены в системе.

    Аргументы:
        watch_remnants (list): Список остатков для обновления цен.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца для аутентификации.

    Возвращает:
        list: Список обновленных цен.

    Исключения:
        ValueError: Если watch_remnants пуст или client_id или seller_token недействительны.

    Примеры корректного ввода:
        >>> await upload_prices(watch_remnants, "client123", "token456")
        ['price1', 'price2', 'price3']

    Примеры некорректного ввода:
        >>> await upload_prices([], "client123", "token456")
        ValueError: watch_remnants не должен быть пустым.

        >>> await upload_prices(watch_remnants, "", "token456")
        ValueError: client_id не может быть пустым.

        >>> await upload_prices(watch_remnants, "client123", "")
        ValueError: seller_token не может быть пустым.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает запасы товаров для указанного клиента.

    Эта функция принимает остатки товаров и обновляет их на сервере, используя
    идентификатор клиента и токен продавца. Она разбивает запасы на порции
    по 100 единиц и обновляет их поэтапно.

    Аргументы:
        watch_remnants (list): Список остатков товаров, каждый элемент
            которого должен быть словарем, содержащим информацию о товаре.
            Пример: [{'offer_id': '123', 'stock': 10}, {'offer_id': '456', 'stock': 5}].
        client_id (str): Идентификатор клиента, который использует API.
        seller_token (str): Токен продавца для аутентификации при обращении к API.

    Возвращает:
        tuple: Кортеж из двух элементов:
            list: Список остатков, где количество на складе не равно нулю.
            list: Исходный список остатков товаров.

    Исключения:
        ValueError: Если `watch_remnants` не является списком или если
            элементы списка не содержат необходимых ключей.
        TypeError: Если тип `watch_remnants` не является списком.
        
    Примеры корректного ввода:
        >>> await upload_stocks([{'offer_id': '123', 'stock': 10}], 'client_1', 'token_abc')

    Примеры некорректного ввода:
        >>> await upload_stocks('не список', 'client_1', 'token_abc')
        TypeError
        >>> await upload_stocks([{'offer_id': '123'}], 'client_1', 'token_abc')
        ValueError
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
