import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из Яндекс.Маркета.

    Отправляет запрос к API Яндекс Маркета для получения списка товаров,
    используя указанный токен доступа и идентификатор кампании.

    Аргументы:
        page (str): Токен страницы для постраничного вывода.
            Должен быть строкой, представляющей токен страницы.
        campaign_id (str): Идентификатор кампании.
            Должен быть строкой, представляющей уникальный идентификатор кампании.
        access_token (str): Токен доступа для авторизации.
            Должен быть строкой, представляющей действительный токен.

    Возвращает:
        list: Список продуктов, соответствующих заданной кампании.
            Возвращает пустой список, если продуктов не найдено.

    Исключения:
        HTTPError: Если запрос завершился неудачей (например, неверные параметры или проблемы с сетью).

    Примеры  корректного ввода:
            get_product_list("some_page_token", "123456", "your_access_token")

    Примеры  некорректного ввода:
            get_product_list(123, "123456", "your_access_token")  # page должен быть строкой
            get_product_list("some_page_token", "invalid_campaign_id", "your_access_token")  # некорректный campaign_id
            get_product_list("some_page_token", "123456", None)  # access_token не должен быть None
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет запасы товаров на Яндекс.Маркете.

    Отправляет запрос к API Яндекс Маркета для обновления остатков товаров
    в указанной кампании.

    Аргументы:
        stocks (list): Список идентификаторов SKU товаров, для которых
            необходимо обновить запасы. Каждый элемент должен быть строкой.
            Пример корректного ввода: ["sku1", "sku2", "sku3"].
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа для авторизации в API.

    Возвращает:
        dict: Ответ API в формате JSON, содержащий информацию об обновленных запасах.

    Исключения:
        HTTPError: Если запрос завершился неудачей (например, неверный токен доступа,
            некорректный идентификатор кампании или неверный формат данных).
    
    Примеры  корректного ввода:
            update_stocks(["sku1", "sku2"], "12345", "your_access_token")
        
    Примеры  некорректного ввода:
            update_stocks("sku1, sku2", "12345", "your_access_token")  # stocks должен быть списком
            update_stocks(["sku1", "sku2"], 12345, "your_access_token")  # campaign_id должен быть строкой
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены на товары.

    Отправляет запрос к API Яндекс Маркета для обновления цен товаров
    в указанной кампании.

    Аргументы:
        prices (list): Список объектов с ценами для обновления. Каждый объект
            должен содержать идентификатор SKU и новую цену. 
        campaign_id (str): Идентификатор кампании.
        access_token (str): Токен доступа для авторизации в API.

    Возвращает:
        dict: Ответ API в формате JSON, содержащий информацию об обновленных ценах.

    Исключения:
        HTTPError: Если запрос завершился неудачей (например, неверный токен доступа,
            некорректный идентификатор кампании или неверный формат данных).
    
    Примеры  корректного ввода:
            update_price([{"sku": "sku1", "price": 100}, {"sku": "sku2", "price": 200}], "12345", "your_access_token")
        
    Примеры  некорректного ввода:
            update_price([{"sku": "sku1", "price": "сто"], "12345", "your_access_token")  # цена должна быть числом
            update_price([{"sku": "sku1", "price": 100}], 12345, "your_access_token")  # campaign_id должен быть строкой
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы товаров Яндекс маркета.

    Эта функция извлекает идентификаторы (артикулы) товаров из Яндекс Маркета.
    Данные извлекаются постранично до тех пор, пока не будут
    получены все товары.

    Аргументы:
        campaign_id (str): Идентификатор кампании. Должен быть непустой строкой.
        market_token (str): Токен доступа к Яндекс Маркету. Должен быть непустой строкой.

    Возвращает:
        list: Список идентификаторов  (артикулов) товаров. Пустой список
        возвращается, если товары не найдены.

    Исключения:
        ValueError: Если campaign_id или market_token пустые строки.
        Exception: Если происходит ошибка при запросе данных.

    Примеры  корректного ввода:
            offer_ids = get_offer_ids("12345", "your_market_token")

    Примеры  некорректного ввода:
            get_offer_ids("", "your_market_token")  # Вызывает ValueError
            get_offer_ids("12345", "")  # Вызывает ValueError
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """ Создает список запасов на основе остатка часов и идентификаторов товаров.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих информацию о часах.
            Каждый словарь должен содержать ключи:
                - "Код" (str): Код товара.
                - "Количество" (str): Количество товара (например, "1", ">10").
        offer_ids (list): Список идентификаторов товаров (str), которые необходимо проверить.
        warehouse_id (str): Идентификатор склада, к которому относятся запасы.

    Возвращает:
        list: Список словарей, каждый из которых содержит информацию о запасах для
        каждого из товаров. Каждый словарь имеет следующую структуру:
            {
                "sku": str,           # Код товара
                "warehouseId": str,   # Идентификатор склада
                "items": [
                    {
                        "count": int,    # Количество товара
                        "type": str,     # Тип товара ("FIT")
                        "updatedAt": str, # Дата обновления в формате ISO
                    }
                ]
            }

    Примеры  корректного ввода:
        create_stocks(
            watch_remnants=[
                {"Код": "123", "Количество": "5"},
                {"Код": "456", "Количество": ">10"},
                {"Код": "789", "Количество": "1"}
            ],
            offer_ids=["123", "456", "999"],
            warehouse_id="warehouse_1"
        )
        
    Примеры  некорректного ввода:
        create_stocks(
            watch_remnants=[
                {"Код": "123", "Количество": "abc"},  # Некорректное количество
            ],
            offer_ids="not_a_list",  # Некорректный тип
            warehouse_id=123  # Некорректный тип
        )
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """ Создает список цен на основе остатка часов и идентификаторов товаров.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих информацию о часах.
            Каждый словарь должен содержать ключи:
                - "Код" (str): Код товара.
                - "Цена" (str или float): Цена товара, которую необходимо конвертировать.
        offer_ids (list): Список идентификаторов товаров (str), которые необходимо проверить.

    Возвращает:
        list: Список словарей, каждый из которых содержит информацию о ценах для
        каждого из товаров. Каждый словарь имеет следующую структуру:
            {
                "id": str,             # Код товара
                "price": {
                    "value": int,       # Цена товара после конвертации
                    "currencyId": str,  # Идентификатор валюты (например, "RUR")
                }
            }

    Примеры  корректного ввода:
        create_prices(
            watch_remnants=[
                {"Код": "123", "Цена": "1000"},
                {"Код": "456", "Цена": 1500.75},
                {"Код": "789", "Цена": "500"}
            ],
            offer_ids=["123", "456", "999"]
        )
        
    Примеры  некорректного ввода:
        create_prices(
            watch_remnants=[
                {"Код": "123", "Цена": "abc"},  # Некорректное значение цены
            ],
            offer_ids="not_a_list"  # Некорректный тип, ожидается список
        )
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """ Загружает цены на основе остатка часов, идентификатора кампании и токена рынка.

    Эта функция получает идентификаторы товаров, создает список цен на основе
    остатков и обновляет цены, отправляя их пакетами по 500.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих информацию о часах.
            Каждый словарь должен содержать ключи:
                - "Код" (str): Код товара.
                - "Цена" (str или float): Цена товара, которую необходимо конвертировать.
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен для доступа к API рынка.

    Возвращает:
        list: Список словарей с ценами, которые были загружены на рынок.

    Примеры  корректного ввода:
        await upload_prices(
            watch_remnants=[
                {"Код": "123", "Цена": "1000"},
                {"Код": "456", "Цена": 1500.75},
                {"Код": "789", "Цена": "500"}
            ],
            campaign_id="campaign_123",
            market_token="your_market_token"
        )
        
    Примеры  некорректного ввода:
        await upload_prices(
            watch_remnants=[
                {"Код": "123", "Цена": "abc"},  # Некорректное значение цены
            ],
            campaign_id=123,  # Некорректный тип, ожидается строка
            market_token=None  # Некорректный тип, ожидается строка
        )
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """ Загружает остатки на основе информации о часах, идентификатора кампании, токена рынка и идентификатора склада.

    Эта функция получает идентификаторы товаров, создает список остатков на основе
    остатков часов и обновляет их, отправляя данные пакетами по 2000.

    Аргументы:
        watch_remnants (list): Список словарей, содержащих информацию о часах.
            Каждый словарь должен содержать ключи:
                - "Код" (str): Код товара.
                - "Количество" (str): Количество товара (например, "1", ">10").
        campaign_id (str): Идентификатор кампании.
        market_token (str): Токен для доступа к API рынка.
        warehouse_id (str): Идентификатор склада, к которому относятся запасы.

    Возвращает:
        tuple: Кортеж из двух элементов:
            - list: Список словарей с не нулевыми остатками, которые были загружены на рынок.
            - list: Список всех остатков, которые были созданы.

    Примеры  корректного ввода:
        await upload_stocks(
            watch_remnants=[
                {"Код": "123", "Количество": "5"},
                {"Код": "456", "Количество": ">10"},
                {"Код": "789", "Количество": "1"}
            ],
            campaign_id="campaign_123",
            market_token="your_market_token",
            warehouse_id="warehouse_1"
        )
        
    Примеры  некорректного ввода:
        await upload_stocks(
            watch_remnants=[
                {"Код": "123", "Количество": "abc"},  # Некорректное значение количества
            ],
            campaign_id=123,  # Некорректный тип, ожидается строка
            market_token=None,  # Некорректный тип, ожидается строка
            warehouse_id=456  # Некорректный тип, ожидается строка
        )
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """ Главная функция для обновления остатков и цен на товары в системе FBS и DBS.

    Эта функция выполняет следующие действия:
    1. Загружает необходимые переменные окружения.
    2. Скачивает остатки товаров.
    3. Обновляет остатки и цены для кампаний FBS и DBS.
    
    Вызывает исключения в случае ошибок соединения или превышения времени ожидания.

    Примеры  корректного ввода:
        Если все переменные окружения корректны и все функции, вызываемые в этой функции,
        работают без ошибок, то функция выполнит обновление остатков и цен.
        
    Примеры  некорректного ввода:
        Если переменные окружения, такие как MARKET_TOKEN, FBS_ID, DBS_ID и т.д., не заданы,
        либо если они имеют неверный формат или значения, функция вызовет исключение,
        и выполнение будет прервано. Например:
        - Если MARKET_TOKEN не задан, будет выброшено исключение при попытке его использования.
        - Если функции, такие как download_stock() или upload_prices(), вызовут исключение,
          то выполнение также будет прервано, и будет выведено сообщение об ошибке.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
       
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

       
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
       
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
