from io import BytesIO
import json

import requests
from airflow.hooks.base import BaseHook
from minio import Minio


def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"

    api = BaseHook.get_connection("stock_api")
    headers = api.extra_dejson["headers"]

    response = requests.get(url, headers=headers)

    return json.dumps(response.json()["chart"]["result"][0])


def _store_prices(stock):
    minio = BaseHook.get_connection("minio")

    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )

    bucket_name = "stock-market"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    stock_json = json.loads(stock)
    symbol = stock_json["meta"]["symbol"]
    data = json.dumps(stock_json, ensure_ascii=False).encode("utf8")

    stored_object = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
    )

    return f"{stored_object.bucket_name}/{symbol}"
