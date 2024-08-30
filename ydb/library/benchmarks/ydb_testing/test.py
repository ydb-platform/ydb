import ydb


# DATABASE_ENDPOINT = "grpcs://lb.etnvsjbk7kh1jc6bbfi8.ydb.mdb.yandexcloud.net:2135"
DATABASE_ENDPOINT = "grpc://ydb-ru.yandex.net:2135" 
# DATABASE_PATH = "/ru-central1/b1ggceeul2pkher8vhb6/etnvsjbk7kh1jc6bbfi8"
DATABASE_PATH = "/ru/yql/prod/reports"

def test():
    creds = ydb.AccessTokenCredentials("")
    with ydb.Driver(
        endpoint=DATABASE_ENDPOINT,
        database=DATABASE_PATH,
        credentials=creds
    ) as driver:
        driver.wait(timeout=15)