import ydb
from ydb.library.yql.providers.generic.connector.tests.utils.settings import Settings


def make_client(s: Settings.Ydb) -> ydb.Driver:
    endpoint = f"grpc://{s.host_external}:{s.port_external}"

    driver = ydb.Driver(endpoint=endpoint, database=s.dbname, credentials=ydb.AnonymousCredentials())
    driver.wait(timeout=5)
    return driver
