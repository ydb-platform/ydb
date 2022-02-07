import os


class Config(object):
    _ydb_endpoint = os.getenv("YDB_ENDPOINT")
    _ydb_database = os.getenv("YDB_DATABASE")
    _ydb_path = os.getenv("YDB_PATH", "")

    _reservation_period_minutes = int(os.getenv("RESERVATION_PERIOD_MINUTES", "60"))

    @classmethod
    def ydb_endpoint(cls):
        return cls._ydb_endpoint

    @classmethod
    def ydb_database(cls):
        return cls._ydb_database

    @classmethod
    def ydb_path(cls):
        return cls._ydb_path

    @classmethod
    def reservation_period_minutes(cls):
        return cls._reservation_period_minutes
