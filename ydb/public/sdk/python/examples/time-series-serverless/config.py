import os


class Config(object):
    def __init__(self):
        self._endpoint = os.getenv("YDB_ENDPOINT")
        self._database = os.getenv("YDB_DATABASE")
        self._path = os.getenv("YDB_PATH")

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def database(self):
        return self._database

    @property
    def path(self):
        return self._path

    @property
    def full_path(self):
        return os.path.join(self.database, self._path)


ydb_configuration = Config()
