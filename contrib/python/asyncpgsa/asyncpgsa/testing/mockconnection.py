from asyncio import Queue

from asyncpg.connection import Connection, ConnectionMeta

from .mockpreparedstmt import MockPreparedStatement

results = Queue()
completed_queries = []


def __subclasscheck__(cls, subclass):
    if subclass == MockConnection:
        return True
    return old_subclass_check(cls, subclass)

old_subclass_check = ConnectionMeta.__subclasscheck__
ConnectionMeta.__subclasscheck__ = __subclasscheck__


class MockConnection:
    __slots__ = Connection.__slots__

    @property
    def completed_queries(self):
        return completed_queries

    @property
    def results(self):
        return results

    @results.setter
    def results(self, result):
        global results
        results = result

    def set_database_results(self, *dbresults):
        self.results = Queue()
        for result in dbresults:
            self.results.put_nowait(result)

    async def general_query(self, query, *args, **kwargs):
        completed_queries.append((query, *args, kwargs))
        return results.get_nowait()

    execute = fetch = fetchval = fetchrow = general_query

    async def prepare(self, query, *, timeout=None):
        return MockPreparedStatement(self, query, None)

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def __await__(self):
        async def get_conn():
            return self
        return get_conn()
