from asyncio import Queue

from asyncpgsa.connection import compile_query
from asyncpgsa.pgsingleton import CursorInterface

from .mockpool import MockSAPool
from .mockconnection import MockConnection


class MockPG:
    def __init__(self):

        # connection_class = SAConnection
        # self.connection = connection_class()
        self.connection = MockConnection()
        self.__pool = MockSAPool(connection=self.connection)

    def get_completed_queries(self):
        return self.connection.completed_queries

    def set_database_results(self, *results):
        self.connection.results = Queue()  # reset queue
        for result in results:
            self.connection.results.put_nowait(result)

    def query(self, query, *args, **kwargs):
        compiled_q, compiled_args = compile_query(query)
        query, args = compiled_q, compiled_args or args

        return MockQueryContextManager(self.connection, query, args)

    def __getattr__(self, item):
        if item in ('execute', 'fetch', 'fetchval', 'fetchrow'):
            return getattr(self.connection, item)

    def transaction(self, **kwargs):
        return self.__pool.transaction(**kwargs)


class MockQueryContextManager:
    def __init__(self, connection, query, args=None):
        self.connection = connection
        self.query = query
        self.args = args
        self.cursor = None

    def __enter__(self):
        raise RuntimeError('Must use "async with"')

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aenter__(self):
        ps = await self.connection.prepare(self.query)
        self.cursor = ps.cursor()
        return CursorInterface(self.cursor)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
