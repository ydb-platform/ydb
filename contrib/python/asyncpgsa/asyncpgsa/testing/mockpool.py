from asyncpgsa import compile_query
from asyncpg import protocol
from asyncpg.pool import Pool

from .mockconnection import MockConnection
from .mocktransactionmanager import MockTransactionManager


class MockSAPool(Pool):
    def __init__(self, connection=None):
        super().__init__(min_size=1,
                         max_size=1,
                         max_queries=1,
                         max_inactive_connection_lifetime=300.0,
                         setup=None,
                         loop=None,
                         init=None,
                         connection_class=MockConnection,
                         record_class=protocol.Record)

        self.connection = connection
        if not self.connection:
            self.connection = MockConnection()

    def __getattr__(self, item):

        raise Exception('Sorry, {} doesnt exist yet. '
                        'Consider making a PR.'.format(item))

    async def _new_connection(self, timeout=None):
        return self.connection

    async def acquire(self, *, timeout=None):
        return self.connection

    async def release(self, connection):
        pass

    async def general_query(self, query, *args, **kwargs):
        q, a = compile_query(query)
        return self.connection.general_query(q, *args, **kwargs)

    def transaction(self, **kwargs):
        return MockTransactionManager(self, self.connection)

    def terminate(self):
        pass

    def __await__(self):
        async def _():
            return self
        return _()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def close(self):
        pass
