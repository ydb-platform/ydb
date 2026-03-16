

class MockTransactionManager:
    def __init__(self, pool, connection):
        self.pool = pool
        self.connection = connection

    def __enter__(self):
        raise RuntimeError('Must use "async with" for a transaction')

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
