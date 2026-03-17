"""Unit tests for `ConnectionAsync` class.

"""


import asyncio
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, '..')

from ppadb.connection_async import ConnectionAsync

from .async_wrapper import awaiter
from .patchers import FakeStreamReader, FakeStreamWriter, async_patch


class TestConnectionAsync(unittest.TestCase):
    @awaiter
    async def test_connect_close(self):
        with async_patch('asyncio.open_connection', return_value=(FakeStreamReader(), FakeStreamWriter())):
            conn = ConnectionAsync()
            await conn.connect()
            self.assertIsNotNone(conn.reader)
            self.assertIsNotNone(conn.writer)

        await conn.close()
        self.assertIsNone(conn.reader)
        self.assertIsNone(conn.writer)

    @awaiter
    async def test_connect_close_catch_oserror(self):
        with async_patch('asyncio.open_connection', return_value=(FakeStreamReader(), FakeStreamWriter())):
            conn = ConnectionAsync()
            await conn.connect()
            self.assertIsNotNone(conn.reader)
            self.assertIsNotNone(conn.writer)

        with patch('{}.FakeStreamWriter.close'.format(__name__), side_effect=OSError):
            await conn.close()
            self.assertIsNone(conn.reader)
            self.assertIsNone(conn.writer)

    @awaiter
    async def test_connect_with_timeout(self):
        with self.assertRaises(RuntimeError):
            with async_patch('asyncio.open_connection', side_effect=asyncio.TimeoutError):
                conn = ConnectionAsync(timeout=1)
                await conn.connect()


if __name__ == '__main__':
    unittest.main()
