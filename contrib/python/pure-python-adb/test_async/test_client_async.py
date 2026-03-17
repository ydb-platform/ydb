"""Unit tests for the `ClientAsync` class.

"""


import asyncio
import sys
import unittest

sys.path.insert(0, '..')

from ppadb.client_async import ClientAsync

from .async_wrapper import awaiter
from .patchers import FakeStreamReader, FakeStreamWriter, async_patch


class TestClientAsync(unittest.TestCase):
    def setUp(self):
        self.client = ClientAsync()

    @awaiter
    async def test_create_connection_fail(self):
        with self.assertRaises(RuntimeError):
            await self.client.create_connection()

    @awaiter
    async def test_device_returns_none(self):
        with async_patch('asyncio.open_connection', return_value=(FakeStreamReader(), FakeStreamWriter())):
            with async_patch('{}.FakeStreamReader.read'.format(__name__), side_effect=[b'OKAY', b'0000', b'']):
                self.assertIsNone(await self.client.device('serial'))

    @awaiter
    async def test_device(self):
        with async_patch('asyncio.open_connection', return_value=(FakeStreamReader(), FakeStreamWriter())):
            with async_patch('{}.FakeStreamReader.read'.format(__name__), side_effect=[b'OKAY', b'000b', b'serial test']):
                self.assertIsNotNone(await self.client.device('serial'))


if __name__ == '__main__':
    unittest.main()
