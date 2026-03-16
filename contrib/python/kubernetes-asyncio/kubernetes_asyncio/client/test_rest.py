import asyncio
import ssl
import unittest
from unittest.mock import AsyncMock
import aiohttp
from kubernetes_asyncio.client.rest import RESTClientObject
from kubernetes_asyncio.client.configuration import Configuration
class TestRESTClientObject(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = Configuration()


    async def test_rest_request_timeout(self):
        rest_api = RESTClientObject(configuration=self.config)
        for request_timeout, expected_timeout_arg in [
                (None, aiohttp.ClientTimeout()),
                (5.0, aiohttp.ClientTimeout(total=5.0)),
                (3, aiohttp.ClientTimeout(total=3)),
                ((5, 7), aiohttp.ClientTimeout(connect=5, sock_connect=5, sock_read=7)),
                (aiohttp.ClientTimeout(total=None), aiohttp.ClientTimeout(total=None)),
        ]:
            with self.subTest(request_timeout=request_timeout, expected_timeout_arg=expected_timeout_arg):
                mock_request = AsyncMock()
                rest_api.pool_manager.request = mock_request
                await rest_api.request(method="GET", url="http://test-api", _preload_content=False, _request_timeout=request_timeout)
                mock_request.assert_called_once_with(
                    method="GET",
                    url="http://test-api",
                    timeout=expected_timeout_arg,
                    headers={"Content-Type": "application/json"}
                )

    async def test_disable_ssl_verification(self):
        configuration = Configuration()
        configuration.disable_strict_ssl_verification = True
        rest_api = RESTClientObject(configuration=configuration)
        ssl_context = rest_api.pool_manager._connector._ssl
        self.assertEqual(ssl_context.verify_flags & ssl.VERIFY_X509_STRICT, 0)
