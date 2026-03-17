import pytest
from siosocks.exceptions import SocksException


@pytest.mark.asyncio
async def test_socks_success(pair_factory, Client, socks):
    client = Client(
        socks_host=socks.host,
        socks_port=socks.port,
        socks_version=5,
        username="foo",
        password="bar",
    )
    async with pair_factory(client):
        pass


@pytest.mark.asyncio
async def test_socks_fail(pair_factory, Client, socks):
    client = Client(
        socks_host=socks.host,
        socks_port=socks.port,
        socks_version=5,
        username="bar",
        password="bar",
    )
    with pytest.raises(SocksException):
        async with pair_factory(client):
            pass
