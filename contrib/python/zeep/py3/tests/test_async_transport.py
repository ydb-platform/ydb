import pytest
from lxml import etree
from pretend import stub
from pytest_httpx import HTTPXMock

from zeep import exceptions
from zeep.cache import InMemoryCache
from zeep.transports import AsyncTransport


@pytest.mark.requests
def test_no_cache(event_loop):
    transport = AsyncTransport()
    assert transport.cache is None


@pytest.mark.requests
def test_load(httpx_mock):
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = AsyncTransport(cache=cache)

    httpx_mock.add_response(url="http://tests.python-zeep.org/test.xml", content="x")
    result = transport.load("http://tests.python-zeep.org/test.xml")
    assert result == b"x"


@pytest.mark.requests
@pytest.mark.asyncio
def test_load_cache(httpx_mock):
    cache = InMemoryCache()
    transport = AsyncTransport(cache=cache)

    httpx_mock.add_response(url="http://tests.python-zeep.org/test.xml", content="x")
    result = transport.load("http://tests.python-zeep.org/test.xml")
    assert result == b"x"

    assert cache.get("http://tests.python-zeep.org/test.xml") == b"x"


@pytest.mark.requests
@pytest.mark.asyncio
async def test_post(httpx_mock: HTTPXMock):
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = AsyncTransport(cache=cache)

    envelope = etree.Element("Envelope")

    httpx_mock.add_response(url="http://tests.python-zeep.org/test.xml", content="x")
    result = await transport.post_xml(
        "http://tests.python-zeep.org/test.xml", envelope=envelope, headers={}
    )

    assert result.content == b"x"
    await transport.aclose()


@pytest.mark.requests
@pytest.mark.asyncio
async def test_session_close(httpx_mock: HTTPXMock):
    transport = AsyncTransport()
    return await transport.aclose()


@pytest.mark.requests
@pytest.mark.asyncio
async def test_http_error(httpx_mock: HTTPXMock):
    transport = AsyncTransport()

    httpx_mock.add_response(
        url="http://tests.python-zeep.org/test.xml", content="x", status_code=500
    )
    with pytest.raises(exceptions.TransportError) as exc:
        transport.load("http://tests.python-zeep.org/test.xml")
        assert exc.value.status_code == 500
        assert exc.value.message is None
