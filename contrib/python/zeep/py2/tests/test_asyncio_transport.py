import aiohttp
import pytest
from aioresponses import aioresponses
from lxml import etree
from pretend import stub

from zeep import asyncio, exceptions
from zeep.cache import InMemoryCache


@pytest.mark.requests
def test_no_cache(event_loop):
    transport = asyncio.AsyncTransport(loop=event_loop)
    assert transport.cache is None


@pytest.mark.requests
def test_load(event_loop):
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = asyncio.AsyncTransport(loop=event_loop, cache=cache)

    with aioresponses() as m:
        m.get("http://tests.python-zeep.org/test.xml", body="x")
        result = transport.load("http://tests.python-zeep.org/test.xml")
        assert result == b"x"


@pytest.mark.requests
def test_load_cache(event_loop):
    cache = InMemoryCache()
    transport = asyncio.AsyncTransport(loop=event_loop, cache=cache)

    with aioresponses() as m:
        m.get("http://tests.python-zeep.org/test.xml", body="x")
        result = transport.load("http://tests.python-zeep.org/test.xml")
        assert result == b"x"

    assert cache.get("http://tests.python-zeep.org/test.xml") == b"x"


def test_cache_checks_type():
    cache = InMemoryCache()

    async def foo():
        pass

    with pytest.raises(TypeError):
        cache.add("x", foo())


@pytest.mark.requests
@pytest.mark.asyncio
async def test_post(event_loop):
    cache = stub(get=lambda url: None, add=lambda url, content: None)
    transport = asyncio.AsyncTransport(loop=event_loop, cache=cache)

    envelope = etree.Element("Envelope")

    with aioresponses() as m:
        m.post("http://tests.python-zeep.org/test.xml", body="x")
        result = await transport.post_xml(
            "http://tests.python-zeep.org/test.xml", envelope=envelope, headers={}
        )

        assert result.content == b"x"


@pytest.mark.requests
@pytest.mark.asyncio
async def test_session_close(event_loop):
    transport = asyncio.AsyncTransport(loop=event_loop)
    session = transport.session  # copy session object from transport
    del transport
    assert session.closed


@pytest.mark.requests
@pytest.mark.asyncio
async def test_session_no_close(event_loop):
    session = aiohttp.ClientSession(loop=event_loop)
    transport = asyncio.AsyncTransport(loop=event_loop, session=session)
    del transport
    assert not session.closed


@pytest.mark.requests
def test_http_error(event_loop):
    transport = asyncio.AsyncTransport(loop=event_loop)

    with aioresponses() as m:
        m.get("http://tests.python-zeep.org/test.xml", body="x", status=500)
        with pytest.raises(exceptions.TransportError) as exc:
            transport.load("http://tests.python-zeep.org/test.xml")
            assert exc.value.status_code == 500
            assert exc.value.message is None
