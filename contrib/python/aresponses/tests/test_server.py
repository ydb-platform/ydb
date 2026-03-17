import asyncio
import re

import aiohttp
import pytest
import sys
from aiohttp import ServerDisconnectedError

import aresponses as aresponses_mod

# example test in readme.md
from aresponses.errors import (
    NoRouteFoundError,
    UnusedRouteError,
    UnorderedRouteCallError,
)


@pytest.mark.asyncio
async def test_foo(aresponses):
    # text as response (defaults to status 200 response)
    aresponses.add("foo.com", "/", "get", "hi there!!")

    # custom status code response
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="error", status=500))

    # JSON response
    aresponses.add("foo.com", "/", "get", {"status": "ok"})

    # passthrough response (makes an actual network call)
    aresponses.add("httpstat.us", "/200", "get", aresponses.passthrough)

    # custom handler response
    def my_handler(request):
        return aresponses.Response(status=200, text=str(request.url))

    aresponses.add("foo.com", "/", "get", my_handler)

    url = "http://foo.com"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
            assert text == "hi there!!"

        async with session.get(url) as response:
            text = await response.text()
            assert text == "error"
            assert response.status == 500

        async with session.get(url) as response:
            text = await response.text()
            assert text == '{"status": "ok"}'

        async with session.get("https://httpstat.us/200") as response:
            text = await response.text()
        assert text == "200 OK"

        async with session.get(url) as response:
            text = await response.text()
            assert text == "http://foo.com/"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_fixture(aresponses):
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="hi"))

    url = "http://foo.com"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "hi"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_body_match(aresponses):
    aresponses.add(
        "foo.com",
        "/",
        "get",
        aresponses.Response(text="hi"),
        body_pattern=re.compile(r".*?apple.*"),
    )

    url = "http://foo.com"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, data={"fruit": "pineapple"}) as response:
                text = await response.text()
                assert text == "hi"
        except ServerDisconnectedError:
            pass

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_https(aresponses):
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="hi"))

    url = "https://foo.com"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "hi"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_context_manager():
    loop = asyncio.get_running_loop()
    async with aresponses_mod.ResponsesMockServer(loop=loop) as arsps:
        arsps.add("foo.com", "/", "get", aresponses_mod.Response(text="hi"))

        url = "http://foo.com"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                text = await response.text()
        assert text == "hi"

    arsps.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_bad_redirect(aresponses):
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="hi", status=301))
    url = "http://foo.com"
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
        await response.text()

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_regex(aresponses):
    aresponses.add(
        aresponses.ANY, aresponses.ANY, aresponses.ANY, aresponses.Response(text="hi")
    )
    aresponses.add(
        aresponses.ANY,
        aresponses.ANY,
        aresponses.ANY,
        aresponses.Response(text="there"),
    )

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com") as response:
            text = await response.text()
            assert text == "hi"

        async with session.get("http://bar.com") as response:
            text = await response.text()
            assert text == "there"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_callable(aresponses):
    def handler(request):
        return aresponses.Response(body=request.host)

    aresponses.add(aresponses.ANY, aresponses.ANY, aresponses.ANY, handler)
    aresponses.add(aresponses.ANY, aresponses.ANY, aresponses.ANY, handler)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com") as response:
            text = await response.text()
            assert text == "foo.com"

        async with session.get("http://bar.com") as response:
            text = await response.text()
            assert text == "bar.com"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_raw_response(aresponses):
    raw_response = b"""HTTP/1.1 200 OK\r
Date: Tue, 26 Dec 2017 05:47:50 GMT\r
\r
<html><body><h1>It works!</h1></body></html>
"""
    aresponses.add(
        aresponses.ANY,
        aresponses.ANY,
        aresponses.ANY,
        aresponses.RawResponse(raw_response),
    )

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com") as response:
            text = await response.text()
            assert "It works!" in text

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_querystring(aresponses):
    aresponses.add("foo.com", "/path", "get", aresponses.Response(text="hi"))

    url = "http://foo.com/path?reply=42"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "hi"

    aresponses.add(
        "foo.com",
        "/path2?reply=42",
        "get",
        aresponses.Response(text="hi"),
        match_querystring=True,
    )

    url = "http://foo.com/path2?reply=42"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "hi"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_querystring_not_match(aresponses):
    aresponses.add(
        "foo.com",
        "/path",
        "get",
        aresponses.Response(text="hi"),
        match_querystring=True,
    )
    aresponses.add(
        "foo.com",
        aresponses.ANY,
        "get",
        aresponses.Response(text="miss"),
        match_querystring=True,
    )
    aresponses.add(
        "foo.com",
        aresponses.ANY,
        "get",
        aresponses.Response(text="miss"),
        match_querystring=True,
    )

    url = "http://foo.com/path"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "hi"

    url = "http://foo.com/path?reply=42"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "miss"

    url = "http://foo.com/path?reply=43"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "miss"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_passthrough(aresponses):
    aresponses.add("httpstat.us", "/200", "get", aresponses.passthrough)

    url = "https://httpstat.us/200"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
    assert text == "200 OK"

    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_failure_not_called(aresponses):
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="hi"))
    with pytest.raises(UnusedRouteError):
        aresponses.assert_no_unused_routes()

    with pytest.raises(UnusedRouteError):
        aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_failure_no_match(aresponses):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("http://foo.com") as response:
                await response.text()
        except ServerDisconnectedError:
            pass
    with pytest.raises(NoRouteFoundError):
        aresponses.assert_all_requests_matched()

    with pytest.raises(NoRouteFoundError):
        aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_failure_bad_ordering(aresponses):
    aresponses.add("foo.com", "/a", "get", aresponses.Response(text="hi"))
    aresponses.add("foo.com", "/b", "get", aresponses.Response(text="hi"))

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com/b") as response:
            await response.text()
        async with session.get("http://foo.com/a") as response:
            await response.text()

    aresponses.assert_all_requests_matched()
    aresponses.assert_no_unused_routes()
    with pytest.raises(UnorderedRouteCallError):
        aresponses.assert_called_in_order()

    with pytest.raises(UnorderedRouteCallError):
        aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_failure_not_called_enough_times(aresponses):
    aresponses.add("foo.com", "/", "get", aresponses.Response(text="hi"), repeat=2)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com/") as response:
            await response.text()

    with pytest.raises(UnusedRouteError):
        aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_history(aresponses):
    aresponses.add(response=aresponses.Response(text="hi"), repeat=2)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com/b") as response:
            await response.text()
        async with session.get("http://bar.com/a") as response:
            await response.text()

    assert len(aresponses.history) == 2
    assert aresponses.history[0].request.host == "foo.com"
    assert aresponses.history[1].request.host == "bar.com"
    assert "Route(" in repr(aresponses.history[0].route)
    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_history_post(aresponses):
    """Ensure the request contents exist in the history"""
    aresponses.add(method_pattern="POST", response={"some": "response"})

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://bar.com/zzz", json={"greeting": "hello"}
        ) as response:
            response_data = await response.json()
            assert response_data == {"some": "response"}

    assert len(aresponses.history) == 1
    assert aresponses.history[0].request.host == "bar.com"
    request_data = await aresponses.history[0].request.json()
    assert request_data == {"greeting": "hello"}
    assert "Route(" in repr(aresponses.history[0].route)
    aresponses.assert_plan_strictly_followed()


@pytest.mark.asyncio
async def test_history_post_binary(aresponses):
    """
    Ensure the request contents exist in the history

    ...and that it can handle binary requests
    """
    binary_not_utf = b"o\xad<|6\xd2a\x116\x17\xdb\x98-60:"
    aresponses.add(method_pattern="POST", response={"some": "response"})

    async with aiohttp.ClientSession() as session:
        async with session.post("http://bar.com/zzz", data=binary_not_utf) as response:
            response_data = await response.json()
            assert response_data == {"some": "response"}

    assert len(aresponses.history) == 1
    assert aresponses.history[0].request.host == "bar.com"
    request_data = await aresponses.history[0].request.read()
    assert request_data == binary_not_utf


@pytest.fixture()
def _short_recursion_limit():
    # The default is 1000, but it takes time (seconds); 100 is much faster.
    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(100)
    yield
    sys.setrecursionlimit(old_limit)


@pytest.mark.asyncio
@pytest.mark.usefixtures("_short_recursion_limit")
async def test_not_exceeding_recursion_limit():
    loop = asyncio.get_running_loop()
    for _ in range(sys.getrecursionlimit()):
        async with aresponses_mod.ResponsesMockServer(loop=loop) as arsps:
            arsps.add("fake-host", "/", "get", "hello")
            async with aiohttp.ClientSession() as session:
                async with session.get("http://fake-host"):
                    pass
