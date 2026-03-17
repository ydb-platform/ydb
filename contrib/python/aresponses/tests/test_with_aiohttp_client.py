import aiohttp
import pytest
from aiohttp import web


@pytest.fixture()
def loop(event_loop):
    """replace aiohttp loop fixture with pytest-asyncio fixture"""
    return event_loop


def make_app():
    app = web.Application()

    async def constant_handler(request):
        return web.Response(text="42")

    async def ip_handler(request):
        protocol = request.query["protocol"]
        ip = await get_ip_address(protocol=protocol)
        return web.Response(text=f"ip is {ip}")

    app.add_routes([web.get("/constant", constant_handler)])
    app.add_routes([web.get("/ip", ip_handler)])
    return app


async def get_ip_address(protocol):
    async with aiohttp.ClientSession() as s:
        async with s.get(f"{protocol}://httpbin.org/ip") as resp:
            ip = (await resp.json())["origin"]
            return ip


@pytest.mark.asyncio
async def test_app_simple_endpoint(aiohttp_client):
    client = await aiohttp_client(make_app())
    r = await client.get("/constant")
    assert (await r.text()) == "42"


@pytest.mark.asyncio
async def test_app_simple_endpoint_with_aresponses(aiohttp_client, aresponses):
    """
    when testing your own aiohttp server you must setup passthrough to it

    Ideally this wouldn't be necessary but haven't figured that out yet.
    Perhaps all local calls should be passthrough.
    """
    aresponses.add("127.0.0.1:4241", response=aresponses.passthrough)

    client = await aiohttp_client(make_app(), server_kwargs={"port": 4241})
    r = await client.get("/constant")
    assert (await r.text()) == "42"


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["http", "https"])
async def test_app_with_subrequest_using_aresponses(
    aiohttp_client, aresponses, protocol
):
    """
    but passthrough doesn't work if the handler itself makes an aiohttp https request
    """
    aresponses.add_local_passthrough(repeat=1)
    aresponses.add("httpbin.org", response={"origin": "1.2.3.4"})

    client = await aiohttp_client(make_app())
    r = await client.get(f"/ip?protocol={protocol}")
    body = await r.text()
    assert r.status == 200, body
    assert "ip is" in (await r.text())
    aresponses.assert_plan_strictly_followed()
