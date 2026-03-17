import asyncio
import socket
import ssl
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
)

import pytest

import aiohttp
from aiohttp import web
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import DefaultResolver
from aiohttp.test_utils import TestClient, unused_port
from aiohttp_remotes import Cloudflare, setup as _setup

_Client = Callable[[web.Application], Awaitable[TestClient]]
_CloudSession = Callable[..., Awaitable[aiohttp.ClientSession]]


try:
    from aiohttp.abc import ResolveResult
except ImportError:
    ResolveResult = Dict[str, Any]  # type: ignore[assignment, misc]


class FakeResolver(AbstractResolver):
    _LOCAL_HOST = {0: "127.0.0.1", socket.AF_INET: "127.0.0.1", socket.AF_INET6: "::1"}

    def __init__(self, fakes: Mapping[str, int]) -> None:
        """fakes -- dns -> port dict"""
        self._fakes = fakes
        self._resolver: AbstractResolver = DefaultResolver()

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> List[ResolveResult]:
        fake_port = self._fakes.get(host)
        if fake_port is not None:
            return [
                {
                    "hostname": host,
                    "host": self._LOCAL_HOST[family],
                    "port": fake_port,
                    "family": family,
                    "proto": 0,
                    "flags": socket.AI_NUMERICHOST,
                }
            ]
        else:
            return await self._resolver.resolve(host, port, family)

    async def close(self) -> None:
        await self._resolver.close()


class FakeCloudfare:
    def __init__(
        self,
        *,
        ipv4: Sequence[str] = ("127.0.0.0/16",),
        ipv6: Sequence[str] = ("::/16",),
        ssl_ctx: ssl.SSLContext
    ) -> None:
        self._ipv4 = ipv4
        self._ipv6 = ipv6
        self.app = web.Application()
        self.app.router.add_get("/ips-v4", self.ipv4)
        self.app.router.add_get("/ips-v6", self.ipv6)

        self.runner: Optional[web.AppRunner] = None
        self.ssl_context = ssl_ctx

    async def start(self) -> Dict[str, int]:
        port = unused_port()
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", port, ssl_context=self.ssl_context)
        await site.start()
        return {"www.cloudflare.com": port}

    async def stop(self) -> None:
        assert self.runner is not None
        await self.runner.cleanup()

    async def ipv4(self, request: web.Request) -> web.Response:
        return web.Response(text="\n".join(self._ipv4))

    async def ipv6(self, request: web.Request) -> web.Response:
        return web.Response(text="\n".join(self._ipv6))


@pytest.fixture
async def cloudfare_session(
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> AsyncIterator[_CloudSession]:
    sessions = []

    async def go(**kwargs: Any) -> aiohttp.ClientSession:
        kwargs.setdefault("ssl_ctx", ssl_ctx)
        fake = FakeCloudfare(**kwargs)
        info = await fake.start()
        resolver = FakeResolver(info)
        connector = aiohttp.TCPConnector(resolver=resolver, ssl=client_ssl_ctx)

        session = aiohttp.ClientSession(connector=connector)
        sessions.append(session)
        return session

    yield go

    for s in sessions:
        await s.close()

    await asyncio.sleep(0.01)


async def test_cloudfare_ok(
    aiohttp_client: _Client, cloudfare_session: _CloudSession
) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.remote == "10.10.10.10"

        return web.Response()

    cf_client = await cloudfare_session()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Cloudflare(cf_client))
    cl = await aiohttp_client(app)
    async with cl.get("/", headers={"CF-CONNECTING-IP": "10.10.10.10"}) as resp:
        assert resp.status == 200


async def test_cloudfare_no_networks(
    aiohttp_client: _Client, cloudfare_session: _CloudSession
) -> None:
    cf_client = await cloudfare_session(ipv4=[], ipv6=[])

    app = web.Application()
    with pytest.raises(RuntimeError):
        await _setup(app, Cloudflare(cf_client))


async def test_cloudfare_not_cloudfare(
    aiohttp_client: _Client, cloudfare_session: _CloudSession
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    cf_client = await cloudfare_session(ipv4=["10.0.0.0"], ipv6=["10::"])

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Cloudflare(cf_client))
    cl = await aiohttp_client(app)
    async with cl.get("/", headers={"CF-CONNECTING-IP": "10.10.10.10"}) as resp:
        assert resp.status == 400


async def test_cloudfare_garbage_config(
    aiohttp_client: _Client, cloudfare_session: _CloudSession
) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.remote == "10.10.10.10"

        return web.Response()

    cf_client = await cloudfare_session(ipv4=["127.0.0.0/16", "garbage"])

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Cloudflare(cf_client))
    cl = await aiohttp_client(app)
    async with cl.get("/", headers={"CF-CONNECTING-IP": "10.10.10.10"}) as resp:
        assert resp.status == 200
