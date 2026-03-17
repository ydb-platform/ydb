import ssl
from typing import Awaitable, Callable

import pytest
from yarl import URL

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer, make_mocked_request
from aiohttp_remotes import Secure, setup as _setup

_Server = Callable[..., Awaitable[TestServer]]
_Client = Callable[..., Awaitable[TestClient]]


async def test_secure_ok(
    aiohttp_client: _Client,
    aiohttp_server: _Server,
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Secure())
    srv = await aiohttp_server(app, ssl=ssl_ctx)
    conn = aiohttp.TCPConnector(ssl=client_ssl_ctx)
    cl = await aiohttp_client(srv, connector=conn)
    resp = await cl.get("/")
    print(resp.request_info.url)
    assert resp.status == 200
    assert resp.headers["X-Frame-Options"] == "DENY"
    expected = "max-age=31536000; includeSubDomains"
    assert resp.headers["Strict-Transport-Security"] == expected
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-XSS-Protection"] == "1; mode=block"


async def test_secure_redirect(
    aiohttp_client: _Client,
    aiohttp_server: _Server,
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    secure = Secure()
    await _setup(app, secure)
    http_srv = await aiohttp_server(app)
    https_srv = await aiohttp_server(app, ssl=ssl_ctx)
    secure._redirect_url = https_srv.make_url("/")
    conn = aiohttp.TCPConnector(ssl=client_ssl_ctx)
    async with aiohttp.ClientSession(connector=conn) as cl:
        url = http_srv.make_url("/")
        resp = await cl.get(url)
        assert resp.status == 200
        assert resp.request_info.url.scheme == "https"


async def test_secure_no_redirection(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Secure(redirect=False))
    cl = await aiohttp_client(app)
    resp = await cl.get("/")
    assert resp.status == 400
    assert resp.request_info.url.scheme == "http"


async def test_no_x_frame(
    aiohttp_client: _Client,
    aiohttp_server: _Server,
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Secure(x_frame=None))
    srv = await aiohttp_server(app, ssl=ssl_ctx)
    conn = aiohttp.TCPConnector(ssl=client_ssl_ctx)
    cl = await aiohttp_client(srv, connector=conn)
    resp = await cl.get("/")
    print(resp.request_info.url)
    assert resp.status == 200
    assert "X-Frame-Options" not in resp.headers
    expected = "max-age=31536000; includeSubDomains"
    assert resp.headers["Strict-Transport-Security"] == expected
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-XSS-Protection"] == "1; mode=block"


async def test_no_sts(
    aiohttp_client: _Client,
    aiohttp_server: _Server,
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Secure(sts=None))
    srv = await aiohttp_server(app, ssl=ssl_ctx)
    conn = aiohttp.TCPConnector(ssl=client_ssl_ctx)
    cl = await aiohttp_client(srv, connector=conn)
    resp = await cl.get("/")
    print(resp.request_info.url)
    assert resp.status == 200
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert "Strict-Transport-Security" not in resp.headers
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-XSS-Protection"] == "1; mode=block"


async def test_no_cto(
    aiohttp_client: _Client,
    aiohttp_server: _Server,
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Secure(cto=None))
    srv = await aiohttp_server(app, ssl=ssl_ctx)
    conn = aiohttp.TCPConnector(ssl=client_ssl_ctx)
    cl = await aiohttp_client(srv, connector=conn)
    resp = await cl.get("/")
    print(resp.request_info.url)
    assert resp.status == 200
    assert resp.headers["X-Frame-Options"] == "DENY"
    expected = "max-age=31536000; includeSubDomains"
    assert resp.headers["Strict-Transport-Security"] == expected
    assert "X-Content-Type-Options" not in resp.headers
    assert resp.headers["X-XSS-Protection"] == "1; mode=block"


async def test_no_xss(
    aiohttp_client: _Client,
    aiohttp_server: _Server,
    ssl_ctx: ssl.SSLContext,
    client_ssl_ctx: ssl.SSLContext,
) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, Secure(xss=None))
    srv = await aiohttp_server(app, ssl=ssl_ctx)
    conn = aiohttp.TCPConnector(ssl=client_ssl_ctx)
    cl = await aiohttp_client(srv, connector=conn)
    resp = await cl.get("/")
    print(resp.request_info.url)
    assert resp.status == 200
    assert resp.headers["X-Frame-Options"] == "DENY"
    expected = "max-age=31536000; includeSubDomains"
    assert resp.headers["Strict-Transport-Security"] == expected
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert "X-XSS-Protection" not in resp.headers


async def test_default_redirect() -> None:
    s = Secure()

    async def handler(request: web.Request) -> web.Response:
        return web.Response()  # never executed

    req = make_mocked_request("GET", "/path", headers={"Host": "example.com"})
    with pytest.raises(web.HTTPPermanentRedirect) as ctx:
        await s.middleware(req, handler)
    assert ctx.value.location == URL("https://example.com/path")


def test_non_https_redirect_url() -> None:
    with pytest.raises(ValueError):
        Secure(redirect_url="http://example.com")


def test_redirect_url_with_path() -> None:
    with pytest.raises(ValueError):
        Secure(redirect_url="https://example.com/path/to")


def test_redirect_url_ok() -> None:
    Secure(redirect_url="https://example.com")
