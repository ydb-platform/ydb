from typing import Awaitable, Callable

import pytest

from aiohttp import web
from aiohttp.test_utils import TestClient
from aiohttp_remotes import (
    XForwardedFiltered,
    XForwardedRelaxed,
    XForwardedStrict,
    setup as _setup,
)

_Client = Callable[[web.Application], Awaitable[TestClient]]


async def test_x_forwarded_relaxed_ok(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedRelaxed())
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={
            "X-Forwarded-For": "10.10.10.10",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "example.com",
        },
    )
    assert resp.status == 200


async def test_x_forwarded_relaxed_no_forwards(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        url = cl.make_url("/")
        assert url.host is not None
        assert url.port is not None
        host = url.host + ":" + str(url.port)
        assert request.host == host
        assert request.scheme == "http"
        assert not request.secure
        assert request.remote == "127.0.0.1"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedRelaxed())
    cl = await aiohttp_client(app)
    resp = await cl.get("/")
    assert resp.status == 200


async def test_x_forwarded_relaxed_multiple_for(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedRelaxed())
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers=[
            ("X-Forwarded-For", "10.10.10.10"),
            ("X-Forwarded-For", "20.20.20.20"),
            ("X-Forwarded-Proto", "https"),
            ("X-Forwarded-Host", "example.com"),
        ],
    )
    assert resp.status == 400


async def test_x_forwarded_relaxed_multiple_proto(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedRelaxed())
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers=[
            ("X-Forwarded-For", "10.10.10.10"),
            ("X-Forwarded-Proto", "http"),
            ("X-Forwarded-Proto", "https"),
            ("X-Forwarded-Host", "example.com"),
        ],
    )
    assert resp.status == 400


async def test_x_forwarded_relaxed_multiple_host(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedRelaxed())
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers=[
            ("X-Forwarded-For", "10.10.10.10"),
            ("X-Forwarded-Proto", "http"),
            ("X-Forwarded-Host", "example.org"),
            ("X-Forwarded-Host", "example.com"),
        ],
    )
    assert resp.status == 400


async def test_x_forwarded_filtered_ok(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered(["11.0.0.0/8"]))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={
            "X-Forwarded-For": "10.10.10.10, 11.11.11.11",
            "X-Forwarded-Proto": "https, http",
            "X-Forwarded-Host": "example.com",
        },
    )
    assert resp.status == 200


def test_x_forwarded_filtered_invalid_config() -> None:
    for invalid in ("127.0.0.1", "10.0.0.0/8", 42):
        with pytest.raises(TypeError):
            XForwardedFiltered(invalid)  # type: ignore


async def test_x_forwarded_filtered_no_forwards(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        url = cl.make_url("/")
        assert url.host is not None
        assert url.port is not None
        host = url.host + ":" + str(url.port)
        assert request.host == host
        assert request.scheme == "http"
        assert not request.secure
        assert request.remote == "127.0.0.1"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered(["127.0.0.1"]))
    cl = await aiohttp_client(app)
    resp = await cl.get("/")
    assert resp.status == 200


async def test_x_forwarded_filtered_all_filtered(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered({"10.0.0.0/8"}))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={
            "X-Forwarded-For": "10.10.10.10, 10.0.0.1",
            "X-Forwarded-Proto": "https, http",
            "X-Forwarded-Host": "example.com",
        },
    )
    assert resp.status == 200


async def test_x_forwarded_filtered_one_proto(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered({"11.11.11.11"}))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={
            "X-Forwarded-For": "10.10.10.10, 11.11.11.11",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "example.com",
        },
    )
    assert resp.status == 200


async def test_x_forwarded_filtered_no_proto_or_host(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        url = cl.make_url("/")
        assert url.host is not None
        assert url.port is not None
        host = url.host + ":" + str(url.port)
        assert request.host == host
        assert request.scheme == "http"
        assert not request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered({"11.11.11.11"}))
    cl = await aiohttp_client(app)
    resp = await cl.get("/", headers={"X-Forwarded-For": "10.10.10.10, 11.11.11.11"})
    assert resp.status == 200


async def test_x_forwarded_filtered_too_many_headers(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered({"10.0.0.0/8"}))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers=[
            ("X-Forwarded-For", "10.10.10.10"),
            ("X-Forwarded-Proto", "https"),
            ("X-Forwarded-Proto", "http"),
            ("X-Forwarded-Host", "example.com"),
        ],
    )
    assert resp.status == 400


async def test_x_forwarded_invalid_remote_ip(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedFiltered({"10.0.0.0/8"}))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers=[
            ("X-Forwarded-For", "example.com"),
            ("X-Forwarded-Proto", "https"),
            ("X-Forwarded-Host", "example.com"),
        ],
    )
    assert resp.status == 400
    assert resp.reason == "Invalid X-Forwarded-For header"


async def test_x_forwarded_strict_ok(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["127.0.0.1"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={
            "X-Forwarded-For": "10.10.10.10",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "example.com",
        },
    )
    assert resp.status == 200


async def test_x_forwarded_strict_no_proto(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "http"
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["127.0.0.1"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={"X-Forwarded-For": "10.10.10.10", "X-Forwarded-Host": "example.com"},
    )
    assert resp.status == 200


async def test_x_forwarded_strict_no_host(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host.startswith("127.0.0.1:")
        assert request.scheme == "https"
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["127.0.0.1"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/", headers={"X-Forwarded-For": "10.10.10.10", "X-Forwarded-Proto": "https"}
    )
    assert resp.status == 200


async def test_x_forwarded_strict_too_many_headers(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.host == "example.com"
        assert request.scheme == "https"
        assert request.secure
        assert request.remote == "10.10.10.10"

        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["127.0.0.1"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers=[
            ("X-Forwarded-For", "10.10.10.10"),
            ("X-Forwarded-Proto", "https"),
            ("X-Forwarded-Proto", "http"),
            ("X-Forwarded-Host", "example.com"),
        ],
    )
    assert resp.status == 400


async def test_x_forwarded_strict_too_many_protos(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["127.0.0.1"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get(
        "/",
        headers={
            "X-Forwarded-For": "10.10.10.10",
            "X-Forwarded-Proto": "https, http, https",
        },
    )
    assert resp.status == 400


async def test_x_forwarded_strict_too_many_for(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["127.0.0.1"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get("/", headers={"X-Forwarded-For": "10.10.10.10, 11.11.11.11"})
    assert resp.status == 400


async def test_x_forwarded_strict_untrusted_ip(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["20.20.20.20"]]))
    cl = await aiohttp_client(app)
    resp = await cl.get("/", headers={"X-Forwarded-For": "10.10.10.10"})
    assert resp.status == 400


async def test_x_forwarded_strict_whitelist(aiohttp_client: _Client) -> None:
    async def handler(request: web.Request) -> web.Response:
        assert request.remote == "127.0.0.1"
        return web.Response()

    app = web.Application()
    app.router.add_get("/", handler)
    await _setup(app, XForwardedStrict([["20.20.20.20"]], white_paths=["/"]))
    cl = await aiohttp_client(app)
    resp = await cl.get("/", headers={"X-Forwarded-For": "10.10.10.10"})
    assert resp.status == 200
