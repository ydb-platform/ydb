pytest-aiohttp
==============

pytest plugin for aiohttp support

The library provides useful fixtures for creation test aiohttp server and client.


Installation
------------

.. code-block:: console

    $ pip install pytest-aiohttp

Add ``asyncio_mode = auto`` line to `pytest configuration
<https://docs.pytest.org/en/latest/customize.html>`_ (see `pytest-asyncio modes
<https://github.com/pytest-dev/pytest-asyncio#modes>`_ for details).  The plugin works
with ``strict`` mode also.



Usage
-----

Write tests in `pytest-asyncio <https://github.com/pytest-dev/pytest-asyncio>`_ style
using provided fixtures for aiohttp test server and client creation. The plugin provides
resources cleanup out-of-the-box.

The simple usage example:

.. code-block:: python

    from aiohttp import web


    async def hello(request):
        return web.Response(body=b"Hello, world")


    def create_app():
        app = web.Application()
        app.router.add_route("GET", "/", hello)
        return app


    async def test_hello(aiohttp_client):
        client = await aiohttp_client(create_app())
        resp = await client.get("/")
        assert resp.status == 200
        text = await resp.text()
        assert "Hello, world" in text


See `aiohttp documentation <https://docs.aiohttp.org/en/stable/testing.html#pytest>` for
more details about fixtures usage.
