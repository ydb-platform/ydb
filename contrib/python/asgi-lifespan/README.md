# asgi-lifespan

[![Build Status](https://dev.azure.com/florimondmanca/public/_apis/build/status/florimondmanca.asgi-lifespan?branchName=master)](https://dev.azure.com/florimondmanca/public/_build?definitionId=12)
[![Coverage](https://codecov.io/gh/florimondmanca/asgi-lifespan/branch/master/graph/badge.svg)](https://codecov.io/gh/florimondmanca/asgi-lifespan)
[![Package version](https://badge.fury.io/py/asgi-lifespan.svg)](https://pypi.org/project/asgi-lifespan)

Programmatically send startup/shutdown [lifespan](https://asgi.readthedocs.io/en/latest/specs/lifespan.html) events into [ASGI](https://asgi.readthedocs.io) applications. When used in combination with an ASGI-capable HTTP client such as [HTTPX](https://www.python-httpx.org), this allows mocking or testing ASGI applications without having to spin up an ASGI server.

## Features

- Send lifespan events to an ASGI app using `LifespanManager`.
- Support for [`asyncio`](https://docs.python.org/3/library/asyncio) and [`trio`](https://trio.readthedocs.io).
- Fully type-annotated.
- 100% test coverage.

## Installation

```bash
pip install 'asgi-lifespan==2.*'
```

## Usage

`asgi-lifespan` provides a `LifespanManager` to programmatically send ASGI lifespan events into an ASGI app. This can be used to programmatically startup/shutdown an ASGI app without having to spin up an ASGI server.

`LifespanManager` can run on either `asyncio` or `trio`, and will auto-detect the async library in use.

### Basic usage

```python
# example.py
from contextlib import asynccontextmanager
from asgi_lifespan import LifespanManager
from starlette.applications import Starlette

# Example lifespan-capable ASGI app. Any ASGI app that supports
# the lifespan protocol will do, e.g. FastAPI, Quart, Responder, ...

@asynccontextmanager
async def lifespan(app):
    print("Starting up!")
    yield
    print("Shutting down!")

app = Starlette(lifespan=lifespan)

async def main():
    async with LifespanManager(app) as manager:
        print("We're in!")

# On asyncio:
import asyncio; asyncio.run(main())

# On trio:
# import trio; trio.run(main)
```

Output:

```console
$ python example.py
Starting up!
We're in!
Shutting down!
```

### Sending lifespan events for testing

The example below demonstrates how to use `asgi-lifespan` in conjunction with [HTTPX](https://www.python-httpx.org) and `pytest` in order to send test requests into an ASGI app.

- Install dependencies:

```
pip install asgi-lifespan httpx starlette pytest pytest-asyncio
```

- Test script:

```python
# test_app.py
from contextlib import asynccontextmanager
import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route


@pytest_asyncio.fixture
async def app():
    @asynccontextmanager
    async def lifespan(app):
        print("Starting up")
        yield
        print("Shutting down")

    async def home(request):
        return PlainTextResponse("Hello, world!")

    app = Starlette(
        routes=[Route("/", home)],
        lifespan=lifespan,
    )

    async with LifespanManager(app) as manager:
        print("We're in!")
        yield manager.app


@pytest_asyncio.fixture
async def client(app):
    async with httpx.AsyncClient(app=app, base_url="http://app.io") as client:
        print("Client is ready")
        yield client


@pytest.mark.asyncio
async def test_home(client):
    print("Testing")
    response = await client.get("/")
    assert response.status_code == 200
    assert response.text == "Hello, world!"
    print("OK")
```

- Run the test suite:

```console
$ pytest -s test_app.py
======================= test session starts =======================

test_app.py Starting up
We're in!
Client is ready
Testing
OK
.Shutting down

======================= 1 passed in 0.88s =======================
```

### Accessing state

`LifespanManager` provisions a [lifespan state](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#lifespan-state) which persists data from the lifespan cycle for use in request/response handling.

For your app to be aware of it, be sure to use `manager.app` instead of the `app` itself when inside the context manager.

For example if using HTTPX as an async test client:

```python
async with LifespanManager(app) as manager:
    async with httpx.AsyncClient(app=manager.app) as client:
        ...
```

## API Reference

### `LifespanManager`

```python
def __init__(
    self,
    app: Callable,
    startup_timeout: Optional[float] = 5,
    shutdown_timeout: Optional[float] = 5,
)
```

An [asynchronous context manager](https://docs.python.org/3/reference/datamodel.html#async-context-managers) that starts up an ASGI app on enter and shuts it down on exit.

More precisely:

- On enter, start a `lifespan` request to `app` in the background, then send the `lifespan.startup` event and wait for the application to send `lifespan.startup.complete`.
- On exit, send the `lifespan.shutdown` event and wait for the application to send `lifespan.shutdown.complete`.
- If an exception occurs during startup, shutdown, or in the body of the `async with` block, it bubbles up and no shutdown is performed.

**Example**

```python
async with LifespanManager(app) as manager:
    # 'app' was started up.
    ...

# 'app' was shut down.
```

**Parameters**

- `app` (`Callable`): an ASGI application.
- `startup_timeout` (`Optional[float]`, defaults to 5): maximum number of seconds to wait for the application to startup. Use `None` for no timeout.
- `shutdown_timeout` (`Optional[float]`, defaults to 5): maximum number of seconds to wait for the application to shutdown. Use `None` for no timeout.

**Yields**

- `manager` (`LifespanManager`): the `LifespanManager` itself. In case you use [lifespan state](https://asgi.readthedocs.io/en/latest/specs/lifespan.html#lifespan-state), use `async with LifespanManager(app) as manager: ...` then access `manager.app` to get a reference to the state-aware app.

**Raises**

- `LifespanNotSupported`: if the application does not seem to support the lifespan protocol. Based on the rationale that if the app supported the lifespan protocol then it would successfully receive the `lifespan.startup` ASGI event, unsupported lifespan protocol is detected in two situations:
  - The application called `send()` before calling `receive()` for the first time.
  - The application raised an exception during startup before making its first call to `receive()`. For example, this may be because the application failed on a statement such as `assert scope["type"] == "http"`.
- `TimeoutError`: if startup or shutdown timed out.
- `Exception`: any exception raised by the application (during startup, shutdown, or within the `async with` body) that does not indicate it does not support the lifespan protocol.

## License

MIT
