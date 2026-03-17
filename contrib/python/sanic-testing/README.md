# Sanic Core Test

This package is meant to be the core testing utility and clients for testing Sanic applications. It is mainly derived from `sanic.testing` which has (or will be) removed from the main Sanic repository in the future.

[Documentation](https://sanicframework.org/en/plugins/sanic-testing/getting-started.html)

## Getting Started

    pip install sanic-testing

The package is meant to create an almost seemless transition. Therefore, after loading the package, it will attach itself to your Sanic instance and insert test clients.

```python
from sanic import Sanic
from sanic_testing import TestManager

sanic_app = Sanic(__name__)
TestManager(sanic_app)
```

This will provide access to both the sync (`sanic.test_client`) and async (`sanic.asgi_client`) clients. Both of these clients are also available directly on the `TestManager` instance.

## Writing a sync test

Testing should be pretty much the same as when the test client was inside Sanic core. The difference is just that you need to run `TestManager`.

```python
import pytest

@pytest.fixture
def app():
    sanic_app = Sanic(__name__)
    TestManager(sanic_app)

    @sanic_app.get("/")
    def basic(request):
        return response.text("foo")

    return sanic_app

def test_basic_test_client(app):
    request, response = app.test_client.get("/")

    assert response.body == b"foo"
    assert response.status == 200
```

## Writing an async test

Testing of an async method is best done with `pytest-asyncio` installed. Again, the following test should look familiar to anyone that has used `asgi_client` in the Sanic core package before.

The main benefit of using the `asgi_client` is that it is able to reach inside your application, and execute your handlers without ever having to stand up a server or make a network call.

```python
import pytest

@pytest.fixture
def app():
    sanic_app = Sanic(__name__)
    TestManager(sanic_app)

    @sanic_app.get("/")
    def basic(request):
        return response.text("foo")

    return sanic_app

@pytest.mark.asyncio
async def test_basic_asgi_client(app):
    request, response = await app.asgi_client.get("/")

    assert response.body == b"foo"
    assert response.status == 200
```
