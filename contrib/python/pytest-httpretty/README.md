# pytest-httpretty - A thin wrapper of HTTPretty for pytest

pytest-httpretty provides `httpretty` marker and `stub_get()` shorthand function.

```python
import httpretty
import pytest
import requests
from pytest_httpretty import stub_get


@pytest.mark.httpretty
def test_mark_httpretty():
    httpretty.register_uri(httpretty.GET, 'http://example.com/', body='Hello')

    assert requests.get('http://example.com').text == 'Hello'


@pytest.mark.httpretty
def test_stub_get():
    stub_get('http://example.com/', body='World!')

    assert requests.get('http://example.com').text == 'World!'
```
