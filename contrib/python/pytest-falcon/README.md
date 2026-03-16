[![Build Status](https://travis-ci.org/yohanboniface/pytest-falcon.svg?branch=master)](https://travis-ci.org/yohanboniface/pytest-falcon) [![Pypi version](https://img.shields.io/pypi/v/pytest-falcon.svg)](https://pypi.python.org/pypi/pytest-falcon)

# Pytest-Falcon

Pytest helpers for the Falcon framework.


## Install

```
pip install pytest-falcon
```


## Usage

You must create an `app` fixture to expose the Falcon application you want to test:

```python
import falcon
import pytest


application = falcon.API()
application.req_options.auto_parse_form_urlencoded = True


@pytest.fixture
def app():
    return application
```

## Fixtures

### client

Allows you to test your API:

```python
class Resource:

    def on_post(self, req, resp, **kwargs):
        resp.body = json.dumps(req.params)

application.add_route('/route', Resource())

def test_post(client):
    resp = client.post('/route', {'myparam': 'myvalue'})
    assert resp.status == falcon.HTTP_OK
    assert resp.json['myparam'] == 'myvalue'
```

Response properties:
- `body` the body as `str`
- `json` the body parsed as json when the response content-type is 'application/json'
- `headers` the response headers
- `status` the response status, as `str` ('200 OK', '405 Method Not Allowed'…)
- `status_code` the response status code, as `int` (200, 201…)
