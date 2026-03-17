# Dataclass REST

[![PyPI version](https://badge.fury.io/py/dataclass-rest.svg)](https://badge.fury.io/py/dataclass-rest)
[![Build Status](https://travis-ci.org/Tishka17/dataclass_rest.svg?branch=master)](https://travis-ci.org/Tishka17/dataclass_rest)

A modern and simple way to create clients for REST like APIs

## Quickstart


Step 1. Install
```bash
pip install dataclass_rest requests
```


Step 2. Declare models

```python
@dataclass
class Todo:
    id: int
    user_id: int
    title: str
    completed: bool
```

Step 3. Create and configure client

```python

from requests import Session
from dataclass_rest.http.requests import RequestsClient

class RealClient(RequestsClient):
    def __init__(self):
        super().__init__("https://example.com/api", Session())
```

Step 4. Declare methods using `get`/`post`/`delete`/`patch`/`put` decorators. 
Type hints are required. Body of method is ignored.

Use any method arguments to format URL.
`body` argument is sent as request body with json. Other arguments, not used in URL are passed as query parameters.
`get` and `delete` does not have body.

```python
from typing import Optional, List
from requests import Session
from dataclass_rest import get, post, delete
from dataclass_rest.http.requests import RequestsClient

class RealClient(RequestsClient):
    def __init__(self):
        super().__init__("https://example.com/api", Session())

    @get("todos/{id}")
    def get_todo(self, id: str) -> Todo:
        pass

    @get("todos")
    def list_todos(self, user_id: Optional[int]) -> List[Todo]:
        pass

    @delete("todos/{id}")
    def delete_todo(self, id: int):
        pass

    @post("todos")
    def create_todo(self, body: Todo) -> Todo:
        pass
```

You can use Callable ```(...) -> str``` as the url source, 
all parameters passed to the client method can be obtained inside the Callable

```python
from requests import Session
from dataclass_rest import get
from dataclass_rest.http.requests import RequestsClient

def url_generator(todo_id: int) -> str:
    return f"/todos/{todo_id}/"


class RealClient(RequestsClient):
    def __init__(self):
        super().__init__("https://dummyjson.com/", Session())

    @get(url_generator)
    def todo(self, todo_id: int) -> Todo:
        pass


client = RealClient()
client.todo(5)
```

## Asyncio

To use async client insted of sync:

1. Install `aiohttp` (instead of `requests`)
2. Change `dataclass_rest.http.requests.RequestsClient` to `dataclass_rest.http.aiohttp.AiohttpClient`
3. Add `async` keyword to your methods 

## Configuration

### Parsing and serialization

All parsing and serialization is done using instances of `FactoryProtocol`.
They are create by client object during its initialization. Default implementation creates `Retort` from [adaptix](https://adaptix.readthedocs.io/)

There are 3 of them:

1. Request body factory. Created using `_init_request_body_factory`. It is used to convert body object to simple python classes before senging to the server
2. Request args factory. Created using `_init_request_args_factory`. It is used to convert other parameters of method. All parameters are passed as a single object.
Type of that object is generated and can be retrieved from your client methods using `.methodspec.query_params_type`
3. Response body factory. Created using `_init_response_body_factory`. It is used to parse a server response.

### Error handling

You can attach error handler to single method using `@yourmethod.on_error` decorator in your class.

To set same behavior for all methods inherit from BoundMethod class, override `_on_error_default` method and set that class as `Client.method_class`

### Other params

You can use different body argument name if you want. Just pass `body_name` to the decorator.


### Special cases

#### `None` in query params

By default, AioHTTP doesn't skip query params, you can customize that overriding `_pre_process_request` in Method class

```python
class NoneAwareAiohttpMethod(AiohttpMethod):
    async def _pre_process_request(self, request: HttpRequest) -> HttpRequest:
        request.query_params = {
            k: v for k, v in request.query_params.items() if v is not None
        }
        return request


class Client(AiohttpClient):
    method_class = NoneAwareAiohttpMethod
```

#### Handling `No content`

By default, en each method json response is expected. Sometime you expect no content from server. Especially for 204.
You can handle it by overriding `_response_body` method, e.g.:

```python
class NoneAwareRequestsMethod(RequestsMethod):
    def _response_body(self, response: Response) -> Any:
        if response.status_code == http.HTTPStatus.NO_CONTENT:
            return None
        return super()._response_body(response)


class Client(RequestsClient):
    method_class = NoneAwareRequestsMethod
```
