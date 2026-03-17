# fastapi-etag

## Quickstart

Basic etag support for FastAPI, allowing you to benefit from conditional caching in web browsers and reverse-proxy caching layers.

This does not generate etags that are a hash of the response content, but instead lets you pass in a custom etag generating function per endpoint that is called before executing the route function.  
This lets you bypass expensive API calls when client includes a matching etag in the `If-None-Match` header, in this case your endpoint is never called, instead returning a 304 response telling the client nothing has changed.

The etag logis is implemented with a fastapi dependency that you can add to your routes or entire routers.

Here's how you use it:

```python3
# app.py

from fastapi import FastAPI
from starlette.requests import Request
from fastapi_etag import Etag, add_exception_handler

app = FastAPI()
add_exception_handler(app)


async def get_hello_etag(request: Request):
    return "etagfor" + request.path_params["name"]


@app.get("/hello/{name}", dependencies=[Depends(Etag(get_hello_etag))])
async def hello(name: str):
    return {"hello": name}

```

Run this example with `uvicorn: uvicorn --port 8090 app:app`

Let's break it down:

```python3
add_exception_handler(app)
```

The dependency raises a special `CacheHit` exception to exit early when there's a an etag match, this adds a standard exception handler to the app to generate a correct 304 response from the exception.

```python3
async def get_hello_etag(request: Request):
    name = request.path_params.get("name")
    return f"etagfor{name}"
```

This is the function that generates the etag for your endpoint.  
It can do anything you want, it could for example return a hash of a last modified timestamp in your database.  
It can be either a normal function or an async function.  
Only requirement is that it accepts one argument (request) and that it returns either a string (the etag) or `None` (in which case no etag header is added)


```python3
@app.get("/hello/{name}", dependencies=[Depends(Etag(get_hello_etag))])
def hello(name: str):
	...
```

The `Etag` dependency is called like any fastapi dependency.
It always adds the etag returned by your etag gen function to the response.  
If client passes a matching etag in the `If-None-Match` header, it will raise a `CacheHit` exception which triggers a 304 response before calling your endpoint.


Now try it with curl:

```
curl -i "http://localhost:8090/hello/bob"
HTTP/1.1 200 OK
date: Mon, 30 Dec 2019 21:55:43 GMT
server: uvicorn
content-length: 15
content-type: application/json
etag: W/"etagforbob"

{"hello":"bob"}
```

Etag header is added

Now including the etag in `If-None-Match` header (mimicking a web browser):

```
curl -i -X GET "http://localhost:8090/hello/bob" -H "If-None-Match: W/\"etagforbob\""
HTTP/1.1 304 Not Modified
date: Mon, 30 Dec 2019 21:57:37 GMT
server: uvicorn
etag: W/"etagforbob"
```

It now returns no content, only the 304 telling us nothing has changed.

### Add response headers

If you want to add some extra response headers to the 304 and regular response,
you can add the `extra_headers` argument with a dict of headers:

```
@app.get(
    "/hello/{name}",
    dependencies=[
        Depends(
            Etag(
                get_hello_etag,
                extra_headers={"Cache-Control": "public, max-age: 30"},
            )
        )
    ],
)
def hello(name: str):
	...
```

This will add the `cache-control` header on all responses from the endpoint.


## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

