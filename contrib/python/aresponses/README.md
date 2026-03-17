

# aresponses

[![image](https://img.shields.io/pypi/v/aresponses.svg)](https://pypi.org/project/aresponses/)
[![image](https://img.shields.io/pypi/pyversions/aresponses.svg)](https://pypi.org/project/aresponses/)
[![build status](https://github.com/CircleUp/aresponses/workflows/Python%20Checks/badge.svg)](https://github.com/CircleUp/aresponses/actions?query=branch%3Amaster)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)


an asyncio testing server for mocking external services

## Features
 - Fast mocks using actual network connections
 - allows mocking some types of network issues
 - use regular expression matching for domain, path, method, or body
 - works with https requests as well (by switching them to http requests)
 - works with callables
 
## Usage

Add routes and responses via the `aresponses.add` method:

```python
def add(
    host_pattern=ANY, 
    path_pattern=ANY, 
    method_pattern=ANY, 
    response="", 
    *, 
    route=None, 
    body_pattern=ANY, m
    match_querystring=False, 
    repeat=1
    )
```

When a request is received the first matching response will be returned
and removed from the routing table.  The `response` argument can be
either a string, Response, dict, or list.  Use `aresponses.Response`
when you need to do something more complex.


**Note that version >=2.0 requires explicit assertions!**
```python
@pytest.mark.asyncio
async def test_simple(aresponses):
    aresponses.add("google.com", "/api/v1/", "GET", response="OK")
    aresponses.add('foo.com', '/', 'get', aresponses.Response(text='error', status=500))

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com/api/v1/") as response:
            text = await response.text()
            assert text == "OK"
        
        async with session.get("https://foo.com") as response:
            text = await response.text()
            assert text == "error"

    aresponses.assert_plan_strictly_followed()
```

#### Assertions
In aresponses 1.x requests that didn't match a route stopped the event
loop and thus forced an exception.  In aresponses >2.x it's required to
make assertions at the end of the test.

There are three assertions functions provided:
- `aresponses.assert_no_unused_routes` Raises `UnusedRouteError` if all
the routes defined were not used up.
- `aresponses.assert_called_in_order` - Raises `UnorderedRouteCallError`
if the routes weren't called in the order they were defined.
- `aresponses.assert_all_requests_matched` - Raises `NoRouteFoundError`
if any requests were made that didn't match to a route.  It's likely
but not guaranteed that your code will throw an exception in this
situation before the assertion is reached.

Instead of calling these individually, **it's recommended to call
`aresponses.assert_plan_strictly_followed()` at the end of each test as
it runs all three of the above assertions.**


#### Regex and Repeat
`host_pattern`, `path_pattern`, `method_pattern` and `body_pattern` may
be either strings (exact match) or regular expressions.

The repeat argument permits a route to be used multiple times.

If you want to just blanket mock a service, without concern for how many
times its called you could set repeat to a large number and not call
`aresponses.assert_plan_strictly_followed` or
`arespones.assert_no_unused_routes`.

```python
@pytest.mark.asyncio
async def test_regex_repetition(aresponses):
    aresponses.add(re.compile(r".*\.?google\.com"), response="OK", repeat=2)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com") as response:
            text = await response.text()
            assert text == "OK"

        async with session.get("http://api.google.com") as response:
            text = await response.text()
            assert text == "OK"

    aresponses.assert_plan_strictly_followed()
```

#### Json Responses
As a convenience, if a dict or list is passed to `response` then it will
create a json response. A `aiohttp.web_response.json_response` object
can be used for more complex situations.

```python
@pytest.mark.asyncio
async def test_json(aresponses):
    aresponses.add("google.com", "/api/v1/", "GET", response={"status": "OK"})

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com/api/v1/") as response:
            assert {"status": "OK"} == await response.json()

    aresponses.assert_plan_strictly_followed()
```

#### Custom Handler

Custom functions can be used for whatever other complex logic is
desired. In example below the handler is set to repeat infinitely
and always return 500.

```python
import math

@pytest.mark.asyncio
async def test_handler(aresponses):
    def break_everything(request):
        return aresponses.Response(status=500, text=str(request.url))

    aresponses.add(response=break_everything, repeat=math.inf)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://google.com/api/v1/") as response:
            assert response.status == 500
```


#### Passthrough
Pass `aresponses.passthrough` into the response argument to allow a
request to bypass mocking.

```python
    aresponses.add('httpstat.us', '/200', 'get', aresponses.passthrough)
```

#### Inspecting history
History of calls can be inspected via `aresponses.history` which returns
the namedTuple `RoutingLog(request, route, response)`

```python
@pytest.mark.asyncio
async def test_history(aresponses):
    aresponses.add(response=aresponses.Response(text="hi"), repeat=2)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://foo.com/b") as response:
            await response.text()
        async with session.get("http://bar.com/a") as response:
            await response.text()

    assert len(aresponses.history) == 2
    assert aresponses.history[0].request.host == "foo.com"
    assert aresponses.history[1].request.host == "bar.com"
    assert "Route(" in repr(aresponses.history[0].route)
    aresponses.assert_plan_strictly_followed()
```

#### Context manager usage
```python
import aiohttp
import pytest
import aresponses


@pytest.mark.asyncio
async def test_foo(event_loop):
    async with aresponses.ResponsesMockServer(loop=event_loop) as arsps:
        arsps.add('foo.com', '/', 'get', 'hi there!!')
        arsps.add(arsps.ANY, '/', 'get', arsps.Response(text='hey!'))
        
        async with aiohttp.ClientSession(loop=event_loop) as session:
            async with session.get('http://foo.com') as response:
                text = await response.text()
                assert text == 'hi'
            
            async with session.get('https://google.com') as response:
                text = await response.text()
                assert text == 'hey!'
        
```

#### working with [pytest-aiohttp](https://github.com/aio-libs/pytest-aiohttp)

If you need to use aresponses together with pytest-aiohttp, you should re-initialize main aresponses fixture with `loop` fixture
```python
from aresponses import ResponsesMockServer

@pytest.fixture
async def aresponses(loop):
    async with ResponsesMockServer(loop=loop) as server:
        yield server
```

If you're trying to use the `aiohttp_client` test fixture then you'll need to mock out the aiohttp `loop` fixture
instead:
```python
@pytest.fixture
def loop(event_loop):
    """replace aiohttp loop fixture with pytest-asyncio fixture"""
    return event_loop
```

## Contributing

### Dev environment setup
  - **install pyenv and pyenv-virtualenv**  - Makes it easy to install specific versions of python and switch between them. Make sure you install the virtualenv bash hook
  - `git clone` the repo and `cd` into it.
  - `make init` - installs proper version of python, creates the virtual environment, activates it and installs all the requirements
  
### Submitting a feature request  
  - **`git checkout -b my-feature-branch`** 
  - **make some cool changes**
  - **`make autoformat`**
  - **`make test`**
  - **`make lint`**
  - **create pull request**

### Updating package on pypi
  - `make deploy`


## Changelog

#### 3.0.0
- fix: start using `asyncio.get_running_loop()` instead of `event_loop` per the error:
    ```
    PytestDeprecationWarning: aresponses is asynchronous and explicitly requests the "event_loop" fixture. 
    Asynchronous fixtures and test functions should use "asyncio.get_running_loop()" instead.
    ```
- drop support for python 3.6
- add comprehensive matrix testing of all supported python and aiohttp versions
- tighten up the setup.py requirements

#### 2.1.6
- fix: incorrect pytest plugin entrypoint name (#72)

#### 2.1.5
- support asyncio_mode = strict (#68)
 
#### 2.1.4
- fix: don't assume utf8 request contents

#### 2.1.3
- accidental no-op release

#### 2.1.2
- documentation: add pypi documentation

#### 2.1.1

- bugfix: RecursionError when aresponses is used in more than 1000 tests (#63)

#### 2.1.0
- feature: add convenience method `add_local_passthrough`
- bugfix: fix https subrequest mocks. support aiohttp_client compatibility

#### 2.0.2
- bugfix: ensure request body is available in history

#### 2.0.0
**Warning! Breaking Changes!**
- breaking change: require explicit assertions for test failures
- feature: autocomplete works in intellij/pycharm
- feature: can match on body of request
- feature: store calls made
- feature: repeated responses
- bugfix: no longer stops event loop
- feature: if dict or list is passed into `response`, a json response
will be generated


#### 1.1.2
- make passthrough feature work with binary data

#### 1.1.1
- regex fix for Python 3.7.0

#### 1.1.0
- Added passthrough option to permit live network calls
- Added example of using a callable as a response

#### 1.0.0

- Added an optional `match_querystring` argument that lets you match on querystring as well


## Contributors
* Bryce Drennan, CircleUp <aresponses@brycedrennan.com>
* Marco Castelluccio, Mozilla <mcastelluccio@mozilla.com>
* Jesse Vogt, CircleUp <jesse.vogt@gmail.com>
* Pavol Vargovcik, Kiwi.com <pavol.vargovcik@gmail.com>
