
# Flask-Log-Request-Id

[![CircleCI](https://img.shields.io/circleci/project/github/Workable/flask-log-request-id.svg)](https://circleci.com/gh/Workable/flask-log-request-id)

**Flask-Log-Request-Id** is an extension for [Flask](http://flask.pocoo.org/) that can parse and handle the
request-id sent by request processors like [Amazon ELB](http://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-request-tracing.html),
[Heroku Request-ID](https://devcenter.heroku.com/articles/http-request-id) or any multi-tier infrastructure as the one used
at microservices. A common usage scenario is to inject the request_id in the logging system so that all log records,
even those emitted by third party libraries, have attached the request_id that initiated their call. This can
greatly improve tracing and debugging of problems.

## Installation

The easiest way to install it is using ``pip`` from PyPI

```bash
pip install flask-log-request-id
```

## Usage

Flask-Log-Request-Id provides the `current_request_id()` function which can be used at any time to get the request
id of the initiated execution chain.


### Example 1: Parse request id and print to stdout
```python
from flask_log_request_id import RequestID, current_request_id

[...]

RequestID(app)


@app.route('/')
def hello():
    print('Current request id: {}'.format(current_request_id()))
```


### Example 2: Parse request id and send it to to logging

In the following example, we will use the `RequestIDLogFilter` to inject the request id on all log events, and
a custom formatter to print this information. If all these sounds unfamiliar please take a look at [python's logging
system](https://docs.python.org/3/library/logging.html)


```python
import logging
import logging.config
from random import randint
from flask import Flask
from flask_log_request_id import RequestID, RequestIDLogFilter

def generic_add(a, b):
    """Simple function to add two numbers that is not aware of the request id"""
    logging.debug('Called generic_add({}, {})'.format(a, b))
    return a + b

app = Flask(__name__)
RequestID(app)

# Setup logging
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - level=%(levelname)s - request_id=%(request_id)s - %(message)s"))
handler.addFilter(RequestIDLogFilter())  # << Add request id contextual filter
logging.getLogger().addHandler(handler)


@app.route('/')
def index():
    a, b = randint(1, 15), randint(1, 15)
    logging.info('Adding two random numbers {} {}'.format(a, b))
    return str(generic_add(a, b))
```

The above will output the following log entries:

```
2017-07-25 16:15:25,912 - __main__ - level=INFO - request_id=7ff2946c-efe0-4c51-b337-fcdcdfe8397b - Adding two random numbers 11 14
2017-07-25 16:15:25,913 - __main__ - level=DEBUG - request_id=7ff2946c-efe0-4c51-b337-fcdcdfe8397b - Called generic_add(11, 14)
2017-07-25 16:15:25,913 - werkzeug - level=INFO - request_id=None - 127.0.0.1 - - [25/Jul/2017 16:15:25] "GET / HTTP/1.1" 200 -
```

### Example 3: Forward request_id to celery tasks

Flask-Log-Request-Id comes with extras to forward the context of current request_id to the workers of celery tasks.
In order to use this feature you need to enable the celery plugin and configure the `Celery` instance. Then you can
use `current_request_id()` from inside your worker

```python
from flask_log_request_id.extras.celery import enable_request_id_propagation
from flask_log_request_id import current_request_id
from celery.app import Celery
import logging

celery = Celery()
enable_request_id_propagation(celery)  # << This step here is critical to propagate request-id to workers

app = Flask()

@celery.task()
def generic_add(a, b):
    """Simple function to add two numbers that is not aware of the request id"""

    logging.debug('Called generic_add({}, {}) from request_id: {}'.format(a, b, current_request_id()))
    return a + b

@app.route('/')
def index():
    a, b = randint(1, 15), randint(1, 15)
    logging.info('Adding two random numbers {} {}'.format(a, b))
    return str(generic_add.delay(a, b))  # Calling the task here, will forward the request id to the workers
```

You can follow the same logging strategy for both web application and workers using the `RequestIDLogFilter` as shown in
example 1 and 2.

### Example 4: If you want to return request id in response

This will be useful while integrating with frontend where in you can get the request id from the response (be it 400 or 500) and then trace the request in logs.

```python
from flask_log_request_id import current_request_id

@app.after_request
def append_request_id(response):
    response.headers.add('X-REQUEST-ID', current_request_id())
    return response
```

## Configuration

The following parameters can be configured through Flask's configuration system:

| Configuration Name | Description |
| ------------------ | ----------- |
| **LOG_REQUEST_ID_GENERATE_IF_NOT_FOUND**| In case the request does not hold any request id, the extension will generate one. Otherwise `current_request_id` will return None. |
| **LOG_REQUEST_ID_LOG_ALL_REQUESTS** | If True, it will emit a log event at the request containing all the details as `werkzeug` would done along with the `request_id` . |
| **LOG_REQUEST_ID_G_OBJECT_ATTRIBUTE** | This is the attribute of `Flask.g` object to store the current request id. Should be changed only if there is a problem. Use `current_request_id()` to fetch the current id. |


## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (MIT).
