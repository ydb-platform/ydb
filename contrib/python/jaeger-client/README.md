[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![PyPI Version][pypi-img]][pypi] [![Python Version][pythonversion-img]][pythonversion] [![FOSSA Status][fossa-img]][fossa]

# Jaeger Bindings for Python OpenTracing API

This is a client-side library that can be used to instrument Python apps
for distributed trace collection, and to send those traces to Jaeger.
See the [OpenTracing Python API](https://github.com/opentracing/opentracing-python)
for additional detail.

## Contributing and Developing

Please see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Installation

```bash
pip install jaeger-client
```

## Getting Started

```python
import logging
import time
from jaeger_client import Config

if __name__ == "__main__":
    log_level = logging.DEBUG
    logging.getLogger('').handlers = []
    logging.basicConfig(format='%(asctime)s %(message)s', level=log_level)

    config = Config(
        config={ # usually read from some yaml config
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'logging': True,
        },
        service_name='your-app-name',
        validate=True,
    )
    # this call also sets opentracing.tracer
    tracer = config.initialize_tracer()

    with tracer.start_span('TestSpan') as span:
        span.log_kv({'event': 'test message', 'life': 42})

        with tracer.start_span('ChildSpan', child_of=span) as child_span:
            child_span.log_kv({'event': 'down below'})

    time.sleep(2)   # yield to IOLoop to flush the spans - https://github.com/jaegertracing/jaeger-client-python/issues/50
    tracer.close()  # flush any buffered spans
```

**NOTE**: If you're using the Jaeger `all-in-one` Docker image (or similar) and want to run Jaeger in a separate container from your app, use the code below to define the host and port that the Jaeger agent is running on. *Note that this is not recommended, as Jaeger sends spans over UDP and UDP does not guarantee delivery.* (See [this thread](https://github.com/jaegertracing/jaeger-client-python/issues/47) for more details.)

```python
    config = Config(
        config={ # usually read from some yaml config
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'local_agent': {
                'reporting_host': 'your-reporting-host',
                'reporting_port': 'your-reporting-port',
            },
            'logging': True,
        },
        service_name='your-app-name',
        validate=True,
    )
```

### Other Instrumentation

The [OpenTracing Registry](https://opentracing.io/registry/) has many modules that provide explicit instrumentation support for popular frameworks like Django and Flask.

At Uber we are mostly using the [opentracing_instrumentation](https://github.com/uber-common/opentracing-python-instrumentation) module that provides:
  * explicit instrumentation for HTTP servers, and
  * implicit (monkey-patched) instrumentation for several popular libraries like `urllib2`, `redis`, `requests`, some SQL clients, etc.

## Initialization & Configuration

Note: do not initialize the tracer during import, it may cause a deadlock (see issues #31, #60).
Instead define a function that returns a tracer (see example below) and call that function explicitly
after all the imports are done.


Also note that using `gevent.monkey` in asyncio-based applications (python 3+) may need to pass current event loop explicitly (see issue #256):

 ```python
from tornado import ioloop
from jaeger_client import Config

config = Config(config={}, service_name='your-app-name', validate=True)
config.initialize_tracer(io_loop=ioloop.IOLoop.current())
```

### Production

The recommended way to initialize the tracer for production use:

```python
from jaeger_client import Config

def init_jaeger_tracer(service_name='your-app-name'):
    config = Config(config={}, service_name=service_name, validate=True)
    return config.initialize_tracer()
```

Note that the call `initialize_tracer()` also sets the `opentracing.tracer` global variable.
If you need to create additional tracers (e.g., to create spans on the client side for remote services that are not instrumented), use the `new_tracer()` method.

#### Prometheus metrics

This module brings a [Prometheus](https://github.com/prometheus/client_python) integration to the internal Jaeger metrics.
The way to initialize the tracer with Prometheus metrics:

```python
from jaeger_client.metrics.prometheus import PrometheusMetricsFactory

config = Config(
        config={},
        service_name='your-app-name',
        validate=True,
        metrics_factory=PrometheusMetricsFactory(service_name_label='your-app-name')
)
tracer = config.initialize_tracer()
```

Note that the optional argument `service_name_label` to the factory constructor
will force it to tag all Jaeger client metrics with a label `service: your-app-name`.
This way you can distinguish Jaeger client metrics produced by different services.

### Development

For development, some parameters can be passed via `config` dictionary, as in the Getting Started example above. For more details please see the [Config class](jaeger_client/config.py).

### WSGI, multi-processing, fork(2)

When using this library in applications that fork child processes to handle individual requests,
such as with [WSGI / PEP 3333](https://wsgi.readthedocs.io/), care must be taken when initializing the tracer.
When Jaeger tracer is initialized, it may start a new background thread. If the process later forks,
it might cause issues or hang the application (due to exclusive lock on the interpreter).
Therefore, it is recommended that the tracer is not initialized until after the child processes
are forked. Depending on the WSGI framework you might be able to use `@postfork` decorator
to delay tracer initialization (see also issues #31, #60).

## Debug Traces (Forced Sampling)

### Programmatically

The OpenTracing API defines a `sampling.priority` standard tag that
can be used to affect the sampling of a span and its children:

```python
from opentracing.ext import tags as ext_tags

span.set_tag(ext_tags.SAMPLING_PRIORITY, 1)
```

### Via HTTP Headers

Jaeger Tracer also understands a special HTTP Header `jaeger-debug-id`,
which can be set in the incoming request, e.g.

```sh
curl -H "jaeger-debug-id: some-correlation-id" http://myhost.com
```

When Jaeger sees this header in the request that otherwise has no
tracing context, it ensures that the new trace started for this
request will be sampled in the "debug" mode (meaning it should survive
all downsampling that might happen in the collection pipeline), and
the root span will have a tag as if this statement was executed:

```python
span.set_tag('jaeger-debug-id', 'some-correlation-id')
```

This allows using Jaeger UI to find the trace by this tag.

## Zipkin Compatibility

To use this library directly with other Zipkin libraries & backend,
you can provide the configuration property `propagation: 'b3'` and the
`X-B3-*` HTTP headers will be supported.

The B3 codec assumes it will receive lowercase HTTP headers, as this seems
to be the standard in the popular frameworks like Flask and Django.
Please make sure your framework does the same.

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://github.com/jaegertracing/jaeger-client-python/workflows/Unit%20Tests/badge.svg?branch=master
[ci]: https://github.com/jaegertracing/jaeger-client-python/actions?query=branch%3Amaster
[cov-img]: https://codecov.io/gh/jaegertracing/jaeger-client-python/branch/master/graph/badge.svg
[cov]: https://codecov.io/gh/jaegertracing/jaeger-client-python
[pypi-img]: https://badge.fury.io/py/jaeger-client.svg
[pypi]: https://badge.fury.io/py/jaeger-client
[pythonversion-img]: https://img.shields.io/pypi/pyversions/jaeger-client.svg
[pythonversion]: https://pypi.org/project/jaeger-client
[fossa-img]: https://app.fossa.io/api/projects/git%2Bgithub.com%2Fjaegertracing%2Fjaeger-client-python.svg?type=shield
[fossa]: https://app.fossa.io/projects/git%2Bgithub.com%2Fjaegertracing%2Fjaeger-client-python?ref=badge_shield
