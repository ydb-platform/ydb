aiozipkin
=========
.. image:: https://github.com/aio-libs/aiozipkin/workflows/CI/badge.svg
    :target: https://github.com/aio-libs/aiozipkin/actions?query=workflow%3ACI
.. image:: https://codecov.io/gh/aio-libs/aiozipkin/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/aio-libs/aiozipkin
.. image:: https://api.codeclimate.com/v1/badges/1ff813d5cad2d702cbf1/maintainability
   :target: https://codeclimate.com/github/aio-libs/aiozipkin/maintainability
   :alt: Maintainability
.. image:: https://img.shields.io/pypi/v/aiozipkin.svg
    :target: https://pypi.python.org/pypi/aiozipkin
.. image:: https://readthedocs.org/projects/aiozipkin/badge/?version=latest
    :target: http://aiozipkin.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: Chat on Gitter

**aiozipkin** is Python 3.6+ module that adds distributed tracing capabilities
from asyncio_ applications with zipkin (http://zipkin.io) server instrumentation.

zipkin_ is a distributed tracing system. It helps gather timing data needed
to troubleshoot latency problems in microservice architectures. It manages
both the collection and lookup of this data. Zipkin’s design is based on
the Google Dapper paper.

Applications are instrumented with  **aiozipkin** report timing data to zipkin_.
The Zipkin UI also presents a Dependency diagram showing how many traced
requests went through each application. If you are troubleshooting latency
problems or errors, you can filter or sort all traces based on the
application, length of trace, annotation, or timestamp.

.. image:: https://raw.githubusercontent.com/aio-libs/aiozipkin/master/docs/zipkin_animation2.gif
    :alt: zipkin ui animation


Features
========
* Distributed tracing capabilities to **asyncio** applications.
* Support zipkin_ ``v2`` protocol.
* Easy to use API.
* Explicit context handling, no thread local variables.
* Can work with jaeger_ and stackdriver_ through zipkin compatible API.


zipkin vocabulary
-----------------
Before code lets learn important zipkin_ vocabulary, for more detailed
information please visit https://zipkin.io/pages/instrumenting

.. image:: https://raw.githubusercontent.com/aio-libs/aiozipkin/master/docs/zipkin_glossary.png
    :alt: zipkin ui glossary

* **Span** represents one specific method (RPC) call
* **Annotation** string data associated with a particular timestamp in span
* **Tag** - key and value associated with given span
* **Trace** - collection of spans, related to serving particular request


Simple example
--------------

.. code:: python

    import asyncio
    import aiozipkin as az


    async def run():
        # setup zipkin client
        zipkin_address = 'http://127.0.0.1:9411/api/v2/spans'
        endpoint = az.create_endpoint(
            "simple_service", ipv4="127.0.0.1", port=8080)
        tracer = await az.create(zipkin_address, endpoint, sample_rate=1.0)

        # create and setup new trace
        with tracer.new_trace(sampled=True) as span:
            # give a name for the span
            span.name("Slow SQL")
            # tag with relevant information
            span.tag("span_type", "root")
            # indicate that this is client span
            span.kind(az.CLIENT)
            # make timestamp and name it with START SQL query
            span.annotate("START SQL SELECT * FROM")
            # imitate long SQL query
            await asyncio.sleep(0.1)
            # make other timestamp and name it "END SQL"
            span.annotate("END SQL")

        await tracer.close()

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())


aiohttp example
---------------

*aiozipkin* includes *aiohttp* server instrumentation, for this create
`web.Application()` as usual and install aiozipkin plugin:


.. code:: python

    import aiozipkin as az

    def init_app():
        host, port = "127.0.0.1", 8080
        app = web.Application()
        endpoint = az.create_endpoint("AIOHTTP_SERVER", ipv4=host, port=port)
        tracer = await az.create(zipkin_address, endpoint, sample_rate=1.0)
        az.setup(app, tracer)


That is it, plugin adds middleware that tries to fetch context from headers,
and create/join new trace. Optionally on client side you can add propagation
headers in order to force tracing and to see network latency between client and
server.

.. code:: python

    import aiozipkin as az

    endpoint = az.create_endpoint("AIOHTTP_CLIENT")
    tracer = await az.create(zipkin_address, endpoint)

    with tracer.new_trace() as span:
        span.kind(az.CLIENT)
        headers = span.context.make_headers()
        host = "http://127.0.0.1:8080/api/v1/posts/{}".format(i)
        resp = await session.get(host, headers=headers)
        await resp.text()


Documentation
-------------
http://aiozipkin.readthedocs.io/


Installation
------------
Installation process is simple, just::

    $ pip install aiozipkin


Support of other collectors
===========================
**aiozipkin** can work with any other zipkin_ compatible service, currently we
tested it with jaeger_ and stackdriver_.


Jaeger support
--------------
jaeger_ supports zipkin_ span format as result it is possible to use *aiozipkin*
with jaeger_ server. You just need to specify *jaeger* server address and it
should work out of the box. Not need to run local zipkin server.
For more informations see tests and jaeger_ documentation.

.. image:: https://raw.githubusercontent.com/aio-libs/aiozipkin/master/docs/jaeger.png
    :alt: jaeger ui animation


Stackdriver support
-------------------
Google stackdriver_ supports zipkin_ span format as result it is possible to
use *aiozipkin* with this google_ service. In order to make this work you
need to setup zipkin service locally, that will send trace to the cloud. See
google_ cloud documentation how to setup make zipkin collector:

.. image:: https://raw.githubusercontent.com/aio-libs/aiozipkin/master/docs/stackdriver.png
    :alt: jaeger ui animation


Requirements
------------

* Python_ 3.6+
* aiohttp_


.. _PEP492: https://www.python.org/dev/peps/pep-0492/
.. _Python: https://www.python.org
.. _aiohttp: https://github.com/KeepSafe/aiohttp
.. _asyncio: http://docs.python.org/3.5/library/asyncio.html
.. _uvloop: https://github.com/MagicStack/uvloop
.. _zipkin: http://zipkin.io
.. _jaeger: http://jaeger.readthedocs.io/en/latest/
.. _stackdriver: https://cloud.google.com/stackdriver/
.. _google: https://cloud.google.com/trace/docs/zipkin
