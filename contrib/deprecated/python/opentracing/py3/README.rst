OpenTracing API for Python
==========================

|GitterChat| |BuildStatus| |PyPI| |ReadTheDocs|

This library is a Python platform API for OpenTracing.

Required Reading
----------------

In order to understand the Python platform API, one must first be familiar with
the `OpenTracing project <http://opentracing.io>`_ and
`terminology <http://opentracing.io/documentation/pages/spec.html>`_ more
specifically.

Status
------

In the current version, ``opentracing-python`` provides only the API and a
basic no-op implementation that can be used by instrumentation libraries to
collect and propagate distributed tracing context.

Future versions will include a reference implementation utilizing an
abstract Recorder interface, as well as a
`Zipkin <http://openzipkin.github.io>`_-compatible Tracer.

Usage
-----

The work of instrumentation libraries generally consists of three steps:

1. When a service receives a new request (over HTTP or some other protocol),
   it uses OpenTracing's inject/extract API to continue an active trace, creating a
   Span object in the process. If the request does not contain an active trace,
   the service starts a new trace and a new *root* Span.
2. The service needs to store the current Span in some request-local storage,
   (called ``Span`` *activation*) where it can be retrieved from when a child Span must
   be created, e.g. in case of the service making an RPC to another service.
3. When making outbound calls to another service, the current Span must be
   retrieved from request-local storage, a child span must be created (e.g., by
   using the ``start_child_span()`` helper), and that child span must be embedded
   into the outbound request (e.g., using HTTP headers) via OpenTracing's
   inject/extract API.

Below are the code examples for the previously mentioned steps. Implementation
of request-local storage needed for step 2 is specific to the service and/or frameworks /
instrumentation libraries it is using, exposed as a ``ScopeManager`` child contained
as ``Tracer.scope_manager``. See details below.

Inbound request
^^^^^^^^^^^^^^^

Somewhere in your server's request handler code:

.. code-block:: python

   def handle_request(request):
       span = before_request(request, opentracing.global_tracer())
       # store span in some request-local storage using Tracer.scope_manager,
       # using the returned `Scope` as Context Manager to ensure
       # `Span` will be cleared and (in this case) `Span.finish()` be called.
       with tracer.scope_manager.activate(span, True) as scope:
           # actual business logic
           handle_request_for_real(request)


   def before_request(request, tracer):
       span_context = tracer.extract(
           format=Format.HTTP_HEADERS,
           carrier=request.headers,
       )
       span = tracer.start_span(
           operation_name=request.operation,
           child_of=span_context)
       span.set_tag('http.url', request.full_url)

       remote_ip = request.remote_ip
       if remote_ip:
           span.set_tag(tags.PEER_HOST_IPV4, remote_ip)

       caller_name = request.caller_name
       if caller_name:
           span.set_tag(tags.PEER_SERVICE, caller_name)

       remote_port = request.remote_port
       if remote_port:
           span.set_tag(tags.PEER_PORT, remote_port)

       return span

Outbound request
----------------

Somewhere in your service that's about to make an outgoing call:

.. code-block:: python

   from opentracing import tags
   from opentracing.propagation import Format
   from opentracing_instrumentation import request_context

   # create and serialize a child span and use it as context manager
   with before_http_request(
       request=out_request,
       current_span_extractor=request_context.get_current_span):

       # actual call
       return urllib2.urlopen(request)


   def before_http_request(request, current_span_extractor):
       op = request.operation
       parent_span = current_span_extractor()
       outbound_span = opentracing.global_tracer().start_span(
           operation_name=op,
           child_of=parent_span
       )

       outbound_span.set_tag('http.url', request.full_url)
       service_name = request.service_name
       host, port = request.host_port
       if service_name:
           outbound_span.set_tag(tags.PEER_SERVICE, service_name)
       if host:
           outbound_span.set_tag(tags.PEER_HOST_IPV4, host)
       if port:
           outbound_span.set_tag(tags.PEER_PORT, port)

       http_header_carrier = {}
       opentracing.global_tracer().inject(
           span_context=outbound_span,
           format=Format.HTTP_HEADERS,
           carrier=http_header_carrier)

       for key, value in http_header_carrier.iteritems():
           request.add_header(key, value)

       return outbound_span

Scope and within-process propagation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For getting/setting the current active ``Span`` in the used request-local storage,
OpenTracing requires that every ``Tracer`` contains a ``ScopeManager`` that grants
access to the active ``Span`` through a ``Scope``. Any ``Span`` may be transferred to
another task or thread, but not ``Scope``.

.. code-block:: python

       # Access to the active span is straightforward.
       scope = tracer.scope_manager.active()
       if scope is not None:
           scope.span.set_tag('...', '...')

The common case starts a ``Scope`` that's automatically registered for intra-process
propagation via ``ScopeManager``.

Note that ``start_active_span('...')`` automatically finishes the span on ``Scope.close()``
(``start_active_span('...', finish_on_close=False)`` does not finish it, in contrast).

.. code-block:: python

       # Manual activation of the Span.
       span = tracer.start_span(operation_name='someWork')
       with tracer.scope_manager.activate(span, True) as scope:
           # Do things.

       # Automatic activation of the Span.
       # finish_on_close is a required parameter.
       with tracer.start_active_span('someWork', finish_on_close=True) as scope:
           # Do things.

       # Handling done through a try construct:
       span = tracer.start_span(operation_name='someWork')
       scope = tracer.scope_manager.activate(span, True)
       try:
           # Do things.
       except Exception as e:
           span.set_tag('error', '...')
       finally:
           scope.close()

**If there is a Scope, it will act as the parent to any newly started Span** unless
the programmer passes ``ignore_active_span=True`` at ``start_span()``/``start_active_span()``
time or specified parent context explicitly:

.. code-block:: python

       scope = tracer.start_active_span('someWork', ignore_active_span=True)

Each service/framework ought to provide a specific ``ScopeManager`` implementation
that relies on their own request-local storage (thread-local storage, or coroutine-based storage
for asynchronous frameworks, for example).

Scope managers
^^^^^^^^^^^^^^

This project includes a set of ``ScopeManager`` implementations under the ``opentracing.scope_managers`` submodule, which can be imported on demand:

.. code-block:: python

   from opentracing.scope_managers import ThreadLocalScopeManager

There exist implementations for ``thread-local`` (the default instance of the submodule ``opentracing.scope_managers``), ``gevent``, ``Tornado``, ``asyncio`` and ``contextvars``:

.. code-block:: python

   from opentracing.scope_managers.gevent import GeventScopeManager # requires gevent
   from opentracing.scope_managers.tornado import TornadoScopeManager # requires tornado<6
   from opentracing.scope_managers.asyncio import AsyncioScopeManager # fits for old asyncio applications, requires Python 3.4 or newer.
   from opentracing.scope_managers.contextvars import ContextVarsScopeManager # for asyncio applications, requires Python 3.7 or newer.


**Note** that for asyncio applications it's preferable to use ``ContextVarsScopeManager`` instead of ``AsyncioScopeManager`` because of automatic parent span propagation to children coroutines, tasks or scheduled callbacks.


Development
-----------

Tests
^^^^^

.. code-block:: sh

   virtualenv env
   . ./env/bin/activate
   make bootstrap
   make test

You can use `tox <https://tox.readthedocs.io>`_ to run tests as well.

.. code-block:: sh

    tox

Testbed suite
^^^^^^^^^^^^^

A testbed suite designed to test API changes and experimental features is included under the *testbed* directory. For more information, see the `Testbed README <testbed/README.md>`_.

Instrumentation Tests
---------------------

This project has a working design of interfaces for the OpenTracing API. There is a MockTracer to
facilitate unit-testing of OpenTracing Python instrumentation.

.. code-block:: python

       from opentracing.mocktracer import MockTracer

       tracer = MockTracer()
       with tracer.start_span('someWork') as span:
           pass

       spans = tracer.finished_spans()
       someWorkSpan = spans[0]


Documentation
^^^^^^^^^^^^^

.. code-block:: sh

   virtualenv env
   . ./env/bin/activate
   make bootstrap
   make docs

The documentation is written to *docs/_build/html*.

LICENSE
^^^^^^^

`Apache 2.0 License <./LICENSE>`__.

Releases
^^^^^^^^

Before new release, add a summary of changes since last version to CHANGELOG.rst

.. code-block:: sh

   pip install zest.releaser[recommended]
   prerelease
   release
   git push origin master --follow-tags
   python setup.py sdist upload -r pypi upload_docs -r pypi
   postrelease
   git push

.. |GitterChat| image:: http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg
   :target: https://gitter.im/opentracing/public
.. |BuildStatus| image:: https://travis-ci.org/opentracing/opentracing-python.svg?branch-master
   :target: https://travis-ci.org/opentracing/opentracing-python
.. |PyPI| image:: https://badge.fury.io/py/opentracing.svg
   :target: https://badge.fury.io/py/opentracing
.. |ReadTheDocs| image:: http://readthedocs.org/projects/opentracing-python/badge/?version=latest
   :target: https://opentracing-python.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
