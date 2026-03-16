##################
Django Opentracing
##################

.. image:: https://travis-ci.org/opentracing-contrib/python-django.svg?branch=master
    :target: https://travis-ci.org/opentracing-contrib/python-django

.. image:: https://img.shields.io/pypi/v/django_opentracing.svg
    :target: https://pypi.org/project/django_opentracing/

.. image:: https://img.shields.io/pypi/pyversions/django_opentracing.svg
    :target: https://pypi.org/project/django_opentracing/

.. image:: https://img.shields.io/pypi/dm/django_opentracing.svg
    :target: https://pypi.org/project/django_opentracing/


This package enables distributed tracing in Django projects via `The OpenTracing Project`_. Once a production system contends with real concurrency or splits into many services, crucial (and formerly easy) tasks become difficult: user-facing latency optimization, root-cause analysis of backend errors, communication about distinct pieces of a now-distributed system, etc. Distributed tracing follows a request on its journey from inception to completion from mobile/browser all the way to the microservices. 

As core services and libraries adopt OpenTracing, the application builder is no longer burdened with the task of adding basic tracing instrumentation to their own code. In this way, developers can build their applications with the tools they prefer and benefit from built-in tracing instrumentation. OpenTracing implementations exist for major distributed tracing systems and can be bound or swapped with a one-line configuration change.

If you want to learn more about the underlying python API, visit the python `source code`_.

If you are migrating from the 0.x series, you may want to read the list of `breaking changes`_.

.. _The OpenTracing Project: http://opentracing.io/
.. _source code: https://github.com/opentracing/opentracing-python
.. _breaking changes: #breaking-changes-from-0-x

Installation
============

Run the following command::

    $ pip install django_opentracing

Setting up Tracing
==================

In order to implement tracing in your system, add the following lines of code to your site's settings.py file:

.. code-block:: python

    import django_opentracing

    # OpenTracing settings

    # if not included, defaults to True.
    # has to come before OPENTRACING_TRACING setting because python...
    OPENTRACING_TRACE_ALL = True

    # defaults to []
    # only valid if OPENTRACING_TRACE_ALL == True
    OPENTRACING_TRACED_ATTRIBUTES = ['arg1', 'arg2']

    # Callable that returns an `opentracing.Tracer` implementation.
    OPENTRACING_TRACER_CALLABLE = 'opentracing.Tracer'

    # Parameters for the callable (Depending on the tracer implementation chosen)
    OPENTRACING_TRACER_PARAMETERS = {
        'example-parameter-host': 'collector',
    }

If you want to directly override the ``DjangoTracing`` used, you can use the following. This may cause import loops (See #10)

.. code-block:: python

    # some_opentracing_tracer can be any valid OpenTracing tracer implementation
    OPENTRACING_TRACING = django_opentracing.DjangoTracing(some_opentracing_tracer)

**Note:** Valid request attributes to trace are listed  `here`_. When you trace an attribute, this means that created spans will have tags with the attribute name and the request's value.

.. _here: https://docs.djangoproject.com/en/1.11/ref/request-response/#django.http.HttpRequest


Tracing All Requests
====================

In order to trace all requests, ``OPENTRACING_TRACE_ALL`` needs to be set to ``True`` (the default). If you want to trace any attributes for all requests, then add them to ``OPENTRACING_TRACED_ATTRIBUTES``. For example, if you wanted to trace the path and method, then set ``OPENTRACING_TRACED_ATTRIBUTES = ['path', 'method']``.

Tracing all requests uses the middleware django_opentracing.OpenTracingMiddleware, so add this to your settings.py file's ``MIDDLEWARE_CLASSES`` at the top of the stack.

.. code-block:: python

    MIDDLEWARE_CLASSES = [
        'django_opentracing.OpenTracingMiddleware',
        ... # other middleware classes
    ]

Tracing Individual Requests
===========================

If you don't want to trace all requests to your site, set ``OPENTRACING_TRACE_ALL`` to ``False``. Then you can use function decorators to trace individual view functions. This can be done by adding the following lines of code to views.py (or any other file that has url handler functions):

.. code-block:: python

    from django.conf import settings

    tracing = settings.OPENTRACING_TRACING

    @tracing.trace(optional_args)
    def some_view_func(request):
        ... # do some stuff

This tracing method doesn't use middleware, so there's no need to add it to your settings.py file.

The optional arguments allow for tracing of request attributes. For example, if you want to trace metadata, you could pass in ``@tracing.trace('META')`` and ``request.META`` would be set as a tag on all spans for this view function.

**Note:** If ``OPENTRACING_TRACE_ALL`` is set to ``True``, this decorator will be ignored, including any traced request attributes.

Accessing Spans Manually
========================

In order to access the span for a request, we've provided an method ``DjangoTracing.get_span(request)`` that returns the span for the request, if it is exists and is not finished. This can be used to log important events to the span, set tags, or create child spans to trace non-RPC events.

Tracing an RPC
==============

If you want to make an RPC and continue an existing trace, you can inject the current span into the RPC. For example, if making an http request, the following code will continue your trace across the wire:

.. code-block:: python

    @tracing.trace()
    def some_view_func(request):
        new_request = some_http_request
        current_span = tracing.get_span(request)
        text_carrier = {}
        opentracing_tracer.inject(span, opentracing.Format.TEXT_MAP, text_carrier)
        for k, v in text_carrier.items():
            request.add_header(k,v)
        ... # make request

Example
=======

Here is an `example`_ of a Django application that acts as both a client and server,
with integrated OpenTracing tracers.

.. _example: https://github.com/opentracing-contrib/python-django/tree/master/example

Breaking changes from 0.x
=========================

Starting with the 1.0 version, a few changes have taken place from previous versions:

* ``DjangoTracer`` has been renamed to ``DjangoTracing``, although ``DjangoTracer``
  can be used still as a deprecated name. Likewise for
  ``OPENTRACING_TRACER`` being renamed to ``OPENTRACING_TRACING``.
* When using the middleware layer, ``OPENTRACING_TRACE_ALL`` defaults to ``True``.
* When no ``opentracing.Tracer`` is provided, ``DjangoTracing`` will rely on the
  global tracer.

Further Information
===================

If you’re interested in learning more about the OpenTracing standard, please visit `opentracing.io`_ or `join the mailing list`_. If you would like to implement OpenTracing in your project and need help, feel free to send us a note at `community@opentracing.io`_.

.. _opentracing.io: http://opentracing.io/
.. _join the mailing list: http://opentracing.us13.list-manage.com/subscribe?u=180afe03860541dae59e84153&id=19117aa6cd
.. _community@opentracing.io: community@opentracing.io

