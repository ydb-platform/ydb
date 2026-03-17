=================================
Python multipart/form-data parser
=================================

.. image:: https://github.com/defnull/multipart/actions/workflows/test.yaml/badge.svg
    :target: https://github.com/defnull/multipart/actions/workflows/test.yaml
    :alt: Tests Status

.. image:: https://img.shields.io/pypi/v/multipart.svg
    :target: https://pypi.python.org/pypi/multipart/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/multipart.svg?color=%2334D058
    :target: https://pypi.python.org/pypi/multipart/
    :alt: Supported Python Version

.. image:: https://img.shields.io/pypi/l/multipart.svg
    :target: https://github.com/defnull/multipart/
    :alt: License

.. _HTML5: https://html.spec.whatwg.org/multipage/form-control-infrastructure.html#multipart-form-data
.. _RFC7578: https://www.rfc-editor.org/rfc/rfc7578
.. _WSGI: https://peps.python.org/pep-3333
.. _ASGI: https://asgi.readthedocs.io/en/latest/
.. _SansIO: https://sans-io.readthedocs.io/
.. _asyncio: https://docs.python.org/3/library/asyncio.html

This module provides a fast incremental non-blocking parser for
``multipart/form-data`` [HTML5_, RFC7578_], as well as blocking alternatives for
easier use in WSGI_ or CGI applications:

* **PushMultipartParser**: Fast SansIO_ (incremental, non-blocking) parser suitable
  for ASGI_, asyncio_ and other IO, time or memory constrained environments.
* **MultipartParser**: Streaming parser that reads from a byte stream and yields
  memory- or disk-buffered `MultipartPart` instances.
* **WSGI Helper**: High-level functions and containers for WSGI_ or CGI applications with support
  for both `multipart` and `urlencoded` form submissions.

Features
========

* Pure python single file module with no dependencies.
* Optimized for both blocking and non-blocking applications.
* 100% test coverage with test data from actual browsers and HTTP clients.
* High throughput and low latency (see `benchmarks <https://github.com/defnull/multipart_bench>`_).
* Predictable memory and disk resource consumption via fine grained limits.
* Strict mode: Spent less time parsing malicious or broken inputs.

Scope and compatibility
=======================
All parsers in this module implement ``multipart/form-data`` as defined by HTML5_
and RFC7578_, supporting all modern browsers or HTTP clients in use today.
Legacy browsers (e.g. IE6) are supported to some degree, but only if the
required workarounds do not impact performance or security. In detail this means:

* Just ``multipart/form-data``, not suitable for email parsing.
* No ``multipart/mixed`` support (deprecated in RFC7578_).
* No ``base64`` or ``quoted-printable`` transfer encoding (deprecated in RFC7578_).
* No ``encoded-word`` or ``name=_charset_`` encoding markers (deprecated in HTML5_).
* No support for clearly broken clients (e.g. invalid line breaks or headers).

Installation
============

``pip install multipart``

Documentation
=============

Examples and API documentation can be found at: https://multipart.readthedocs.io/

License
=======

.. __: https://github.com/defnull/multipart/raw/master/LICENSE

Code and documentation are available under MIT License (see LICENSE__).
