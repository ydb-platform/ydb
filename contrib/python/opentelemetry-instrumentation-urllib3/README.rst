OpenTelemetry urllib3 Instrumentation
======================================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-instrumentation-urllib3.svg
   :target: https://pypi.org/project/opentelemetry-instrumentation-urllib3/

This library allows tracing HTTP requests made by the
`urllib3 <https://urllib3.readthedocs.io/>`_ library.

Installation
------------

::

     pip install opentelemetry-instrumentation-urllib3

Usage
-----
.. code-block:: python

    import urllib3
    from opentelemetry.instrumentation.urllib3 import URLLib3Instrumentor

    def strip_query_params(url: str) -> str:
        return url.split("?")[0]

    URLLib3Instrumentor().instrument(
        # Remove all query params from the URL attribute on the span.
        url_filter=strip_query_params,
    )

    http = urllib3.PoolManager()
    response = http.request("GET", "https://www.example.org/")

Configuration
-------------

Request/Response hooks
**********************

The urllib3 instrumentation supports extending tracing behavior with the help of
request and response hooks. These are functions that are called back by the instrumentation
right after a Span is created for a request and right before the span is finished processing a response respectively.
The hooks can be configured as follows:

.. code:: python

    from typing import Any

    from urllib3.connectionpool import HTTPConnectionPool
    from urllib3.response import HTTPResponse

    from opentelemetry.instrumentation.urllib3 import RequestInfo, URLLib3Instrumentor
    from opentelemetry.trace import Span

    def request_hook(
        span: Span,
        pool: HTTPConnectionPool,
        request_info: RequestInfo,
    ) -> Any:
        pass

    def response_hook(
        span: Span,
        pool: HTTPConnectionPool,
        response: HTTPResponse,
    ) -> Any:
        pass

    URLLib3Instrumentor().instrument(
        request_hook=request_hook,
        response_hook=response_hook,
    )

Exclude lists
*************

To exclude certain URLs from being tracked, set the environment variable ``OTEL_PYTHON_URLLIB3_EXCLUDED_URLS``
(or ``OTEL_PYTHON_EXCLUDED_URLS`` as fallback) with comma delimited regexes representing which URLs to exclude.

For example,

::

    export OTEL_PYTHON_URLLIB3_EXCLUDED_URLS="client/.*/info,healthcheck"

will exclude requests such as ``https://site/client/123/info`` and ``https://site/xyz/healthcheck``.

References
----------

* `OpenTelemetry urllib3 Instrumentation <https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/urllib3/urllib3.html>`_
* `OpenTelemetry Project <https://opentelemetry.io/>`_
* `OpenTelemetry Python Examples <https://github.com/open-telemetry/opentelemetry-python/tree/main/docs/examples>`_
