# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""

Instrument `django`_ to trace Django applications.

.. _django: https://pypi.org/project/django/

Usage
-----

.. code:: python

    from opentelemetry.instrumentation.django import DjangoInstrumentor

    DjangoInstrumentor().instrument()


Configuration
-------------

Exclude lists
*************
To exclude certain URLs from tracking, set the environment variable ``OTEL_PYTHON_DJANGO_EXCLUDED_URLS``
(or ``OTEL_PYTHON_EXCLUDED_URLS`` to cover all instrumentations) to a string of comma delimited regexes that match the
URLs.

For example,

::

    export OTEL_PYTHON_DJANGO_EXCLUDED_URLS="client/.*/info,healthcheck"

will exclude requests such as ``https://site/client/123/info`` and ``https://site/xyz/healthcheck``.

Request attributes
******************
To extract attributes from Django's request object and use them as span attributes, set the environment variable
``OTEL_PYTHON_DJANGO_TRACED_REQUEST_ATTRS`` to a comma delimited list of request attribute names.

For example,

::

    export OTEL_PYTHON_DJANGO_TRACED_REQUEST_ATTRS='path_info,content_type'

will extract the ``path_info`` and ``content_type`` attributes from every traced request and add them as span attributes.

* `Django Request object reference <https://docs.djangoproject.com/en/5.2/ref/request-response/#attributes>`_

Request and Response hooks
**************************
This instrumentation supports request and response hooks. These are functions that get called
right after a span is created for a request and right before the span is finished for the response.
The hooks can be configured as follows:

.. code:: python

    from opentelemetry.instrumentation.django import DjangoInstrumentor

    def request_hook(span, request):
        pass

    def response_hook(span, request, response):
        pass

    DjangoInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

* `Django Request object <https://docs.djangoproject.com/en/5.2/ref/request-response/#httprequest-objects>`_
* `Django Response object <https://docs.djangoproject.com/en/5.2/ref/request-response/#httpresponse-objects>`_

Adding attributes from middleware context
#########################################
In many Django applications, certain request attributes become available only *after*
specific middlewares have executed. For example:

- ``django.contrib.auth.middleware.AuthenticationMiddleware`` populates ``request.user``
- ``django.contrib.sites.middleware.CurrentSiteMiddleware`` populates ``request.site``

Because the OpenTelemetry instrumentation creates the span **before** Django middlewares run,
these attributes are **not yet available** in the ``request_hook`` stage.

Therefore, such attributes should be safely attached in the **response_hook**, which executes
after Django finishes processing the request (and after all middlewares have completed).

Example: Attaching the authenticated user and current site to the span:

.. code:: python

    def response_hook(span, request, response):
        # Attach user information if available
        if request.user.is_authenticated:
            span.set_attribute("enduser.id", request.user.pk)
            span.set_attribute("enduser.username", request.user.get_username())

        # Attach current site (if provided by CurrentSiteMiddleware)
        if hasattr(request, "site"):
            span.set_attribute("site.id", getattr(request.site, "pk", None))
            span.set_attribute("site.domain", getattr(request.site, "domain", None))

    DjangoInstrumentor().instrument(response_hook=response_hook)

This ensures that middleware-dependent context (like user or site information) is properly
recorded once Django’s middleware stack has finished execution.

Custom Django middleware can also attach arbitrary data to the ``request`` object,
which can later be included as span attributes in the ``response_hook``.

* `Django middleware reference <https://docs.djangoproject.com/en/5.2/topics/http/middleware/>`_

Best practices
##############
- Use **response_hook** (not request_hook) when accessing attributes added by Django middlewares.
- Common middleware-provided attributes include:

  - ``request.user`` (AuthenticationMiddleware)
  - ``request.site`` (CurrentSiteMiddleware)

- Avoid adding large or sensitive data (e.g., passwords, session tokens, PII) to spans.
- Use **namespaced attribute keys**, e.g., ``enduser.*``, ``site.*``, or ``custom.*``, for clarity.
- Hooks should execute quickly — avoid blocking or long-running operations.
- Hooks can be safely combined with OpenTelemetry **Context propagation** or **Baggage**
  for consistent tracing across services.

* `OpenTelemetry semantic conventions <https://opentelemetry.io/docs/specs/semconv/http/http-spans/>`_

Middleware execution order
##########################
In Django’s request lifecycle, the OpenTelemetry `request_hook` is executed before
the first middleware runs. Therefore:

- At `request_hook` time → only the bare `HttpRequest` object is available.
- After middlewares → `request.user`, `request.site` etc. become available.
- At `response_hook` time → all middlewares (including authentication and site middlewares)
  have already run, making it the correct place to attach these attributes.

Developers who need to trace attributes from middlewares should always use `response_hook`
to ensure complete and accurate span data.

Capture HTTP request and response headers
*****************************************
You can configure the agent to capture specified HTTP headers as span attributes, according to the
`semantic conventions <https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-spans.md#http-server-span>`_.

Request headers
***************
To capture HTTP request headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to a comma delimited list of HTTP header names.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="content-type,custom_request_header"

will extract ``content-type`` and ``custom_request_header`` from the request headers and add them as span attributes.

Request header names in Django are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
variable will capture the header named ``custom-header``.

Regular expressions may also be used to match multiple headers that correspond to the given pattern.  For example:
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST="Accept.*,X-.*"

Would match all request headers that start with ``Accept`` and ``X-``.

To capture all request headers, set ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST`` to ``".*"``.
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST=".*"

The name of the added span attribute will follow the format ``http.request.header.<header_name>`` where ``<header_name>``
is the normalized HTTP header name (lowercase, with ``-`` replaced by ``_``). The value of the attribute will be a
single item list containing all the header values.

For example:
``http.request.header.custom_request_header = ["<value1>,<value2>"]``

Response headers
****************
To capture HTTP response headers as span attributes, set the environment variable
``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to a comma delimited list of HTTP header names.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="content-type,custom_response_header"

will extract ``content-type`` and ``custom_response_header`` from the response headers and add them as span attributes.

Response header names in Django are case-insensitive. So, giving the header name as ``CUStom-Header`` in the environment
variable will capture the header named ``custom-header``.

Regular expressions may also be used to match multiple headers that correspond to the given pattern.  For example:
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE="Content.*,X-.*"

Would match all response headers that start with ``Content`` and ``X-``.

To capture all response headers, set ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE`` to ``".*"``.
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE=".*"

The name of the added span attribute will follow the format ``http.response.header.<header_name>`` where ``<header_name>``
is the normalized HTTP header name (lowercase, with ``-`` replaced by ``_``). The value of the attribute will be a
single item list containing all the header values.

For example:
``http.response.header.custom_response_header = ["<value1>,<value2>"]``

Sanitizing headers
******************
In order to prevent storing sensitive data such as personally identifiable information (PII), session keys, passwords,
etc, set the environment variable ``OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS``
to a comma delimited list of HTTP header names to be sanitized.  Regexes may be used, and all header names will be
matched in a case-insensitive manner.

For example,
::

    export OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS=".*session.*,set-cookie"

will replace the value of headers such as ``session-id`` and ``set-cookie`` with ``[REDACTED]`` in the span.

Note:
    The environment variable names used to capture HTTP headers are still experimental, and thus are subject to change.

SQLCommenter
************
You can optionally enable sqlcommenter which enriches the query with contextual
information. Queries made after setting up trace integration with sqlcommenter
enabled will have configurable key-value pairs appended to them, e.g.
``Users().objects.all()`` will result in
``"select * from auth_users; /*traceparent=00-01234567-abcd-01*/"``. This
supports context propagation between database client and server when database log
records are enabled. For more information, see:

* `Semantic Conventions - Database Spans <https://github.com/open-telemetry/semantic-conventions/blob/main/docs/database/database-spans.md#sql-commenter>`_
* `sqlcommenter <https://google.github.io/sqlcommenter/>`_

.. code:: python

    from opentelemetry.instrumentation.django import DjangoInstrumentor

    DjangoInstrumentor().instrument(is_sql_commentor_enabled=True)

Warning:
    Duplicate sqlcomments may be appended to the sqlquery log if DjangoInstrumentor
    sqlcommenter is enabled in addition to sqlcommenter for an active instrumentation
    of a database driver or object-relational mapper (ORM) in the same database client
    stack. For example, if psycopg2 driver is used and Psycopg2Instrumentor has
    sqlcommenter enabled, then both DjangoInstrumentor and Psycopg2Instrumentor will
    append comments to the query statement.

SQLCommenter with commenter_options
***********************************
The key-value pairs appended to the query can be configured using
variables in Django ``settings.py``. When sqlcommenter is enabled, all
available KVs/tags are calculated by default, i.e. ``True`` for each. The
``settings.py`` values support *opting out* of specific KVs.

Available settings.py commenter options
#######################################

We can configure the tags to be appended to the sqlquery log by adding below variables to
``settings.py``, e.g. ``SQLCOMMENTER_WITH_FRAMEWORK = False``

+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``settings.py`` variable            | Description                                               | Example                                                                   |
+=====================================+===========================================================+===========================================================================+
| ``SQLCOMMENTER_WITH_FRAMEWORK``     | Django framework name with version (URL encoded).         | ``framework='django%%%%3A4.2.0'``                                         |
+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``SQLCOMMENTER_WITH_CONTROLLER``    | Django controller/view name that handles the request.     | ``controller='index'``                                                    |
+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``SQLCOMMENTER_WITH_ROUTE``         | URL path pattern that handles the request.                | ``route='polls/'``                                                        |
+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``SQLCOMMENTER_WITH_APP_NAME``      | Django app name that handles the request.                 | ``app_name='polls'``                                                      |
+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``SQLCOMMENTER_WITH_OPENTELEMETRY`` | OpenTelemetry context as traceparent at time of query.    | ``traceparent='00-fd720cffceba94bbf75940ff3caaf3cc-4fd1a2bdacf56388-01'`` |
+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+
| ``SQLCOMMENTER_WITH_DB_DRIVER``     | Database driver name used by Django.                      | ``db_driver='django.db.backends.postgresql'``                             |
+-------------------------------------+-----------------------------------------------------------+---------------------------------------------------------------------------+

API
---

"""

from logging import getLogger
from os import environ
from typing import Collection

from django import VERSION as django_version
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from opentelemetry.instrumentation._semconv import (
    HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
    _get_schema_url,
    _OpenTelemetrySemanticConventionStability,
    _OpenTelemetryStabilitySignalType,
    _report_new,
    _report_old,
)
from opentelemetry.instrumentation.django.environment_variables import (
    OTEL_PYTHON_DJANGO_INSTRUMENT,
)
from opentelemetry.instrumentation.django.middleware.otel_middleware import (
    _DjangoMiddleware,
)
from opentelemetry.instrumentation.django.package import _instruments
from opentelemetry.instrumentation.django.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.metrics import get_meter
from opentelemetry.semconv._incubating.metrics.http_metrics import (
    create_http_server_active_requests,
)
from opentelemetry.semconv.metrics import MetricInstruments
from opentelemetry.semconv.metrics.http_metrics import (
    HTTP_SERVER_REQUEST_DURATION,
)
from opentelemetry.trace import get_tracer
from opentelemetry.util.http import get_excluded_urls, parse_excluded_urls

DJANGO_2_0 = django_version >= (2, 0)

_excluded_urls_from_env = get_excluded_urls("DJANGO")
_logger = getLogger(__name__)


def _get_django_middleware_setting() -> str:
    # In Django versions 1.x, setting MIDDLEWARE_CLASSES can be used as a legacy
    # alternative to MIDDLEWARE. This is the case when `settings.MIDDLEWARE` has
    # its default value (`None`).
    if not DJANGO_2_0 and getattr(settings, "MIDDLEWARE", None) is None:
        return "MIDDLEWARE_CLASSES"
    return "MIDDLEWARE"


def _get_django_otel_middleware_position(
    middleware_length, default_middleware_position=0
):
    otel_position = environ.get("OTEL_PYTHON_DJANGO_MIDDLEWARE_POSITION")
    try:
        middleware_position = int(otel_position)
    except (ValueError, TypeError):
        _logger.debug(
            "Invalid OTEL_PYTHON_DJANGO_MIDDLEWARE_POSITION value: (%s). Using default position: %d.",
            otel_position,
            default_middleware_position,
        )
        middleware_position = default_middleware_position

    if middleware_position < 0 or middleware_position > middleware_length:
        _logger.debug(
            "Middleware position %d is out of range (0-%d). Using 0 as the position",
            middleware_position,
            middleware_length,
        )
        middleware_position = 0
    return middleware_position


class DjangoInstrumentor(BaseInstrumentor):
    """An instrumentor for Django

    See `BaseInstrumentor`
    """

    _opentelemetry_middleware = ".".join(
        [_DjangoMiddleware.__module__, _DjangoMiddleware.__qualname__]
    )

    _sql_commenter_middleware = "opentelemetry.instrumentation.django.middleware.sqlcommenter_middleware.SqlCommenter"

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        # FIXME this is probably a pattern that will show up in the rest of the
        # ext. Find a better way of implementing this.
        if environ.get(OTEL_PYTHON_DJANGO_INSTRUMENT) == "False":
            return

        # initialize semantic conventions opt-in if needed
        _OpenTelemetrySemanticConventionStability._initialize()
        sem_conv_opt_in_mode = _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
            _OpenTelemetryStabilitySignalType.HTTP,
        )

        tracer_provider = kwargs.get("tracer_provider")
        meter_provider = kwargs.get("meter_provider")
        _excluded_urls = kwargs.get("excluded_urls")
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=_get_schema_url(sem_conv_opt_in_mode),
        )
        meter = get_meter(
            __name__,
            __version__,
            meter_provider=meter_provider,
            schema_url=_get_schema_url(sem_conv_opt_in_mode),
        )
        _DjangoMiddleware._sem_conv_opt_in_mode = sem_conv_opt_in_mode
        _DjangoMiddleware._tracer = tracer
        _DjangoMiddleware._meter = meter
        _DjangoMiddleware._excluded_urls = (
            _excluded_urls_from_env
            if _excluded_urls is None
            else parse_excluded_urls(_excluded_urls)
        )
        _DjangoMiddleware._otel_request_hook = kwargs.pop("request_hook", None)
        _DjangoMiddleware._otel_response_hook = kwargs.pop(
            "response_hook", None
        )
        _DjangoMiddleware._duration_histogram_old = None
        if _report_old(sem_conv_opt_in_mode):
            _DjangoMiddleware._duration_histogram_old = meter.create_histogram(
                name=MetricInstruments.HTTP_SERVER_DURATION,
                unit="ms",
                description="Measures the duration of inbound HTTP requests.",
            )
        _DjangoMiddleware._duration_histogram_new = None
        if _report_new(sem_conv_opt_in_mode):
            _DjangoMiddleware._duration_histogram_new = meter.create_histogram(
                name=HTTP_SERVER_REQUEST_DURATION,
                description="Duration of HTTP server requests.",
                unit="s",
                explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
            )
        _DjangoMiddleware._active_request_counter = (
            create_http_server_active_requests(meter)
        )
        # This can not be solved, but is an inherent problem of this approach:
        # the order of middleware entries matters, and here you have no control
        # on that:
        # https://docs.djangoproject.com/en/3.0/topics/http/middleware/#activating-middleware
        # https://docs.djangoproject.com/en/3.0/ref/middleware/#middleware-ordering

        _middleware_setting = _get_django_middleware_setting()
        settings_middleware = []
        try:
            settings_middleware = getattr(settings, _middleware_setting, [])
        except ImproperlyConfigured as exception:
            _logger.debug(
                "DJANGO_SETTINGS_MODULE environment variable not configured. Defaulting to empty settings: %s",
                exception,
            )
            settings.configure()
            settings_middleware = getattr(settings, _middleware_setting, [])
        except ModuleNotFoundError as exception:
            _logger.debug(
                "DJANGO_SETTINGS_MODULE points to a non-existent module. Defaulting to empty settings: %s",
                exception,
            )
            settings.configure()
            settings_middleware = getattr(settings, _middleware_setting, [])

        # Django allows to specify middlewares as a tuple, so we convert this tuple to a
        # list, otherwise we wouldn't be able to call append/remove
        if isinstance(settings_middleware, tuple):
            settings_middleware = list(settings_middleware)

        is_sql_commentor_enabled = kwargs.pop("is_sql_commentor_enabled", None)

        middleware_position = _get_django_otel_middleware_position(
            len(settings_middleware), kwargs.pop("middleware_position", 0)
        )

        if is_sql_commentor_enabled:
            settings_middleware.insert(
                middleware_position, self._sql_commenter_middleware
            )

        settings_middleware.insert(
            middleware_position, self._opentelemetry_middleware
        )

        setattr(settings, _middleware_setting, settings_middleware)

    def _uninstrument(self, **kwargs):
        _middleware_setting = _get_django_middleware_setting()
        settings_middleware = getattr(settings, _middleware_setting, None)

        # FIXME This is starting to smell like trouble. We have 2 mechanisms
        # that may make this condition be True, one implemented in
        # BaseInstrumentor and another one implemented in _instrument. Both
        # stop _instrument from running and thus, settings_middleware not being
        # set.
        if settings_middleware is None or (
            self._opentelemetry_middleware not in settings_middleware
        ):
            return

        settings_middleware.remove(self._opentelemetry_middleware)
        setattr(settings, _middleware_setting, settings_middleware)
