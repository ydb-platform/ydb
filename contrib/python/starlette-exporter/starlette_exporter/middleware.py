""" Middleware for exporting Prometheus metrics using Starlette """
import inspect
import logging
import re
import time
import warnings
from collections import OrderedDict
from contextlib import suppress
from inspect import iscoroutine
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from prometheus_client import Counter, Gauge, Histogram
from prometheus_client.metrics import MetricWrapperBase
from starlette.requests import Request
from starlette.routing import BaseRoute, Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from starlette_exporter.labels import ResponseHeaderLabel

from . import optional_metrics

logger = logging.getLogger("starlette_exporter")


def get_matching_route_path(
    scope: Dict[Any, Any],
    routes: List[BaseRoute],
    route_name: Optional[str] = None,
) -> Optional[str]:
    """
    Find a matching route and return its original path string

    Will attempt to enter mounted routes and subrouters.

    Credit to https://github.com/elastic/apm-agent-python
    """

    for route in routes:
        match, child_scope = route.matches(scope)
        if match == Match.FULL:
            # set route name
            route_name = getattr(route, "path", None)
            if route_name is None:
                return None

            # for routes of type `BaseRoute`, the base route name may not
            # be the complete path (it may represent the path to the
            # mounted router). If this is a mounted route, descend into it to
            # get the complete path.
            if isinstance(route, BaseRoute) and getattr(route, "routes", None):
                child_scope = {**scope, **child_scope}
                child_route_name = get_matching_route_path(
                    child_scope,
                    getattr(route, "routes"),
                    route_name,
                )
                if child_route_name is None:
                    route_name = None
                else:
                    route_name += child_route_name
            return route_name
        elif match == Match.PARTIAL and route_name is None:
            route_name = getattr(route, "path", None)

    return None


class PrometheusMiddleware:
    """Middleware that collects Prometheus metrics for each request.
    Use in conjuction with the Prometheus exporter endpoint handler.
    """

    _metrics: ClassVar[Dict[str, MetricWrapperBase]] = {}

    def __init__(
        self,
        app: ASGIApp,
        group_paths: bool = True,
        app_name: str = "starlette",
        prefix: str = "starlette",
        buckets: Optional[Sequence[Union[float, str]]] = None,
        filter_unhandled_paths: bool = True,
        skip_paths: Optional[List[str]] = None,
        skip_methods: Optional[List[str]] = None,
        optional_metrics: Optional[List[str]] = None,
        always_use_int_status: bool = False,
        labels: Optional[Mapping[str, Union[str, Callable]]] = None,
        exemplars: Optional[Callable] = None,
        group_unhandled_paths: bool = False,
    ):
        self.app = app
        self.app_name = app_name
        self.prefix = prefix
        self.group_paths = group_paths

        if group_unhandled_paths and filter_unhandled_paths:
            filter_unhandled_paths = False
            warnings.warn(
                "filter_unhandled_paths was set to True but has been changed to False "
                "because group_unhandled_paths is True and these settings are mutually exclusive",
                UserWarning,
            )

        self.group_unhandled_paths = group_unhandled_paths
        self.filter_unhandled_paths = filter_unhandled_paths

        self.kwargs = {}
        if buckets is not None:
            self.kwargs["buckets"] = buckets
        self.skip_paths: List[re.Pattern] = []
        if skip_paths is not None:
            self.skip_paths = [re.compile(path) for path in skip_paths]
        self.skip_methods = []
        if skip_methods is not None:
            self.skip_methods = skip_methods
        self.optional_metrics_list = []
        if optional_metrics is not None:
            self.optional_metrics_list = optional_metrics
        self.always_use_int_status = always_use_int_status

        self.exemplars = exemplars
        self._exemplars_req_kw = ""

        if self.exemplars:
            # if the exemplars func has an argument annotated as Request, note its name.
            # it will be used to inject the request when the func is called
            exemplar_sig = inspect.signature(self.exemplars)
            for p in exemplar_sig.parameters.values():
                if p.annotation is Request:
                    self._exemplars_req_kw = p.name
                    break
            else:
                # if there's no parameter with a Request type annotation but there is a
                # parameter with name "request", it will be chosen for injection
                if "request" in exemplar_sig.parameters:
                    self._exemplars_req_kw = "request"


        # split labels into request and response labels.
        # response labels will be evaluated while the response is
        # written.
        self.request_labels = OrderedDict({})
        self.response_labels: OrderedDict[str, ResponseHeaderLabel] = OrderedDict({})

        if labels is not None:
            for k, v in labels.items():
                if isinstance(v, ResponseHeaderLabel):
                    self.response_labels[k] = v
                else:
                    self.request_labels[k] = v

    # Default metrics
    # Starlette initialises middleware multiple times, so store metrics on the class

    @property
    def request_count(self):
        metric_name = f"{self.prefix}_requests_total"
        if metric_name not in PrometheusMiddleware._metrics:
            PrometheusMiddleware._metrics[metric_name] = Counter(
                metric_name,
                "Total HTTP requests",
                (
                    "method",
                    "path",
                    "status_code",
                    "app_name",
                    *self.request_labels.keys(),
                    *self.response_labels.keys(),
                ),
            )
        return PrometheusMiddleware._metrics[metric_name]

    @property
    def response_body_size_count(self):
        """
        Optional metric for tracking the size of response bodies.
        If using gzip middleware, you should test that the starlette_exporter middleware computes
        the proper response size value. Please post any feedback on this metric as an issue
        at https://github.com/stephenhillier/starlette_exporter.

        """
        if (
            self.optional_metrics_list is not None
            and optional_metrics.response_body_size in self.optional_metrics_list
        ):
            metric_name = f"{self.prefix}_response_body_bytes_total"
            if metric_name not in PrometheusMiddleware._metrics:
                PrometheusMiddleware._metrics[metric_name] = Counter(
                    metric_name,
                    "Total HTTP response body bytes",
                    (
                        "method",
                        "path",
                        "status_code",
                        "app_name",
                        *self.request_labels.keys(),
                        *self.response_labels.keys(),
                    ),
                )
            return PrometheusMiddleware._metrics[metric_name]
        else:
            pass

    @property
    def request_body_size_count(self):
        """
        Optional metric tracking the received content-lengths of request bodies
        """
        if (
            self.optional_metrics_list is not None
            and optional_metrics.request_body_size in self.optional_metrics_list
        ):
            metric_name = f"{self.prefix}_request_body_bytes_total"
            if metric_name not in PrometheusMiddleware._metrics:
                PrometheusMiddleware._metrics[metric_name] = Counter(
                    metric_name,
                    "Total HTTP request body bytes",
                    (
                        "method",
                        "path",
                        "status_code",
                        "app_name",
                        *self.request_labels.keys(),
                        *self.response_labels.keys(),
                    ),
                )
            return PrometheusMiddleware._metrics[metric_name]
        else:
            pass

    @property
    def request_time(self):
        metric_name = f"{self.prefix}_request_duration_seconds"
        if metric_name not in PrometheusMiddleware._metrics:
            PrometheusMiddleware._metrics[metric_name] = Histogram(
                metric_name,
                "HTTP request duration, in seconds",
                (
                    "method",
                    "path",
                    "status_code",
                    "app_name",
                    *self.request_labels.keys(),
                    *self.response_labels.keys(),
                ),
                **self.kwargs,
            )
        return PrometheusMiddleware._metrics[metric_name]

    @property
    def requests_in_progress(self):
        metric_name = f"{self.prefix}_requests_in_progress"
        if metric_name not in PrometheusMiddleware._metrics:
            PrometheusMiddleware._metrics[metric_name] = Gauge(
                metric_name,
                "Total HTTP requests currently in progress",
                ("method", "app_name", *self.request_labels.keys()),
                multiprocess_mode="livesum",
            )
        return PrometheusMiddleware._metrics[metric_name]

    async def _request_label_values(self, request: Request) -> List[str]:
        values: List[str] = []

        for k, v in self.request_labels.items():
            if callable(v):
                parsed_value = ""
                # if provided a callable, try to use it on the request.
                try:
                    result = v(request)
                    if iscoroutine(result):
                        result = await result
                except Exception:
                    logger.warn(f"label function for {k} failed", exc_info=True)
                else:
                    parsed_value = str(result)
                values.append(parsed_value)
                continue

            values.append(v)

        return values

    def _response_label_values(self, message: Message) -> List[str]:
        values: List[str] = []

        # bail if no response labels were defined by the user
        if not self.response_labels:
            return values

        # create a dict of headers to make it easy to find keys
        headers = {
            k.decode("utf-8").lower(): v.decode("utf-8")
            for (k, v) in message.get("headers", ())
        }

        for k, v in self.response_labels.items():
            # currently only ResponseHeaderLabel supported
            if isinstance(v, ResponseHeaderLabel):
                parsed_value = ""
                try:
                    result = v(headers)
                except Exception:
                    logger.warn(f"label function for {k} failed", exc_info=True)
                else:
                    parsed_value = str(result)
                values.append(parsed_value)


        return values

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ["http"]:
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)

        method = request.method
        path = request.url.path
        base_path = request.base_url.path.rstrip("/")

        if base_path and path.startswith(base_path):
            path = path[len(base_path) :]

        if any(pattern.fullmatch(path) for pattern in self.skip_paths) or method in self.skip_methods:
            await self.app(scope, receive, send)
            return

        begin = time.perf_counter()
        end = None

        request_labels = await self._request_label_values(request)

        # Increment requests_in_progress gauge when request comes in
        self.requests_in_progress.labels(method, self.app_name, *request_labels).inc()

        status_code = None

        # custom response label values, to be populated when response is written.
        response_labels = []

        # optional request and response body size metrics
        response_body_size: int = 0

        request_body_size: int = 0
        if (
            self.optional_metrics_list is not None
            and optional_metrics.request_body_size in self.optional_metrics_list
        ):
            if request.headers.get("content-length"):
                request_body_size = int(request.headers["content-length"])

        async def wrapped_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                nonlocal status_code
                status_code = message["status"]

                nonlocal response_labels
                response_labels = self._response_label_values(message)

                if self.always_use_int_status:
                    try:
                        status_code = int(message["status"])
                    except ValueError:
                        logger.warning(
                            f"always_use_int_status flag selected but failed to convert status_code to int for value: {status_code}"
                        )

                # find response body size for optional metric
                if (
                    self.optional_metrics_list is not None
                    and optional_metrics.response_body_size
                    in self.optional_metrics_list
                ):
                    nonlocal response_body_size
                    for message_content_length in message["headers"]:
                        if (
                            message_content_length[0].decode("utf-8")
                            == "content-length"
                        ):
                            response_body_size += int(
                                message_content_length[1].decode("utf-8")
                            )

            if message["type"] == "http.response.body":
                nonlocal end
                end = time.perf_counter()

            await send(message)

        exception: Optional[Exception] = None
        original_scope = scope.copy()
        try:
            await self.app(scope, receive, wrapped_send)
        except Exception as e:
            status_code = 500

            # during an unhandled exception, populate response labels with empty strings.
            response_labels = self._response_label_values({})

            exception = e
        finally:
            # Decrement 'requests_in_progress' gauge after response sent
            self.requests_in_progress.labels(
                method, self.app_name, *request_labels
            ).dec()

        if status_code is None:
            if await request.is_disconnected():
                # In case no response was returned and the client is disconnected, 499 is reported as status code.
                status_code = 499
            else:
                status_code = 500

        if self.filter_unhandled_paths or self.group_paths or self.group_unhandled_paths:
            grouped_path: Optional[str] = None

            endpoint = scope.get("endpoint", None)
            router = scope.get("router", None)
            if endpoint and router:
                with suppress(Exception):
                    grouped_path = get_matching_route_path(original_scope, router.routes)

            # filter_unhandled_paths removes any requests without mapped endpoint from the metrics.
            if self.filter_unhandled_paths and grouped_path is None:
                if exception:
                    raise exception
                return

            # group_unhandled_paths works similar to filter_unhandled_paths, but instead of
            # removing the request from the metrics, it groups it under a single path.
            if self.group_unhandled_paths and grouped_path is None:
                path = "__unknown__"

            # group_paths enables returning the original router path (with url param names)
            # for example, when using this option, requests to /api/product/1 and /api/product/3
            # will both be grouped under /api/product/{product_id}. See the README for more info.
            if self.group_paths and grouped_path is not None:
                path = grouped_path

        labels = [
                method,
                path,
                status_code,
                self.app_name,
                *request_labels,
                *response_labels,
            ]

        # optional extra arguments to be passed as kwargs to observations
        # note: only used for histogram observations and counters to support exemplars
        extra = {}
        if self.exemplars:
            exemplar_kwargs = {}
            if self._exemplars_req_kw:
                exemplar_kwargs[self._exemplars_req_kw] = request
            extra["exemplar"] = self.exemplars(**exemplar_kwargs)

        # optional response body size metric
        if (
            self.optional_metrics_list is not None
            and optional_metrics.response_body_size in self.optional_metrics_list
            and self.response_body_size_count is not None
        ):
            self.response_body_size_count.labels(*labels).inc(
                response_body_size, **extra
            )

        # optional request body size metric
        if (
            self.optional_metrics_list is not None
            and optional_metrics.request_body_size in self.optional_metrics_list
            and self.request_body_size_count is not None
        ):
            self.request_body_size_count.labels(*labels).inc(
                request_body_size, **extra
            )

        # if we were not able to set end when the response body was written,
        # set it now.
        if end is None:
            end = time.perf_counter()

        self.request_count.labels(*labels).inc(**extra)
        self.request_time.labels(*labels).observe(end - begin, **extra)

        if exception:
            raise exception

