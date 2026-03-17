"""
This module contains ready-to-use functions that can be passed on to the
instrumentator instance with the `add()` method. The idea behind this is to
make the types of metrics you want to export with the instrumentation easily
customizable. The default instrumentation function `default` can also be found
here.

If your requirements are really specific or very extensive it makes sense to
create your own instrumentation function instead of combining several functions
from this module.
"""

from typing import Callable, List, Optional, Sequence, Tuple, Union

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram, Summary
from starlette.requests import Request
from starlette.responses import Response


# ------------------------------------------------------------------------------
class Info:
    def __init__(
        self,
        request: Request,
        response: Optional[Response],
        method: str,
        modified_handler: str,
        modified_status: str,
        modified_duration: float,
        modified_duration_without_streaming: float = 0.0,
    ):
        """Creates Info object that is used for instrumentation functions.

        This is the only argument that is passed to the instrumentation functions.

        Args:
            request (Request): Python Requests request object.
            response (Response or None): Python Requests response object.
            method (str): Unmodified method of the request.
            modified_handler (str): Handler representation after processing by
                instrumentator. For example grouped to `none` if not templated.
            modified_status (str): Status code representation after processing
                by instrumentator. For example grouping into `2xx`, `3xx` and so on.
            modified_duration (float): Latency representation after processing
                by instrumentator. For example rounding of decimals. Seconds.
            modified_duration_without_streaming (float): Latency between request arrival and response starts (i.e. first chunk duration).
                Excluding the streaming duration. Defaults to 0.
        """

        self.request = request
        self.response = response
        self.method = method
        self.modified_handler = modified_handler
        self.modified_status = modified_status
        self.modified_duration = modified_duration
        self.modified_duration_without_streaming = modified_duration_without_streaming


def _build_label_attribute_names(
    should_include_handler: bool,
    should_include_method: bool,
    should_include_status: bool,
) -> Tuple[List[str], List[str]]:
    """Builds up tuple with to be used label and attribute names.

    Args:
        should_include_handler (bool): Should the `handler` label be part of the metric?
        should_include_method (bool): Should the `method` label be part of the metric?
        should_include_status (bool): Should the `status` label be part of the metric?

    Returns:
        Tuple with two list elements.

        First element: List with all labels to be used.
        Second element: List with all attribute names to be used from the
            `Info` object. Done like this to enable dynamic on / off of labels.
    """

    label_names = []
    info_attribute_names = []

    if should_include_handler:
        label_names.append("handler")
        info_attribute_names.append("modified_handler")

    if should_include_method:
        label_names.append("method")
        info_attribute_names.append("method")

    if should_include_status:
        label_names.append("status")
        info_attribute_names.append("modified_status")

    return label_names, info_attribute_names


def _is_duplicated_time_series(error: ValueError) -> bool:
    return any(
        map(
            error.args[0].__contains__,
            [
                "Duplicated timeseries in CollectorRegistry:",
                "Duplicated time series in CollectorRegistry:",
            ],
        )
    )


# ------------------------------------------------------------------------------
# Instrumentation / Metrics functions


def latency(
    metric_name: str = "http_request_duration_seconds",
    metric_doc: str = "Duration of HTTP requests in seconds",
    metric_namespace: str = "",
    metric_subsystem: str = "",
    should_include_handler: bool = True,
    should_include_method: bool = True,
    should_include_status: bool = True,
    should_exclude_streaming_duration: bool = False,
    buckets: Sequence[Union[float, str]] = Histogram.DEFAULT_BUCKETS,
    registry: CollectorRegistry = REGISTRY,
    custom_labels: dict = {},
) -> Optional[Callable[[Info], None]]:
    """Default metric for the Prometheus Starlette Instrumentator.

    Args:
        metric_name (str, optional): Name of the metric to be created. Must be
            unique. Defaults to "http_request_duration_seconds".

        metric_doc (str, optional): Documentation of the metric. Defaults to
            "Duration of HTTP requests in seconds".

        metric_namespace (str, optional): Namespace of all  metrics in this
            metric function. Defaults to "".

        metric_subsystem (str, optional): Subsystem of all  metrics in this
            metric function. Defaults to "".

        should_include_handler: Should the `handler` label be part of the
            metric? Defaults to `True`.

        should_include_method: Should the `method` label be part of the
            metric? Defaults to `True`.

        should_include_status: Should the `status` label be part of the
            metric? Defaults to `True`.

        should_exclude_streaming_duration: Should the streaming duration be
            excluded? Defaults to `False`.

        buckets: Buckets for the histogram. Defaults to Prometheus default.
            Defaults to default buckets from Prometheus client library.

    Returns:
        Function that takes a single parameter `Info`.
    """

    if buckets[-1] != float("inf"):
        buckets = [*buckets, float("inf")]

    label_names, info_attribute_names = _build_label_attribute_names(
        should_include_handler, should_include_method, should_include_status
    )
    for key in custom_labels:
        label_names.append(key)
        info_attribute_names.append(key)

    # Starlette will call app.build_middleware_stack() with every new middleware
    # added, which will call all this again, which will make the registry
    # complain about duplicated metrics.
    #
    # The Python Prometheus client currently doesn't seem to have a way to
    # verify if adding a metric will cause errors or not, so the only way to
    # handle it seems to be with this try block.
    try:
        if label_names:
            METRIC = Histogram(
                metric_name,
                metric_doc,
                labelnames=label_names,
                buckets=buckets,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )
        else:
            METRIC = Histogram(
                metric_name,
                metric_doc,
                buckets=buckets,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )

        def instrumentation(info: Info) -> None:
            duration = info.modified_duration
            if should_exclude_streaming_duration:
                duration = info.modified_duration_without_streaming
            else:
                duration = info.modified_duration

            if label_names:
                label_values = [
                    getattr(info, attribute_name)
                    for attribute_name in info_attribute_names
                ]

                METRIC.labels(*label_values).observe(duration)
            else:
                METRIC.observe(duration)

        return instrumentation
    except ValueError as e:
        if not _is_duplicated_time_series(e):
            raise e

    return None


def request_size(
    metric_name: str = "http_request_size_bytes",
    metric_doc: str = "Content bytes of requests.",
    metric_namespace: str = "",
    metric_subsystem: str = "",
    should_include_handler: bool = True,
    should_include_method: bool = True,
    should_include_status: bool = True,
    registry: CollectorRegistry = REGISTRY,
    custom_labels: dict = {},
) -> Optional[Callable[[Info], None]]:
    """Record the content length of incoming requests.

    If content length is missing 0 will be assumed.

    Args:
        metric_name (str, optional): Name of the metric to be created. Must be
            unique. Defaults to "http_request_size_bytes".
        metric_doc (str, optional): Documentation of the metric. Defaults to
            "Content bytes of requests.".
        metric_namespace (str, optional): Namespace of all  metrics in this
            metric function. Defaults to "".
        metric_subsystem (str, optional): Subsystem of all  metrics in this
            metric function. Defaults to "".
        should_include_handler: Should the `handler` label be part of the
            metric? Defaults to `True`.
        should_include_method: Should the `method` label be part of the
            metric? Defaults to `True`.
        should_include_status: Should the `status` label be part of the metric?
            Defaults to `True`.

    Returns:
        Function that takes a single parameter `Info`.
    """

    label_names, info_attribute_names = _build_label_attribute_names(
        should_include_handler, should_include_method, should_include_status
    )
    for key in custom_labels:
        label_names.append(key)
        info_attribute_names.append(key)

    # Starlette will call app.build_middleware_stack() with every new middleware
    # added, which will call all this again, which will make the registry
    # complain about duplicated metrics.
    #
    # The Python Prometheus client currently doesn't seem to have a way to
    # verify if adding a metric will cause errors or not, so the only way to
    # handle it seems to be with this try block.
    try:
        if label_names:
            METRIC = Summary(
                metric_name,
                metric_doc,
                labelnames=label_names,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )
        else:
            METRIC = Summary(
                metric_name,
                metric_doc,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )

        def instrumentation(info: Info) -> None:
            content_length = info.request.headers.get("Content-Length", 0)
            if label_names:
                label_values = [
                    getattr(info, attribute_name)
                    for attribute_name in info_attribute_names
                ]

                METRIC.labels(*label_values).observe(int(content_length))
            else:
                METRIC.observe(int(content_length))

        return instrumentation
    except ValueError as e:
        if not _is_duplicated_time_series(e):
            raise e

    return None


def response_size(
    metric_name: str = "http_response_size_bytes",
    metric_doc: str = "Content bytes of responses.",
    metric_namespace: str = "",
    metric_subsystem: str = "",
    should_include_handler: bool = True,
    should_include_method: bool = True,
    should_include_status: bool = True,
    registry: CollectorRegistry = REGISTRY,
    custom_labels: dict = {},
) -> Optional[Callable[[Info], None]]:
    """Record the content length of outgoing responses.

    If content length is missing 0 will be assumed.

    Args:
        metric_name (str, optional): Name of the metric to be created. Must be
            unique. Defaults to "http_response_size_bytes".

        metric_doc (str, optional): Documentation of the metric. Defaults to
            "Content bytes of responses.".

        metric_namespace (str, optional): Namespace of all  metrics in this
            metric function. Defaults to "".

        metric_subsystem (str, optional): Subsystem of all  metrics in this
            metric function. Defaults to "".

        should_include_handler: Should the `handler` label be part of the
            metric? Defaults to `True`.

        should_include_method: Should the `method` label be part of the metric?
            Defaults to `True`.

        should_include_status: Should the `status` label be part of the metric?
            Defaults to `True`.

    Returns:
        Function that takes a single parameter `Info`.
    """

    label_names, info_attribute_names = _build_label_attribute_names(
        should_include_handler, should_include_method, should_include_status
    )
    for key in custom_labels:
        label_names.append(key)
        info_attribute_names.append(key)

    # Starlette will call app.build_middleware_stack() with every new middleware
    # added, which will call all this again, which will make the registry
    # complain about duplicated metrics.
    #
    # The Python Prometheus client currently doesn't seem to have a way to
    # verify if adding a metric will cause errors or not, so the only way to
    # handle it seems to be with this try block.
    try:
        if label_names:
            METRIC = Summary(
                metric_name,
                metric_doc,
                labelnames=label_names,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )
        else:
            METRIC = Summary(
                metric_name,
                metric_doc,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )

        def instrumentation(info: Info) -> None:
            if info.response and hasattr(info.response, "headers"):
                content_length = info.response.headers.get("Content-Length", 0)
            else:
                content_length = 0

            if label_names:
                label_values = [
                    getattr(info, attribute_name)
                    for attribute_name in info_attribute_names
                ]

                METRIC.labels(*label_values).observe(int(content_length))
            else:
                METRIC.observe(int(content_length))

        return instrumentation
    except ValueError as e:
        if not _is_duplicated_time_series(e):
            raise e

    return None


def combined_size(
    metric_name: str = "http_combined_size_bytes",
    metric_doc: str = "Content bytes of requests and responses.",
    metric_namespace: str = "",
    metric_subsystem: str = "",
    should_include_handler: bool = True,
    should_include_method: bool = True,
    should_include_status: bool = True,
    registry: CollectorRegistry = REGISTRY,
    custom_labels: dict = {},
) -> Optional[Callable[[Info], None]]:
    """Record the combined content length of requests and responses.

    If content length is missing 0 will be assumed.

    Args:
        metric_name (str, optional): Name of the metric to be created. Must be
            unique. Defaults to "http_combined_size_bytes".

        metric_doc (str, optional): Documentation of the metric. Defaults to
            "Content bytes of requests and responses.".

        metric_namespace (str, optional): Namespace of all  metrics in this
            metric function. Defaults to "".

        metric_subsystem (str, optional): Subsystem of all  metrics in this
            metric function. Defaults to "".

        should_include_handler: Should the `handler` label be part of the
            metric? Defaults to `True`.

        should_include_method: Should the `method` label be part of the metric?
            Defaults to `True`.

        should_include_status: Should the `status` label be part of the metric?
            Defaults to `True`.

    Returns:
        Function that takes a single parameter `Info`.
    """

    label_names, info_attribute_names = _build_label_attribute_names(
        should_include_handler, should_include_method, should_include_status
    )
    for key in custom_labels:
        label_names.append(key)
        info_attribute_names.append(key)
    # Starlette will call app.build_middleware_stack() with every new middleware
    # added, which will call all this again, which will make the registry
    # complain about duplicated metrics.
    #
    # The Python Prometheus client currently doesn't seem to have a way to
    # verify if adding a metric will cause errors or not, so the only way to
    # handle it seems to be with this try block.
    try:
        if label_names:
            METRIC = Summary(
                metric_name,
                metric_doc,
                labelnames=label_names,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )
        else:
            METRIC = Summary(
                metric_name,
                metric_doc,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )

        def instrumentation(info: Info) -> None:
            request_cl = info.request.headers.get("Content-Length", 0)

            if info.response and hasattr(info.response, "headers"):
                response_cl = info.response.headers.get("Content-Length", 0)
            else:
                response_cl = 0

            content_length = int(request_cl) + int(response_cl)

            if label_names:
                label_values = [
                    getattr(info, attribute_name)
                    for attribute_name in info_attribute_names
                ]

                METRIC.labels(*label_values).observe(int(content_length))
            else:
                METRIC.observe(int(content_length))

        return instrumentation
    except ValueError as e:
        if not _is_duplicated_time_series(e):
            raise e

    return None


def requests(
    metric_name: str = "http_requests_total",
    metric_doc: str = "Total number of requests by method, status and handler.",
    metric_namespace: str = "",
    metric_subsystem: str = "",
    should_include_handler: bool = True,
    should_include_method: bool = True,
    should_include_status: bool = True,
    registry: CollectorRegistry = REGISTRY,
    custom_labels: dict = {},
) -> Optional[Callable[[Info], None]]:
    """Record the number of requests.

    Args:
        metric_name (str, optional): Name of the metric to be created. Must
            be unique. Defaults to "http_requests_total".

        metric_doc (str, optional): Documentation of the metric. Defaults to
            "Total number of requests by method, status and handler.".

        metric_namespace (str, optional): Namespace of all  metrics in this
            metric function. Defaults to "".

        metric_subsystem (str, optional): Subsystem of all  metrics in this
            metric function. Defaults to "".

        should_include_handler (bool, optional): Should the `handler` label
            be part of the metric? Defaults to `True`.

        should_include_method (bool, optional): Should the `method` label be
            part of the metric? Defaults to `True`.

        should_include_status (bool, optional): Should the `status` label be
            part of the metric? Defaults to `True`.

    Returns:
        Function that takes a single parameter `Info`.
    """

    label_names, info_attribute_names = _build_label_attribute_names(
        should_include_handler, should_include_method, should_include_status
    )
    for key in custom_labels:
        label_names.append(key)
        info_attribute_names.append(key)

    # Starlette will call app.build_middleware_stack() with every new middleware
    # added, which will call all this again, which will make the registry
    # complain about duplicated metrics.
    #
    # The Python Prometheus client currently doesn't seem to have a way to
    # verify if adding a metric will cause errors or not, so the only way to
    # handle it seems to be with this try block.
    try:
        if label_names:
            METRIC = Counter(
                metric_name,
                metric_doc,
                labelnames=label_names,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )
        else:
            METRIC = Counter(
                metric_name,
                metric_doc,
                namespace=metric_namespace,
                subsystem=metric_subsystem,
                registry=registry,
            )

        def instrumentation(info: Info) -> None:
            if label_names:
                label_values = [
                    getattr(info, attribute_name)
                    for attribute_name in info_attribute_names
                ]

                METRIC.labels(*label_values).inc()
            else:
                METRIC.inc()

        return instrumentation
    except ValueError as e:
        if not _is_duplicated_time_series(e):
            raise e

    return None


def _map_label_name_value(label_name: tuple) -> list[str]:
    attribute_names = []
    mapping = {
        "handler": "modified_handler",
        "status": "modified_status",
        "duration": "modified_duration",
    }
    for item in label_name:
        if item in mapping:
            attribute_names.append(mapping[item])
        else:
            attribute_names.append(item)
    return attribute_names


def default(
    metric_namespace: str = "",
    metric_subsystem: str = "",
    should_only_respect_2xx_for_highr: bool = False,
    should_exclude_streaming_duration: bool = False,
    latency_highr_buckets: Sequence[Union[float, str]] = (
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1,
        1.5,
        2,
        2.5,
        3,
        3.5,
        4,
        4.5,
        5,
        7.5,
        10,
        30,
        60,
    ),
    latency_lowr_buckets: Sequence[Union[float, str]] = (0.1, 0.5, 1),
    registry: CollectorRegistry = REGISTRY,
    custom_labels: dict = {},
) -> Optional[Callable[[Info], None]]:
    """Contains multiple metrics to cover multiple things.

    Combines several metrics into a single function. Also more efficient than
    multiple separate instrumentation functions that do more or less the same.

    You get the following:

    * `http_requests_total` (`handler`, `status`, `method`): Total number of
        requests by handler, status and method.
    * `http_request_size_bytes` (`handler`): Total number of incoming
        content length bytes by handler.
    * `http_response_size_bytes` (`handler`): Total number of outgoing
        content length bytes by handler.
    * `http_request_duration_highr_seconds` (no labels): High number of buckets
        leading to more accurate calculation of percentiles.
    * `http_request_duration_seconds` (`handler`, `method`):
        Kepp the bucket count very low. Only put in SLIs.

    Args:
        metric_namespace (str, optional): Namespace of all  metrics in this
            metric function. Defaults to "".

        metric_subsystem (str, optional): Subsystem of all  metrics in this
            metric function. Defaults to "".

        should_only_respect_2xx_for_highr (str, optional): Should the metric
            `http_request_duration_highr_seconds` only include latencies of
            requests / responses that have a status code starting with `2`?
            Defaults to `False`.

        should_exclude_streaming_duration: Should the streaming duration be
            excluded? Defaults to `False`.

        latency_highr_buckets (tuple[float], optional): Buckets tuple for high
            res histogram. Can be large because no labels are used. Defaults to
            (0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 2.5,
            3, 3.5, 4, 4.5, 5, 7.5, 10, 30, 60).

        latency_lowr_buckets (tuple[float], optional): Buckets tuple for low
            res histogram. Should be very small as all possible labels are
            included. Defaults to `(0.1, 0.5, 1)`.

    Returns:
        Function that takes a single parameter `Info`.
    """
    if latency_highr_buckets[-1] != float("inf"):
        latency_highr_buckets = [*latency_highr_buckets, float("inf")]

    if latency_lowr_buckets[-1] != float("inf"):
        latency_lowr_buckets = [*latency_lowr_buckets, float("inf")]

    # Starlette will call app.build_middleware_stack() with every new middleware
    # added, which will call all this again, which will make the registry
    # complain about duplicated metrics.
    #
    # The Python Prometheus client currently doesn't seem to have a way to
    # verify if adding a metric will cause errors or not, so the only way to
    # handle it seems to be with this try block.
    additional_label_names = tuple([key for key in custom_labels])
    try:
        total_label_names = (
            "method",
            "status",
            "handler",
        )
        TOTAL = Counter(
            name="http_requests_total",
            documentation="Total number of requests by method, status and handler.",
            labelnames=total_label_names + additional_label_names,
            namespace=metric_namespace,
            subsystem=metric_subsystem,
            registry=registry,
        )

        in_size_names = ("handler",)
        IN_SIZE = Summary(
            name="http_request_size_bytes",
            documentation=(
                "Content length of incoming requests by handler. "
                "Only value of header is respected. Otherwise ignored. "
                "No percentile calculated. "
            ),
            labelnames=in_size_names + additional_label_names,
            namespace=metric_namespace,
            subsystem=metric_subsystem,
            registry=registry,
        )

        out_size_names = ("handler",)
        OUT_SIZE = Summary(
            name="http_response_size_bytes",
            documentation=(
                "Content length of outgoing responses by handler. "
                "Only value of header is respected. Otherwise ignored. "
                "No percentile calculated. "
            ),
            labelnames=out_size_names + additional_label_names,
            namespace=metric_namespace,
            subsystem=metric_subsystem,
            registry=registry,
        )

        LATENCY_HIGHR = Histogram(
            name="http_request_duration_highr_seconds",
            documentation=(
                "Latency with many buckets but no API specific labels. "
                "Made for more accurate percentile calculations. "
            ),
            buckets=latency_highr_buckets,
            namespace=metric_namespace,
            subsystem=metric_subsystem,
            registry=registry,
        )

        latency_lower_names = (
            "method",
            "handler",
        )
        LATENCY_LOWR = Histogram(
            name="http_request_duration_seconds",
            documentation=(
                "Latency with only few buckets by handler. "
                "Made to be only used if aggregation by handler is important. "
            ),
            buckets=latency_lowr_buckets,
            labelnames=latency_lower_names + additional_label_names,
            namespace=metric_namespace,
            subsystem=metric_subsystem,
            registry=registry,
        )

        def instrumentation(info: Info) -> None:
            duration = info.modified_duration
            if should_exclude_streaming_duration:
                duration = info.modified_duration_without_streaming
            else:
                duration = info.modified_duration

            label_values = [
                getattr(info, attribute_name)
                for attribute_name in _map_label_name_value(total_label_names)
            ] + list(custom_labels.values())
            TOTAL.labels(*label_values).inc()

            label_values = [
                getattr(info, attribute_name)
                for attribute_name in _map_label_name_value(in_size_names)
            ] + list(custom_labels.values())
            IN_SIZE.labels(*label_values).observe(
                int(info.request.headers.get("Content-Length", 0))
            )

            label_values = [
                getattr(info, attribute_name)
                for attribute_name in _map_label_name_value(out_size_names)
            ] + list(custom_labels.values())
            if info.response and hasattr(info.response, "headers"):
                OUT_SIZE.labels(*label_values).observe(
                    int(info.response.headers.get("Content-Length", 0))
                )
            else:
                OUT_SIZE.labels(*label_values).observe(0)

            if not should_only_respect_2xx_for_highr or info.modified_status.startswith(
                "2"
            ):
                LATENCY_HIGHR.observe(duration)

            label_values = [
                getattr(info, attribute_name)
                for attribute_name in _map_label_name_value(latency_lower_names)
            ] + list(custom_labels.values())
            LATENCY_LOWR.labels(*label_values).observe(duration)

        return instrumentation

    except ValueError as e:
        if not _is_duplicated_time_series(e):
            raise e

    return None
