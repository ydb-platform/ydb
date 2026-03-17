import asyncio
import gzip
import importlib.util
import os
import re
import warnings
from enum import Enum
from typing import Any, Awaitable, Callable, List, Optional, Sequence, Union, cast

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    generate_latest,
    multiprocess,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response

from prometheus_fastapi_instrumentator import metrics
from prometheus_fastapi_instrumentator.middleware import (
    PrometheusInstrumentatorMiddleware,
)


class PrometheusFastApiInstrumentator:
    def __init__(
        self,
        should_group_status_codes: bool = True,
        should_ignore_untemplated: bool = False,
        should_group_untemplated: bool = True,
        should_round_latency_decimals: bool = False,
        should_respect_env_var: bool = False,
        should_instrument_requests_inprogress: bool = False,
        should_exclude_streaming_duration: bool = False,
        excluded_handlers: List[str] = [],
        body_handlers: List[str] = [],
        round_latency_decimals: int = 4,
        env_var_name: str = "ENABLE_METRICS",
        inprogress_name: str = "http_requests_inprogress",
        inprogress_labels: bool = False,
        registry: Union[CollectorRegistry, None] = None,
    ) -> None:
        """Create a Prometheus FastAPI (and Starlette) Instrumentator.

        Args:
            should_group_status_codes (bool): Should status codes be grouped into
                `2xx`, `3xx` and so on? Defaults to `True`.

            should_ignore_untemplated (bool): Should requests without a matching
                template be ignored? Defaults to `False`. This means that by
                default a request like `curl -X GET localhost:80/doesnotexist`
                will be ignored.

            should_group_untemplated (bool): Should requests without a matching
                template be grouped to handler `none`? Defaults to `True`.

            should_round_latency_decimals: Should recorded latencies be
                rounded to a certain number of decimals?

            should_respect_env_var (bool): Should the instrumentator only work - for
                example the methods `instrument()` and `expose()` - if a
                certain environment variable is set to `true`? Usecase: A base
                FastAPI app that is used by multiple distinct apps. The apps
                only have to set the variable to be instrumented. Defaults to
                `False`.

            should_instrument_requests_inprogress (bool): Enables a gauge that shows
                the inprogress requests. See also the related args starting
                with `inprogress`. Defaults to `False`.

            should_exclude_streaming_duration: Should the streaming duration be
                excluded? Only relevant if default metrics are used. Defaults
                to `False`.

            excluded_handlers (List[str]): List of strings that will be compiled
                to regex patterns. All matches will be skipped and not
                instrumented. Defaults to `[]`.

            body_handlers (List[str]): List of strings that will be compiled
                to regex patterns to match handlers for the middleware to
                pass through response bodies to instrumentations. So only
                relevant for instrumentations that access `info.response.body`.
                Note that this has a noticeable negative impact on performance
                with responses larger than a few MBs. Defaults to `[]`.

            round_latency_decimals (int): Number of decimals latencies should be
                rounded to. Ignored unless `should_round_latency_decimals` is
                `True`. Defaults to `4`.

            env_var_name (str): Any valid os environment variable name that will
                be checked for existence before instrumentation. Ignored unless
                `should_respect_env_var` is `True`. Defaults to `"ENABLE_METRICS"`.

            inprogress_name (str): Name of the gauge. Defaults to
                `http_requests_inprogress`. Ignored unless
                `should_instrument_requests_inprogress` is `True`.

            inprogress_labels (bool): Should labels `method` and `handler` be
                part of the inprogress label? Ignored unless
                `should_instrument_requests_inprogress` is `True`. Defaults to `False`.

            registry (CollectorRegistry): A custom Prometheus registry to use. If not
                provided, the default `REGISTRY` will be used. This can be useful if
                you need to run multiple apps at the same time, with their own
                registries, for example during testing.

        Raises:
            ValueError: If `PROMETHEUS_MULTIPROC_DIR` env var is found but
                doesn't point to a valid directory.
        """

        self.should_group_status_codes = should_group_status_codes
        self.should_ignore_untemplated = should_ignore_untemplated
        self.should_group_untemplated = should_group_untemplated
        self.should_round_latency_decimals = should_round_latency_decimals
        self.should_respect_env_var = should_respect_env_var
        self.should_instrument_requests_inprogress = should_instrument_requests_inprogress
        self.should_exclude_streaming_duration = should_exclude_streaming_duration

        self.round_latency_decimals = round_latency_decimals
        self.env_var_name = env_var_name
        self.inprogress_name = inprogress_name
        self.inprogress_labels = inprogress_labels

        self.excluded_handlers = [re.compile(path) for path in excluded_handlers]
        self.body_handlers = [re.compile(path) for path in body_handlers]

        self.instrumentations: List[Callable[[metrics.Info], None]] = []
        self.async_instrumentations: List[Callable[[metrics.Info], Awaitable[None]]] = []

        if (
            "prometheus_multiproc_dir" in os.environ
            and "PROMETHEUS_MULTIPROC_DIR" not in os.environ
        ):
            os.environ["PROMETHEUS_MULTIPROC_DIR"] = os.environ[
                "prometheus_multiproc_dir"
            ]
            warnings.warn(
                "prometheus_multiproc_dir variable has been deprecated in favor of the upper case naming PROMETHEUS_MULTIPROC_DIR",
                DeprecationWarning,
            )

        if registry:
            self.registry = registry
        else:
            self.registry = REGISTRY

        if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
            pmd = os.environ["PROMETHEUS_MULTIPROC_DIR"]
            if not os.path.isdir(pmd):
                raise ValueError(
                    f"Env var PROMETHEUS_MULTIPROC_DIR='{pmd}' not a directory."
                )

    def instrument(
        self,
        app: Starlette,
        metric_namespace: str = "",
        metric_subsystem: str = "",
        should_only_respect_2xx_for_highr: bool = False,
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
    ) -> "PrometheusFastApiInstrumentator":
        """Performs the instrumentation by adding middleware.

        The middleware iterates through all `instrumentations` and executes them.

        Args:
            app: Starlette app instance. Note that every FastAPI app is a
                Starlette app.

        Raises:
            e: Only raised if app itself throws an exception.

        Returns:
            self: Instrumentator. Builder Pattern.
        """

        if self.should_respect_env_var and not self._should_instrumentate():
            return self

        app.add_middleware(
            PrometheusInstrumentatorMiddleware,
            should_group_status_codes=self.should_group_status_codes,
            should_ignore_untemplated=self.should_ignore_untemplated,
            should_group_untemplated=self.should_group_untemplated,
            should_round_latency_decimals=self.should_round_latency_decimals,
            should_respect_env_var=self.should_respect_env_var,
            should_instrument_requests_inprogress=self.should_instrument_requests_inprogress,
            should_exclude_streaming_duration=self.should_exclude_streaming_duration,
            round_latency_decimals=self.round_latency_decimals,
            env_var_name=self.env_var_name,
            inprogress_name=self.inprogress_name,
            inprogress_labels=self.inprogress_labels,
            instrumentations=self.instrumentations,
            async_instrumentations=self.async_instrumentations,
            excluded_handlers=self.excluded_handlers,
            body_handlers=self.body_handlers,
            metric_namespace=metric_namespace,
            metric_subsystem=metric_subsystem,
            should_only_respect_2xx_for_highr=should_only_respect_2xx_for_highr,
            latency_highr_buckets=latency_highr_buckets,
            latency_lowr_buckets=latency_lowr_buckets,
            registry=self.registry,
        )
        return self

    def expose(
        self,
        app: Starlette,
        should_gzip: bool = False,
        endpoint: str = "/metrics",
        include_in_schema: bool = True,
        tags: Optional[List[Union[str, Enum]]] = None,
        **kwargs: Any,
    ) -> "PrometheusFastApiInstrumentator":
        """Exposes endpoint for metrics.

        Args:
            app: App instance. Endpoint will be added to this app. This can be
            a Starlette app or a FastAPI app. If it is a Starlette app, `tags`
            `kwargs` will be ignored.

            should_gzip: Should the endpoint return compressed data? It will
                also check for `gzip` in the `Accept-Encoding` header.
                Compression consumes more CPU cycles. In most cases it's best
                to just leave this option off since network bandwidth is usually
                cheaper than CPU cycles. Defaults to `False`.

            endpoint: Endpoint on which metrics should be exposed.

            include_in_schema: Should the endpoint show up in the documentation?

            tags (List[str], optional): If you manage your routes with tags.
                Defaults to None. Only passed to FastAPI app.

            kwargs: Will be passed to app. Only passed to FastAPI app.

        Returns:
            self: Instrumentator. Builder Pattern.
        """

        if self.should_respect_env_var and not self._should_instrumentate():
            return self

        def metrics(request: Request) -> Response:
            """Endpoint that serves Prometheus metrics."""

            ephemeral_registry = self.registry
            if "PROMETHEUS_MULTIPROC_DIR" in os.environ:
                ephemeral_registry = CollectorRegistry()
                multiprocess.MultiProcessCollector(ephemeral_registry)

            if should_gzip and "gzip" in request.headers.get("Accept-Encoding", ""):
                resp = Response(
                    content=gzip.compress(generate_latest(ephemeral_registry))
                )
                resp.headers["Content-Type"] = CONTENT_TYPE_LATEST
                resp.headers["Content-Encoding"] = "gzip"
            else:
                resp = Response(content=generate_latest(ephemeral_registry))
                resp.headers["Content-Type"] = CONTENT_TYPE_LATEST

            return resp

        route_configured = False
        if importlib.util.find_spec("fastapi"):
            from fastapi import FastAPI

            if isinstance(app, FastAPI):
                fastapi_app: FastAPI = app
                fastapi_app.get(
                    endpoint, include_in_schema=include_in_schema, tags=tags, **kwargs
                )(metrics)
                route_configured = True
        if not route_configured:
            app.add_route(
                path=endpoint, route=metrics, include_in_schema=include_in_schema
            )

        return self

    def add(
        self,
        *instrumentation_function: Optional[
            Callable[[metrics.Info], Union[None, Awaitable[None]]]
        ],
    ) -> "PrometheusFastApiInstrumentator":
        """Adds function to list of instrumentations.

        Args:
            instrumentation_function: Function
                that will be executed during every request handler call (if
                not excluded). See above for detailed information on the
                interface of the function.

        Returns:
            self: Instrumentator. Builder Pattern.
        """

        for func in instrumentation_function:
            if func:
                if asyncio.iscoroutinefunction(func):
                    self.async_instrumentations.append(
                        cast(
                            Callable[[metrics.Info], Awaitable[None]],
                            func,
                        )
                    )
                else:
                    self.instrumentations.append(
                        cast(Callable[[metrics.Info], None], func)
                    )

        return self

    def _should_instrumentate(self) -> bool:
        """Checks if instrumentation should be performed based on env var."""

        return os.getenv(self.env_var_name, "False").lower() in ["true", "1"]
