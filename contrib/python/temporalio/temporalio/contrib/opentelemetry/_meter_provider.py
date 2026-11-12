from collections.abc import Sequence
from typing import Any

from opentelemetry.context import Context

# _Gauge is OpenTelemetry's canonical exported name for the spec-experimental
# synchronous gauge, re-exported by opentelemetry.metrics since 1.23.
from opentelemetry.metrics import (
    CallbackT,
    Counter,
    Histogram,
    Meter,
    MeterProvider,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
    _Gauge,
)
from opentelemetry.util.types import Attributes

from temporalio import workflow


def _forward_context_kwarg(context: Context | None) -> dict[str, Any]:
    # Forward the context only when set: the parameter was added to the
    # synchronous instrument methods in opentelemetry 1.28 and older
    # instruments raise TypeError when it is passed.
    return {} if context is None else {"context": context}


def _skip_recording() -> bool:
    # in_workflow() must be evaluated first: is_replaying_history_events()
    # requires an active workflow context. The history-events predicate is
    # deliberate (not is_replaying()): queries and update validators are live,
    # at-most-once-per-request operations even when they execute while the
    # workflow is replaying, so their recordings must not be dropped.
    return workflow.in_workflow() and workflow.unsafe.is_replaying_history_events()


class _ReplaySafeCounter(Counter):
    def __init__(self, counter: Counter) -> None:
        self._counter = counter

    def __getattr__(self, name: str) -> object:
        return getattr(self._counter, name)

    def add(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        context: Context | None = None,
    ) -> None:
        if _skip_recording():
            # Skip recording metrics during workflow replay to avoid duplicate telemetry
            return
        self._counter.add(amount, attributes, **_forward_context_kwarg(context))


class _ReplaySafeUpDownCounter(UpDownCounter):
    def __init__(self, counter: UpDownCounter) -> None:
        self._counter = counter

    def __getattr__(self, name: str) -> object:
        return getattr(self._counter, name)

    def add(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        context: Context | None = None,
    ) -> None:
        if _skip_recording():
            # Skip recording metrics during workflow replay to avoid duplicate telemetry
            return
        self._counter.add(amount, attributes, **_forward_context_kwarg(context))


class _ReplaySafeHistogram(Histogram):
    def __init__(self, histogram: Histogram) -> None:
        self._histogram = histogram

    def __getattr__(self, name: str) -> object:
        return getattr(self._histogram, name)

    def record(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        context: Context | None = None,
    ) -> None:
        if _skip_recording():
            # Skip recording metrics during workflow replay to avoid duplicate telemetry
            return
        self._histogram.record(amount, attributes, **_forward_context_kwarg(context))


class _ReplaySafeGauge(_Gauge):
    def __init__(self, gauge: _Gauge) -> None:
        self._gauge = gauge

    def __getattr__(self, name: str) -> object:
        return getattr(self._gauge, name)

    def set(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        context: Context | None = None,
    ) -> None:
        if _skip_recording():
            # Skip recording metrics during workflow replay to avoid duplicate telemetry
            return
        self._gauge.set(amount, attributes, **_forward_context_kwarg(context))


class _ReplaySafeMeter(Meter):
    # Overrides every Meter method as of opentelemetry-api 1.44. OTel adds new
    # instrument kinds as non-abstract no-op defaults on the Meter ABC (e.g.
    # create_gauge in 1.23), which __getattr__ cannot intercept, so new Meter
    # methods must be audited and overridden here on opentelemetry upgrades.
    # tests/contrib/opentelemetry/test_wrapper_abc_drift.py fails when the
    # installed opentelemetry-api grows surface not covered here.
    def __init__(self, meter: Meter) -> None:
        super().__init__(meter.name, version=meter.version, schema_url=meter.schema_url)
        self._meter = meter

    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> Counter:
        return _ReplaySafeCounter(self._meter.create_counter(name, unit, description))

    def create_up_down_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> UpDownCounter:
        return _ReplaySafeUpDownCounter(
            self._meter.create_up_down_counter(name, unit, description)
        )

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
        *,
        explicit_bucket_boundaries_advisory: Sequence[float] | None = None,
    ) -> Histogram:
        # Forward the advisory only when set: the parameter was added in
        # opentelemetry 1.30 and unconditionally forwarding it raises TypeError
        # on older APIs still within the supported version range.
        kwargs: dict[str, Any] = {}
        if explicit_bucket_boundaries_advisory is not None:
            kwargs["explicit_bucket_boundaries_advisory"] = (
                explicit_bucket_boundaries_advisory
            )
        return _ReplaySafeHistogram(
            self._meter.create_histogram(name, unit, description, **kwargs)
        )

    def create_gauge(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> _Gauge:
        return _ReplaySafeGauge(self._meter.create_gauge(name, unit, description))

    # Observable instruments pass through unwrapped: their callbacks run on the
    # metric reader's collect thread, never inside workflow code.

    def create_observable_counter(
        self,
        name: str,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = "",
        description: str = "",
    ) -> ObservableCounter:
        return self._meter.create_observable_counter(name, callbacks, unit, description)

    def create_observable_gauge(
        self,
        name: str,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = "",
        description: str = "",
    ) -> ObservableGauge:
        return self._meter.create_observable_gauge(name, callbacks, unit, description)

    def create_observable_up_down_counter(
        self,
        name: str,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = "",
        description: str = "",
    ) -> ObservableUpDownCounter:
        return self._meter.create_observable_up_down_counter(
            name, callbacks, unit, description
        )


class ReplaySafeMeterProvider(MeterProvider):
    """A meter provider that is safe for use during workflow replay.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This meter provider wraps an OpenTelemetry MeterProvider and drops
    synchronous instrument recordings (counter ``add()``, up-down counter
    ``add()``, histogram ``record()``, and gauge ``set()``) made from workflow
    code while the workflow is replaying history events. Without this,
    libraries that record metrics from workflow code (e.g. ``google-adk``)
    re-record every measurement on each replay, inflating counts.

    Recordings are therefore first-execution-only, matching
    :py:meth:`temporalio.workflow.metric_meter`: a workflow task retry
    re-executes live and can record again. Queries and update validators are
    live, at-most-once-per-request operations even when they execute while the
    workflow is replaying, so their recordings are kept. Observable
    (asynchronous) instruments pass through untouched since their callbacks
    run on the metric reader's collect thread, never inside workflow code.
    Recordings outside workflows are unaffected.

    Install this as the process-global meter provider before any library
    (e.g. ``google-adk``) creates instruments::

        opentelemetry.metrics.set_meter_provider(
            ReplaySafeMeterProvider(my_meter_provider)
        )

    OpenTelemetry proxy meters late-bind, so calling ``set_meter_provider``
    after such libraries are imported still routes their instruments through
    this wrapper. However, ``set_meter_provider`` only takes effect once per
    process, so this wrapper must be the one and only global meter provider
    ever set.
    """

    def __init__(self, meter_provider: MeterProvider) -> None:
        """Initialize the replay-safe meter provider.

        Args:
            meter_provider: The underlying OpenTelemetry MeterProvider to wrap.
        """
        self._meter_provider = meter_provider

    def __getattr__(self, name: str) -> Any:
        """Delegate all other attributes (e.g. ``shutdown``, ``force_flush``)
        to the underlying meter provider.
        """
        return getattr(self._meter_provider, name)

    def get_meter(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
        attributes: Attributes | None = None,
    ) -> Meter:
        """Get a replay-safe meter from the underlying provider.

        Args:
            name: The name of the instrumenting module.
            version: The version string of the instrumenting library.
            schema_url: The schema URL for the meter.
            attributes: Additional attributes for the meter.

        Returns:
            A replay-safe meter instance.
        """
        inner = self._meter_provider.get_meter(name, version, schema_url, attributes)
        return _ReplaySafeMeter(inner)
