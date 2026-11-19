# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import os
import weakref
from atexit import register, unregister
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from logging import getLogger
from os import environ
from threading import Lock
from time import time_ns

# This kind of import is needed to avoid Sphinx errors.
import opentelemetry.sdk.metrics
from opentelemetry.metrics import Counter as APICounter
from opentelemetry.metrics import Histogram as APIHistogram
from opentelemetry.metrics import Meter as APIMeter
from opentelemetry.metrics import MeterProvider as APIMeterProvider
from opentelemetry.metrics import NoOpMeter
from opentelemetry.metrics import ObservableCounter as APIObservableCounter
from opentelemetry.metrics import ObservableGauge as APIObservableGauge
from opentelemetry.metrics import (
    ObservableUpDownCounter as APIObservableUpDownCounter,
)
from opentelemetry.metrics import UpDownCounter as APIUpDownCounter
from opentelemetry.metrics import _Gauge as APIGauge
from opentelemetry.sdk.environment_variables import (
    OTEL_METRICS_EXEMPLAR_FILTER,
    OTEL_SDK_DISABLED,
)
from opentelemetry.sdk.metrics._internal.exceptions import MetricsTimeoutError
from opentelemetry.sdk.metrics._internal.exemplar import (
    AlwaysOffExemplarFilter,
    AlwaysOnExemplarFilter,
    ExemplarFilter,
    TraceBasedExemplarFilter,
)
from opentelemetry.sdk.metrics._internal.instrument import (
    _Counter,
    _Gauge,
    _Histogram,
    _ObservableCounter,
    _ObservableGauge,
    _ObservableUpDownCounter,
    _UpDownCounter,
)
from opentelemetry.sdk.metrics._internal.measurement_consumer import (
    MeasurementConsumer,
    SynchronousMeasurementConsumer,
)
from opentelemetry.sdk.metrics._internal.sdk_configuration import (
    SdkConfiguration,
)
from opentelemetry.sdk.resources import (
    Resource,
    _get_process_dependent_resource,
)
from opentelemetry.sdk.util._configurator import RuleBasedConfigurator
from opentelemetry.sdk.util.instrumentation import (
    InstrumentationScope,
)
from opentelemetry.util._once import Once
from opentelemetry.util.types import (
    Attributes,
)

_logger = getLogger(__name__)


@dataclass
class _MeterConfig:
    is_enabled: bool = True

    @classmethod
    def default(cls) -> "_MeterConfig":
        return _MeterConfig()


class _ProxyMeterConfig:
    def __init__(self, config: _MeterConfig):
        self._config = config

    @property
    def is_enabled(self) -> bool:
        return self._config.is_enabled

    def update(self, config: _MeterConfig) -> None:
        self._config = config


class Meter(APIMeter):
    """See `opentelemetry.metrics.Meter`."""

    def __init__(
        self,
        instrumentation_scope: InstrumentationScope,
        measurement_consumer: MeasurementConsumer,
        *,
        _meter_config: _MeterConfig | None = None,
    ):
        super().__init__(
            name=instrumentation_scope.name,
            version=instrumentation_scope.version,
            schema_url=instrumentation_scope.schema_url,
        )
        self._instrumentation_scope = instrumentation_scope
        self._measurement_consumer = measurement_consumer
        self._instrument_id_instrument = {}
        self._instrument_registration_lock = Lock()
        self._meter_config = _ProxyMeterConfig(
            _meter_config or _MeterConfig.default()
        )

    def _is_enabled(self) -> bool:
        return self._meter_config.is_enabled

    def _set_meter_config(self, meter_config: _MeterConfig) -> None:
        self._meter_config.update(meter_config)

    def create_counter(self, name, unit="", description="") -> APICounter:
        with self._instrument_registration_lock:
            status = self._register_instrument(
                name, _Counter, unit, description
            )
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = (
                    _Counter(
                        name,
                        self._instrumentation_scope,
                        self._measurement_consumer,
                        unit,
                        description,
                        _meter_config=self._meter_config,
                    )
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APICounter.__name__,
                unit,
                description,
                status,
            )
        return instrument

    def create_up_down_counter(
        self, name, unit="", description=""
    ) -> APIUpDownCounter:
        with self._instrument_registration_lock:
            status = self._register_instrument(
                name, _UpDownCounter, unit, description
            )
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = (
                    _UpDownCounter(
                        name,
                        self._instrumentation_scope,
                        self._measurement_consumer,
                        unit,
                        description,
                        _meter_config=self._meter_config,
                    )
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APIUpDownCounter.__name__,
                unit,
                description,
                status,
            )
        return instrument

    def create_observable_counter(
        self,
        name,
        callbacks=None,
        unit="",
        description="",
    ) -> APIObservableCounter:
        with self._instrument_registration_lock:
            status = self._register_instrument(
                name, _ObservableCounter, unit, description
            )
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = (
                    _ObservableCounter(
                        name,
                        self._instrumentation_scope,
                        self._measurement_consumer,
                        callbacks,
                        unit,
                        description,
                        _meter_config=self._meter_config,
                    )
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if not status.already_registered:
            self._measurement_consumer.register_asynchronous_instrument(
                instrument
            )

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APIObservableCounter.__name__,
                unit,
                description,
                status,
            )
        return instrument

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
        *,
        explicit_bucket_boundaries_advisory: Sequence[float] | None = None,
    ) -> APIHistogram:
        if explicit_bucket_boundaries_advisory is not None:
            invalid_advisory = False
            if isinstance(explicit_bucket_boundaries_advisory, Sequence):
                try:
                    invalid_advisory = not (
                        all(
                            isinstance(e, (float, int))
                            for e in explicit_bucket_boundaries_advisory
                        )
                    )
                except (KeyError, TypeError):
                    invalid_advisory = True
            else:
                invalid_advisory = True

            if invalid_advisory:
                explicit_bucket_boundaries_advisory = None
                _logger.warning(
                    "explicit_bucket_boundaries_advisory must be a sequence of numbers"
                )

        with self._instrument_registration_lock:
            status = self._register_instrument(
                name,
                _Histogram,
                unit,
                description,
                explicit_bucket_boundaries_advisory,
            )
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = (
                    _Histogram(
                        name,
                        self._instrumentation_scope,
                        self._measurement_consumer,
                        unit,
                        description,
                        explicit_bucket_boundaries_advisory,
                        _meter_config=self._meter_config,
                    )
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APIHistogram.__name__,
                unit,
                description,
                status,
            )
        return instrument

    def create_gauge(self, name, unit="", description="") -> APIGauge:
        with self._instrument_registration_lock:
            status = self._register_instrument(name, _Gauge, unit, description)
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = _Gauge(
                    name,
                    self._instrumentation_scope,
                    self._measurement_consumer,
                    unit,
                    description,
                    _meter_config=self._meter_config,
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APIGauge.__name__,
                unit,
                description,
                status,
            )
        return instrument

    def create_observable_gauge(
        self, name, callbacks=None, unit="", description=""
    ) -> APIObservableGauge:
        with self._instrument_registration_lock:
            status = self._register_instrument(
                name, _ObservableGauge, unit, description
            )
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = (
                    _ObservableGauge(
                        name,
                        self._instrumentation_scope,
                        self._measurement_consumer,
                        callbacks,
                        unit,
                        description,
                        _meter_config=self._meter_config,
                    )
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if not status.already_registered:
            self._measurement_consumer.register_asynchronous_instrument(
                instrument
            )

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APIObservableGauge.__name__,
                unit,
                description,
                status,
            )
        return instrument

    def create_observable_up_down_counter(
        self, name, callbacks=None, unit="", description=""
    ) -> APIObservableUpDownCounter:
        with self._instrument_registration_lock:
            status = self._register_instrument(
                name, _ObservableUpDownCounter, unit, description
            )
            if not status.already_registered:
                self._instrument_id_instrument[status.instrument_id] = (
                    _ObservableUpDownCounter(
                        name,
                        self._instrumentation_scope,
                        self._measurement_consumer,
                        callbacks,
                        unit,
                        description,
                        _meter_config=self._meter_config,
                    )
                )
            instrument = self._instrument_id_instrument[status.instrument_id]

        if not status.already_registered:
            self._measurement_consumer.register_asynchronous_instrument(
                instrument
            )

        if status.conflict:
            # FIXME #2558 go through all views here and check if this
            # instrument registration conflict can be fixed. If it can be, do
            # not log the following warning.
            self._log_instrument_registration_conflict(
                name,
                APIObservableUpDownCounter.__name__,
                unit,
                description,
                status,
            )
        return instrument


def _get_exemplar_filter(exemplar_filter: str) -> ExemplarFilter:
    if exemplar_filter == "trace_based":
        return TraceBasedExemplarFilter()
    if exemplar_filter == "always_on":
        return AlwaysOnExemplarFilter()
    if exemplar_filter == "always_off":
        return AlwaysOffExemplarFilter()
    msg = f"Unknown exemplar filter '{exemplar_filter}'."
    raise ValueError(msg)


_MeterConfiguratorT = Callable[[InstrumentationScope], _MeterConfig]
_RuleBasedMeterConfigurator = RuleBasedConfigurator[_MeterConfig]


def _default_meter_configurator(
    _meter_scope: InstrumentationScope,
) -> _MeterConfig:
    return _MeterConfig.default()


def _disable_meter_configurator(
    _meter_scope: InstrumentationScope,
) -> _MeterConfig:
    return _MeterConfig(is_enabled=False)


class MeterProvider(APIMeterProvider):
    r"""See `opentelemetry.metrics.MeterProvider`.

    Args:
        metric_readers: Register metric readers to collect metrics from the SDK
            on demand. Each :class:`opentelemetry.sdk.metrics.export.MetricReader` is
            completely independent and will collect separate streams of
            metrics. For push-based export, use
            :class:`opentelemetry.sdk.metrics.export.PeriodicExportingMetricReader`.
        resource: The resource representing what the metrics emitted from the SDK pertain to.
        shutdown_on_exit: If true, registers an `atexit` handler to call
            `MeterProvider.shutdown`
        views: The views to configure the metric output the SDK

    .. code-block:: python
        :caption: Push-based export with PeriodicExportingMetricReader

        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import (
            ConsoleMetricExporter,
            PeriodicExportingMetricReader,
        )

        reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
        provider = MeterProvider(metric_readers=[reader])

    By default, instruments which do not match any :class:`opentelemetry.sdk.metrics.view.View` (or if no :class:`opentelemetry.sdk.metrics.view.View`\ s
    are provided) will report metrics with the default aggregation for the
    instrument's kind. To disable instruments by default, configure a match-all
    :class:`opentelemetry.sdk.metrics.view.View` with `DropAggregation` and then create :class:`opentelemetry.sdk.metrics.view.View`\ s to re-enable
    individual instruments:

    .. code-block:: python
        :caption: Disable default views

        MeterProvider(
            views=[
                View(instrument_name="*", aggregation=DropAggregation()),
                View(instrument_name="mycounter"),
            ],
            # ...
        )
    """

    _all_metric_readers_lock = Lock()
    _all_metric_readers = weakref.WeakSet()

    def __init__(
        self,
        metric_readers: Sequence[
            "opentelemetry.sdk.metrics.export.MetricReader"
        ] = (),
        resource: Resource | None = None,
        exemplar_filter: ExemplarFilter | None = None,
        shutdown_on_exit: bool = True,
        views: Sequence["opentelemetry.sdk.metrics.view.View"] = (),
        *,
        _meter_configurator: _MeterConfiguratorT | None = None,
    ):
        self._lock = Lock()
        self._meter_lock = Lock()
        self._atexit_handler = None
        if resource is None:
            resource = Resource.create({})
        self._sdk_config = SdkConfiguration(
            exemplar_filter=(
                exemplar_filter
                or _get_exemplar_filter(
                    environ.get(OTEL_METRICS_EXEMPLAR_FILTER, "trace_based")
                )
            ),
            resource=resource,
            views=views,
        )
        self._metric_readers = metric_readers
        self._measurement_consumer = SynchronousMeasurementConsumer(
            sdk_config=self._sdk_config,
            metric_readers=metric_readers,
        )
        disabled = environ.get(OTEL_SDK_DISABLED, "")
        self._disabled = disabled.lower().strip() == "true"

        if shutdown_on_exit:
            self._atexit_handler = register(self.shutdown)

        self._meters: dict[InstrumentationScope, Meter] = {}
        self._shutdown_once = Once()
        self._shutdown = False
        self._meter_configurator = (
            _meter_configurator or _default_meter_configurator
        )

        for metric_reader in self._metric_readers:
            with self._all_metric_readers_lock:
                if metric_reader in self._all_metric_readers:
                    # pylint: disable=broad-exception-raised
                    raise Exception(
                        f"MetricReader {metric_reader} has been registered "
                        "already in other MeterProvider instance"
                    )

                self._all_metric_readers.add(metric_reader)

            metric_reader._set_collect_callback(
                self._measurement_consumer.collect
            )
            metric_reader._set_meter_provider(self)

        if hasattr(os, "register_at_fork"):
            weak_at_fork = weakref.WeakMethod(self._handle_fork)

            def _after_in_child() -> None:
                if at_fork := weak_at_fork():
                    at_fork()

            os.register_at_fork(after_in_child=_after_in_child)

    def _handle_fork(self) -> None:
        self._lock = Lock()
        self._meter_lock = Lock()
        type(self)._all_metric_readers_lock = Lock()
        self._update_resource(_get_process_dependent_resource())

    def _set_meter_configurator(
        self, *, meter_configurator: _MeterConfiguratorT
    ):
        """Set a new MeterConfigurator for this MeterProvider.

        Setting a new MeterConfigurator will result in the configurator being called
        for each outstanding Meter and for any newly created meters thereafter.
        Therefore, it is important that the provided function returns quickly.
        """
        with self._meter_lock:
            self._meter_configurator = meter_configurator
            for instrumentation_scope, meter in self._meters.items():
                # pylint: disable-next=protected-access
                meter._set_meter_config(
                    self._apply_meter_configurator(instrumentation_scope)
                )

    def _update_resource(self, resource: Resource) -> None:
        with self._meter_lock:
            self._sdk_config.resource = self._sdk_config.resource.merge(
                resource
            )

    def _apply_meter_configurator(
        self, instrumentation_scope: InstrumentationScope
    ) -> _MeterConfig:
        try:
            return self._meter_configurator(instrumentation_scope)
        # pylint: disable-next=broad-exception-caught
        except Exception:
            _logger.exception(
                "meter configurator failed for scope '%s', using default config",
                instrumentation_scope.name,
            )
            return _MeterConfig.default()

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        deadline_ns = time_ns() + timeout_millis * 10**6

        metric_reader_error = {}

        for metric_reader in self._metric_readers:
            current_ts = time_ns()
            try:
                if current_ts >= deadline_ns:
                    raise MetricsTimeoutError(
                        "Timed out while flushing metric readers"
                    )
                metric_reader.force_flush(
                    timeout_millis=(deadline_ns - current_ts) / 10**6
                )

            # pylint: disable=broad-exception-caught
            except Exception as error:
                metric_reader_error[metric_reader] = error

        if metric_reader_error:
            metric_reader_error_string = "\n".join(
                [
                    f"{metric_reader.__class__.__name__}: {repr(error)}"
                    for metric_reader, error in metric_reader_error.items()
                ]
            )

            # pylint: disable=broad-exception-raised
            raise Exception(
                "MeterProvider.force_flush failed because the following "
                "metric readers failed during collect:\n"
                f"{metric_reader_error_string}"
            )
        return True

    def shutdown(self, timeout_millis: float = 30_000):
        deadline_ns = time_ns() + timeout_millis * 10**6

        def _shutdown():
            self._shutdown = True

        did_shutdown = self._shutdown_once.do_once(_shutdown)

        if not did_shutdown:
            _logger.warning("shutdown can only be called once")
            return

        metric_reader_error = {}

        for metric_reader in self._metric_readers:
            current_ts = time_ns()
            try:
                if current_ts >= deadline_ns:
                    # pylint: disable=broad-exception-raised
                    raise Exception(
                        "Didn't get to execute, deadline already exceeded"
                    )
                metric_reader.shutdown(
                    timeout_millis=(deadline_ns - current_ts) / 10**6
                )

            # pylint: disable=broad-exception-caught
            except Exception as error:
                metric_reader_error[metric_reader] = error

        if self._atexit_handler is not None:
            unregister(self._atexit_handler)
            self._atexit_handler = None

        if metric_reader_error:
            metric_reader_error_string = "\n".join(
                [
                    f"{metric_reader.__class__.__name__}: {repr(error)}"
                    for metric_reader, error in metric_reader_error.items()
                ]
            )

            # pylint: disable=broad-exception-raised
            raise Exception(
                "MeterProvider.shutdown failed because the following "
                "metric readers failed during shutdown:\n"
                f"{metric_reader_error_string}"
            )

    def get_meter(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
        attributes: Attributes | None = None,
    ) -> APIMeter:
        if self._disabled:
            return NoOpMeter(name, version=version, schema_url=schema_url)

        if self._shutdown:
            _logger.warning(
                "A shutdown `MeterProvider` can not provide a `Meter`"
            )
            return NoOpMeter(name, version=version, schema_url=schema_url)

        if not name:
            _logger.warning("Meter name cannot be None or empty.")
            return NoOpMeter(name, version=version, schema_url=schema_url)

        instrumentation_scope = InstrumentationScope(
            name, version, schema_url, attributes
        )
        with self._meter_lock:
            if not self._meters.get(instrumentation_scope):
                # FIXME #2558 pass SDKConfig object to meter so that the meter
                # has access to views.
                self._meters[instrumentation_scope] = Meter(
                    instrumentation_scope,
                    self._measurement_consumer,
                    _meter_config=self._apply_meter_configurator(
                        instrumentation_scope
                    ),
                )
            return self._meters[instrumentation_scope]

    def add_metric_reader(
        self, metric_reader: "opentelemetry.sdk.metrics.export.MetricReader"
    ) -> None:
        with self._all_metric_readers_lock:
            if metric_reader in self._all_metric_readers:
                _logger.warning(
                    "MetricReader '%s' has been registered already!",
                    metric_reader,
                )
                return
            self._measurement_consumer.add_metric_reader(metric_reader)
            # pylint: disable-next=protected-access
            metric_reader._set_collect_callback(
                self._measurement_consumer.collect
            )
            self._all_metric_readers.add(metric_reader)

    def remove_metric_reader(
        self,
        metric_reader: "opentelemetry.sdk.metrics.export.MetricReader",
    ) -> None:
        with self._all_metric_readers_lock:
            if metric_reader not in self._all_metric_readers:
                _logger.warning(
                    "MetricReader '%s' has not been registered!", metric_reader
                )
                return
            self._measurement_consumer.remove_metric_reader(metric_reader)
            # pylint: disable-next=protected-access
            metric_reader._set_collect_callback(None)
            metric_reader.shutdown()
            self._all_metric_readers.remove(metric_reader)
