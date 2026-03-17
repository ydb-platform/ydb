from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Sequence
from threading import Lock
from typing import Any, Generic, TypeVar
from weakref import WeakSet

from opentelemetry.metrics import (
    CallbackT,
    Counter,
    Histogram,
    Instrument,
    Meter,
    MeterProvider,
    NoOpMeterProvider,
    ObservableCounter,
    ObservableGauge,
    ObservableUpDownCounter,
    UpDownCounter,
    _Gauge as Gauge,
)
from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
from opentelemetry.trace import get_current_span
from opentelemetry.util.types import Attributes

from .tracer import _LogfireWrappedSpan  # type: ignore
from .utils import handle_internal_errors


# The following proxy classes are adapted from OTEL's SDK
@dataclasses.dataclass
class ProxyMeterProvider(MeterProvider):
    provider: MeterProvider
    meters: WeakSet[_ProxyMeter] = dataclasses.field(default_factory=WeakSet)  # type: ignore[reportUnknownVariableType]
    lock: Lock = dataclasses.field(default_factory=Lock)
    suppressed_scopes: set[str] = dataclasses.field(default_factory=set)  # type: ignore[reportUnknownVariableType]

    def get_meter(
        self,
        name: str,
        version: str | None = None,
        schema_url: str | None = None,
        attributes: Attributes | None = None,
    ) -> Meter:
        with self.lock:
            if name in self.suppressed_scopes:
                provider = NoOpMeterProvider()
            else:
                provider = self.provider
            inner_meter = provider.get_meter(name, version, schema_url, *[attributes] if attributes is not None else [])
            meter = _ProxyMeter(inner_meter, name, version, schema_url)
            self.meters.add(meter)
            return meter

    def suppress_scopes(self, *scopes: str) -> None:
        with self.lock:
            self.suppressed_scopes.update(scopes)
            for meter in self.meters:
                if meter.name in scopes:
                    meter.set_meter(NoOpMeterProvider())

    def set_meter_provider(self, meter_provider: MeterProvider) -> None:
        with self.lock:
            self.provider = meter_provider
            for meter in self.meters:
                meter.set_meter(NoOpMeterProvider() if meter.name in self.suppressed_scopes else meter_provider)

    def shutdown(self, timeout_millis: float = 30_000) -> None:
        with self.lock:
            if isinstance(self.provider, SDKMeterProvider):
                self.provider.shutdown(timeout_millis)

    def force_flush(self, timeout_millis: float = 30_000) -> None:
        with self.lock:
            if isinstance(self.provider, SDKMeterProvider):  # pragma: no branch
                self.provider.force_flush(timeout_millis)


class _ProxyMeter(Meter):
    def __init__(
        self,
        meter: Meter,
        name: str,
        version: str | None,
        schema_url: str | None,
    ) -> None:
        super().__init__(name, version=version, schema_url=schema_url)
        self._lock = Lock()
        self._meter = meter
        self._instruments: WeakSet[_ProxyInstrument[Any]] = WeakSet()

    def set_meter(self, meter_provider: MeterProvider) -> None:
        """Called when a real meter provider is set on the creating _ProxyMeterProvider.

        Creates a real backing meter for this instance and notifies all created
        instruments so they can create real backing instruments.
        """
        real_meter = meter_provider.get_meter(self._name, self._version, self._schema_url)

        with self._lock:
            self._meter = real_meter
            # notify all proxy instruments of the new meter so they can create
            # real instruments to back themselves
            for instrument in self._instruments:
                instrument.on_meter_set(real_meter)

    def _add_proxy_instrument(self, instrument_type: type[_ProxyInstrument[InstrumentT]], **kwargs: Any) -> InstrumentT:
        with self._lock:
            proxy = instrument_type(self._meter, **kwargs)
            self._instruments.add(proxy)
            return proxy  # type: ignore

    def create_counter(
        self,
        name: str,
        unit: str = '',
        description: str = '',
    ) -> Counter:
        return self._add_proxy_instrument(_ProxyCounter, name=name, unit=unit, description=description)

    def create_up_down_counter(
        self,
        name: str,
        unit: str = '',
        description: str = '',
    ) -> UpDownCounter:
        return self._add_proxy_instrument(_ProxyUpDownCounter, name=name, unit=unit, description=description)

    def create_observable_counter(
        self,
        name: str,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = '',
        description: str = '',
    ) -> ObservableCounter:
        return self._add_proxy_instrument(
            _ProxyObservableCounter, name=name, unit=unit, description=description, callbacks=callbacks
        )

    def create_histogram(
        self,
        name: str,
        unit: str = '',
        description: str = '',
        **kwargs: Any,
    ) -> Histogram:
        return self._add_proxy_instrument(_ProxyHistogram, name=name, unit=unit, description=description, **kwargs)

    def create_gauge(
        self,
        name: str,
        unit: str = '',
        description: str = '',
    ) -> Gauge:
        return self._add_proxy_instrument(_ProxyGauge, name=name, unit=unit, description=description)

    def create_observable_gauge(
        self,
        name: str,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = '',
        description: str = '',
    ) -> ObservableGauge:
        return self._add_proxy_instrument(
            _ProxyObservableGauge, name=name, unit=unit, description=description, callbacks=callbacks
        )

    def create_observable_up_down_counter(
        self,
        name: str,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = '',
        description: str = '',
    ) -> ObservableUpDownCounter:
        return self._add_proxy_instrument(
            _ProxyObservableUpDownCounter, name=name, unit=unit, description=description, callbacks=callbacks
        )


InstrumentT = TypeVar('InstrumentT', bound=Instrument)


class _ProxyInstrument(ABC, Generic[InstrumentT]):
    def __init__(self, meter: Meter, **kwargs: Any) -> None:
        self._kwargs = kwargs
        self._instrument = self._create_real_instrument(meter)

    def on_meter_set(self, meter: Meter) -> None:
        """Called when a real meter is set on the creating _ProxyMeter."""
        # We don't need any locking on proxy instruments because it's OK if some
        # measurements get dropped while a real backing instrument is being
        # created.
        self._instrument = self._create_real_instrument(meter)

    @abstractmethod
    def _create_real_instrument(self, meter: Meter) -> InstrumentT:
        """Create an instance of the real instrument. Implement this."""

    @handle_internal_errors
    def _increment_span_metric(self, amount: float, attributes: Attributes | None = None):
        span = get_current_span()
        if isinstance(span, _LogfireWrappedSpan):
            span.increment_metric(self._kwargs['name'], attributes or {}, amount)


class _ProxyCounter(_ProxyInstrument[Counter], Counter):
    def add(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        # Starting with opentelemetry-sdk 1.28.0, these methods accept an additional optional `context` argument.
        # This is passed to the underlying instrument using `*args, **kwargs` for compatibility with older versions.
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._increment_span_metric(amount, attributes)
        self._instrument.add(amount, attributes, *args, **kwargs)

    def _create_real_instrument(self, meter: Meter) -> Counter:
        return meter.create_counter(**self._kwargs)


class _ProxyHistogram(_ProxyInstrument[Histogram], Histogram):
    def record(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._increment_span_metric(amount, attributes)
        self._instrument.record(amount, attributes, *args, **kwargs)

    def _create_real_instrument(self, meter: Meter) -> Histogram:
        return meter.create_histogram(**self._kwargs)


class _ProxyObservableCounter(_ProxyInstrument[ObservableCounter], ObservableCounter):
    def _create_real_instrument(self, meter: Meter) -> ObservableCounter:
        return meter.create_observable_counter(**self._kwargs)


class _ProxyObservableGauge(_ProxyInstrument[ObservableGauge], ObservableGauge):
    def _create_real_instrument(self, meter: Meter) -> ObservableGauge:
        return meter.create_observable_gauge(**self._kwargs)


class _ProxyObservableUpDownCounter(_ProxyInstrument[ObservableUpDownCounter], ObservableUpDownCounter):
    def _create_real_instrument(self, meter: Meter) -> ObservableUpDownCounter:
        return meter.create_observable_up_down_counter(**self._kwargs)


class _ProxyUpDownCounter(_ProxyInstrument[UpDownCounter], UpDownCounter):
    def add(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._instrument.add(amount, attributes, *args, **kwargs)

    def _create_real_instrument(self, meter: Meter) -> UpDownCounter:
        return meter.create_up_down_counter(**self._kwargs)


class _ProxyGauge(_ProxyInstrument[Gauge], Gauge):
    def set(
        self,
        amount: int | float,
        attributes: Attributes | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._instrument.set(amount, attributes, *args, **kwargs)

    def _create_real_instrument(self, meter: Meter):
        return meter.create_gauge(**self._kwargs)
