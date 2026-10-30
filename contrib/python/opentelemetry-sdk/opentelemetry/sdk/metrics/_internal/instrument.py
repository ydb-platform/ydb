# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=too-many-ancestors, unused-import
from __future__ import annotations

import math
from collections.abc import Generator, Iterable, Sequence
from logging import getLogger
from time import time_ns
from typing import (
    TYPE_CHECKING,
    Protocol,
    cast,
    runtime_checkable,
)

# This kind of import is needed to avoid Sphinx errors.
from opentelemetry.context import Context, get_current
from opentelemetry.metrics import Asynchronous, CallbackT, Synchronous
from opentelemetry.metrics import Counter as APICounter
from opentelemetry.metrics import Histogram as APIHistogram
from opentelemetry.metrics import ObservableCounter as APIObservableCounter
from opentelemetry.metrics import ObservableGauge as APIObservableGauge
from opentelemetry.metrics import (
    ObservableUpDownCounter as APIObservableUpDownCounter,
)
from opentelemetry.metrics import UpDownCounter as APIUpDownCounter
from opentelemetry.metrics import _Gauge as APIGauge
from opentelemetry.metrics._internal.instrument import (
    CallbackOptions,
    _MetricsHistogramAdvisory,
)
from opentelemetry.sdk.metrics._internal.measurement import Measurement

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics._internal import (
        _ProxyMeterConfig,
    )
    from opentelemetry.sdk.metrics._internal.measurement_consumer import (
        MeasurementConsumer,
    )
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope


_logger = getLogger(__name__)


_ERROR_MESSAGE = (
    "Expected ASCII string of maximum length 63 characters but got {}"
)


@runtime_checkable
class _Instrument(Protocol):
    name: str
    unit: str
    description: str
    instrumentation_scope: InstrumentationScope


class _Synchronous(_Instrument, Synchronous):
    def __init__(
        self,
        name: str,
        instrumentation_scope: InstrumentationScope,
        measurement_consumer: MeasurementConsumer,
        unit: str = "",
        description: str = "",
        *,
        _meter_config: _ProxyMeterConfig | None = None,
    ):
        # pylint: disable=no-member
        result = self._check_name_unit_description(name, unit, description)

        if result["name"] is None:
            # pylint: disable=broad-exception-raised
            raise Exception(_ERROR_MESSAGE.format(name))

        if result["unit"] is None:
            # pylint: disable=broad-exception-raised
            raise Exception(_ERROR_MESSAGE.format(unit))

        name = result["name"]
        unit = result["unit"]
        description = cast(str, result["description"])

        self.name = name.lower()
        self.unit = unit
        self.description = description
        self.instrumentation_scope = instrumentation_scope
        self._measurement_consumer = measurement_consumer
        self._meter_config = _meter_config
        super().__init__(name, unit=unit, description=description)

    def _is_enabled(self) -> bool:
        return self._meter_config is None or self._meter_config.is_enabled


class _Asynchronous(_Instrument, Asynchronous):
    def __init__(
        self,
        name: str,
        instrumentation_scope: InstrumentationScope,
        measurement_consumer: MeasurementConsumer,
        callbacks: Sequence[CallbackT] | None = None,
        unit: str = "",
        description: str = "",
        *,
        _meter_config: _ProxyMeterConfig | None = None,
    ):
        # pylint: disable=no-member
        result = self._check_name_unit_description(name, unit, description)

        if result["name"] is None:
            # pylint: disable=broad-exception-raised
            raise Exception(_ERROR_MESSAGE.format(name))

        if result["unit"] is None:
            # pylint: disable=broad-exception-raised
            raise Exception(_ERROR_MESSAGE.format(unit))

        name = result["name"]
        unit = result["unit"]
        description = cast(str, result["description"])

        self.name = name.lower()
        self.unit = unit
        self.description = description
        self.instrumentation_scope = instrumentation_scope
        self._measurement_consumer = measurement_consumer
        self._meter_config = _meter_config
        super().__init__(name, callbacks, unit=unit, description=description)

        self._callbacks: list[CallbackT] = []

        if callbacks is not None:
            for callback in callbacks:
                if isinstance(callback, Generator):
                    # advance generator to it's first yield
                    next(callback)

                    def inner(
                        options: CallbackOptions,
                        callback=callback,
                    ) -> Iterable[Measurement]:
                        try:
                            return callback.send(options)
                        except StopIteration:
                            return []

                    self._callbacks.append(inner)
                else:
                    self._callbacks.append(callback)

    def _is_enabled(self) -> bool:
        return self._meter_config is None or self._meter_config.is_enabled

    def callback(
        self, callback_options: CallbackOptions
    ) -> Iterable[Measurement]:
        if not self._is_enabled():
            return
        for callback in self._callbacks:
            try:
                for api_measurement in callback(callback_options):
                    if not math.isfinite(api_measurement.value):
                        _logger.warning(
                            "Callback returned a non-finite value %s for instrument %s, ignoring measurement.",
                            api_measurement.value,
                            self.name,
                        )
                        continue
                    yield Measurement(
                        api_measurement.value,
                        time_unix_nano=time_ns(),
                        instrument=self,
                        context=api_measurement.context or get_current(),
                        attributes=api_measurement.attributes,
                    )
            except Exception:  # pylint: disable=broad-exception-caught
                _logger.exception(
                    "Callback failed for instrument %s.", self.name
                )


class Counter(_Synchronous, APICounter):
    def __new__(cls, *args, **kwargs):
        if cls is Counter:
            raise TypeError("Counter must be instantiated via a meter.")
        return super().__new__(cls)

    def add(
        self,
        amount: int | float,
        attributes: dict[str, str] | None = None,
        context: Context | None = None,
    ):
        if not self._is_enabled():
            super().add(amount, attributes=attributes, context=context)
            return

        if not math.isfinite(amount):
            _logger.warning(
                "Add amount %s is not finite on Counter %s, ignoring measurement.",
                amount,
                self.name,
            )
            return
        if amount < 0:
            _logger.warning(
                "Add amount must be non-negative on Counter %s.", self.name
            )
            return
        time_unix_nano = time_ns()
        self._measurement_consumer.consume_measurement(
            Measurement(
                amount,
                time_unix_nano,
                self,
                context or get_current(),
                attributes,
            )
        )


class UpDownCounter(_Synchronous, APIUpDownCounter):
    def __new__(cls, *args, **kwargs):
        if cls is UpDownCounter:
            raise TypeError("UpDownCounter must be instantiated via a meter.")
        return super().__new__(cls)

    def add(
        self,
        amount: int | float,
        attributes: dict[str, str] | None = None,
        context: Context | None = None,
    ):
        if not self._is_enabled():
            super().add(amount, attributes=attributes, context=context)
            return

        if not math.isfinite(amount):
            _logger.warning(
                "Add amount %s is not finite on UpDownCounter %s, ignoring measurement.",
                amount,
                self.name,
            )
            return
        time_unix_nano = time_ns()
        self._measurement_consumer.consume_measurement(
            Measurement(
                amount,
                time_unix_nano,
                self,
                context or get_current(),
                attributes,
            )
        )


class ObservableCounter(_Asynchronous, APIObservableCounter):
    def __new__(cls, *args, **kwargs):
        if cls is ObservableCounter:
            raise TypeError(
                "ObservableCounter must be instantiated via a meter."
            )
        return super().__new__(cls)


class ObservableUpDownCounter(_Asynchronous, APIObservableUpDownCounter):
    def __new__(cls, *args, **kwargs):
        if cls is ObservableUpDownCounter:
            raise TypeError(
                "ObservableUpDownCounter must be instantiated via a meter."
            )
        return super().__new__(cls)


class Histogram(_Synchronous, APIHistogram):
    def __init__(
        self,
        name: str,
        instrumentation_scope: InstrumentationScope,
        measurement_consumer: MeasurementConsumer,
        unit: str = "",
        description: str = "",
        explicit_bucket_boundaries_advisory: Sequence[float] | None = None,
        *,
        _meter_config: _ProxyMeterConfig | None = None,
    ):
        super().__init__(
            name,
            unit=unit,
            description=description,
            instrumentation_scope=instrumentation_scope,
            measurement_consumer=measurement_consumer,
            _meter_config=_meter_config,
        )
        self._advisory = _MetricsHistogramAdvisory(
            explicit_bucket_boundaries=explicit_bucket_boundaries_advisory
        )

    def __new__(cls, *args, **kwargs):
        if cls is Histogram:
            raise TypeError("Histogram must be instantiated via a meter.")
        return super().__new__(cls)

    def record(
        self,
        amount: int | float,
        attributes: dict[str, str] | None = None,
        context: Context | None = None,
    ):
        if not self._is_enabled():
            super().record(amount, attributes=attributes, context=context)
            return

        if not math.isfinite(amount):
            _logger.warning(
                "Record amount %s is not finite on Histogram %s, ignoring measurement.",
                amount,
                self.name,
            )
            return
        if amount < 0:
            _logger.warning(
                "Record amount must be non-negative on Histogram %s.",
                self.name,
            )
            return
        time_unix_nano = time_ns()
        self._measurement_consumer.consume_measurement(
            Measurement(
                amount,
                time_unix_nano,
                self,
                context or get_current(),
                attributes,
            )
        )


class Gauge(_Synchronous, APIGauge):
    def __new__(cls, *args, **kwargs):
        if cls is Gauge:
            raise TypeError("Gauge must be instantiated via a meter.")
        return super().__new__(cls)

    def set(
        self,
        amount: int | float,
        attributes: dict[str, str] | None = None,
        context: Context | None = None,
    ):
        if not self._is_enabled():
            super().set(amount, attributes=attributes, context=context)
            return

        if not math.isfinite(amount):
            _logger.warning(
                "Set amount %s is not finite on Gauge %s, ignoring measurement.",
                amount,
                self.name,
            )
            return
        time_unix_nano = time_ns()
        self._measurement_consumer.consume_measurement(
            Measurement(
                amount,
                time_unix_nano,
                self,
                context or get_current(),
                attributes,
            )
        )


class ObservableGauge(_Asynchronous, APIObservableGauge):
    def __new__(cls, *args, **kwargs):
        if cls is ObservableGauge:
            raise TypeError(
                "ObservableGauge must be instantiated via a meter."
            )
        return super().__new__(cls)


# Below classes exist to prevent the direct instantiation
class _Counter(Counter):
    pass


class _UpDownCounter(UpDownCounter):
    pass


class _ObservableCounter(ObservableCounter):
    pass


class _ObservableUpDownCounter(ObservableUpDownCounter):
    pass


class _Histogram(Histogram):
    pass


class _Gauge(Gauge):
    pass


class _ObservableGauge(ObservableGauge):
    pass
