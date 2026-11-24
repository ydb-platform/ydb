# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# pylint: disable=unused-import

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from threading import Lock
from time import time_ns

# This kind of import is needed to avoid Sphinx errors.
import opentelemetry.sdk.metrics
import opentelemetry.sdk.metrics._internal.instrument
from opentelemetry.metrics._internal.instrument import CallbackOptions
from opentelemetry.sdk.metrics._internal.exceptions import MetricsTimeoutError
from opentelemetry.sdk.metrics._internal.measurement import Measurement
from opentelemetry.sdk.metrics._internal.metric_reader_storage import (
    MetricReaderStorage,
)
from opentelemetry.sdk.metrics._internal.point import MetricsData


class MeasurementConsumer(ABC):
    @abstractmethod
    def consume_measurement(self, measurement: Measurement) -> None:
        pass

    @abstractmethod
    def register_asynchronous_instrument(
        self,
        instrument: (
            "opentelemetry.sdk.metrics._internal.instrument._Asynchronous"
        ),
    ):
        pass

    @abstractmethod
    def collect(
        self,
        metric_reader: "opentelemetry.sdk.metrics.export.MetricReader",
        timeout_millis: float = 10_000,
    ) -> MetricsData | None:
        pass


class SynchronousMeasurementConsumer(MeasurementConsumer):
    def __init__(
        self,
        sdk_config: "opentelemetry.sdk.metrics._internal.sdk_configuration.SdkConfiguration",
        metric_readers: Iterable["opentelemetry.sdk.metrics.MetricReader"],
    ) -> None:
        self._lock = Lock()
        self._sdk_config = sdk_config
        self._reader_storages: Mapping[
            opentelemetry.sdk.metrics.export.MetricReader, MetricReaderStorage
        ] = {
            reader: MetricReaderStorage(
                sdk_config,
                reader._instrument_class_temporality,
                reader._instrument_class_aggregation,
            )
            for reader in metric_readers
        }
        self._async_instruments: list[
            opentelemetry.sdk.metrics._internal.instrument._Asynchronous
        ] = []

    def consume_measurement(self, measurement: Measurement) -> None:
        should_sample_exemplar = (
            self._sdk_config.exemplar_filter.should_sample(
                measurement.value,
                measurement.time_unix_nano,
                measurement.attributes,
                measurement.context,
            )
        )
        # `_reader_storages` is replaced (never mutated in place) by
        # `add_metric_reader` and `remove_metric_reader`, so it is safe
        # to iterate over without a lock.
        for reader_storage in self._reader_storages.values():
            reader_storage.consume_measurement(
                measurement, should_sample_exemplar
            )

    def register_asynchronous_instrument(
        self,
        instrument: (
            "opentelemetry.sdk.metrics._internal.instrument._Asynchronous"
        ),
    ) -> None:
        with self._lock:
            self._async_instruments.append(instrument)

    def collect(
        self,
        metric_reader: "opentelemetry.sdk.metrics.export.MetricReader",
        timeout_millis: float = 10_000,
    ) -> MetricsData | None:
        with self._lock:
            metric_reader_storage = self._reader_storages[metric_reader]
            # for now, just use the defaults
            callback_options = CallbackOptions()
            deadline_ns = time_ns() + (timeout_millis * 1e6)

            default_timeout_ns = 10000 * 1e6

            for async_instrument in self._async_instruments:
                remaining_time = deadline_ns - time_ns()

                if remaining_time < default_timeout_ns:
                    callback_options = CallbackOptions(
                        timeout_millis=remaining_time / 1e6
                    )

                measurements = async_instrument.callback(callback_options)
                if time_ns() >= deadline_ns:
                    raise MetricsTimeoutError(
                        "Timed out while executing callback"
                    )

                for measurement in measurements:
                    should_sample_exemplar = (
                        self._sdk_config.exemplar_filter.should_sample(
                            measurement.value,
                            measurement.time_unix_nano,
                            measurement.attributes,
                            measurement.context,
                        )
                    )
                    metric_reader_storage.consume_measurement(
                        measurement, should_sample_exemplar
                    )

            result = self._reader_storages[metric_reader].collect()

        return result

    def add_metric_reader(
        self, metric_reader: "opentelemetry.sdk.metrics.MetricReader"
    ) -> None:
        """Registers a new metric reader."""
        # Build a new mapping and swap it in atomically so that
        # a concurrent consume_measurement never iterates a mapping
        # that is being mutated in place.
        with self._lock:
            new_reader_storages = dict(self._reader_storages)
            new_reader_storages[metric_reader] = MetricReaderStorage(
                self._sdk_config,
                # pylint: disable-next=protected-access
                metric_reader._instrument_class_temporality,
                # pylint: disable-next=protected-access
                metric_reader._instrument_class_aggregation,
            )
            self._reader_storages = new_reader_storages

    def remove_metric_reader(
        self, metric_reader: "opentelemetry.sdk.metrics.MetricReader"
    ) -> None:
        """Unregisters the given metric reader."""
        # Mutate using copy-on-write: see add_metric_reader.
        with self._lock:
            new_reader_storages = dict(self._reader_storages)
            new_reader_storages.pop(metric_reader)
            self._reader_storages = new_reader_storages
