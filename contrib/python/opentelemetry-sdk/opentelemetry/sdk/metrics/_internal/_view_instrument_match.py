# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from collections.abc import Sequence
from logging import getLogger
from threading import Lock
from time import time_ns
from typing import cast

from opentelemetry.sdk.metrics._internal.aggregation import (
    Aggregation,
    AggregationTemporality,
    DefaultAggregation,
    _Aggregation,
    _SumAggregation,
)
from opentelemetry.sdk.metrics._internal.instrument import _Instrument
from opentelemetry.sdk.metrics._internal.measurement import Measurement
from opentelemetry.sdk.metrics._internal.point import DataPointT
from opentelemetry.sdk.metrics._internal.view import View

_logger = getLogger(__name__)


class _ViewInstrumentMatch:
    def __init__(
        self,
        view: View,
        instrument: _Instrument,
        instrument_class_aggregation: dict[type, Aggregation],
    ):
        self._view = view
        self._instrument = instrument
        self._attributes_aggregation: dict[frozenset, _Aggregation] = {}
        self._lock = Lock()
        self._instrument_class_aggregation = instrument_class_aggregation
        self._name = self._view._name or self._instrument.name
        self._description = (
            self._view._description or self._instrument.description
        )
        if not isinstance(self._view._aggregation, DefaultAggregation):
            self._aggregation = self._view._aggregation._create_aggregation(
                self._instrument,
                None,
                self._view._exemplar_reservoir_factory,
                0,
            )
        else:
            self._aggregation = self._instrument_class_aggregation[
                self._instrument.__class__
            ]._create_aggregation(
                self._instrument,
                None,
                self._view._exemplar_reservoir_factory,
                0,
            )

    def conflicts(self, other: "_ViewInstrumentMatch") -> bool:
        # pylint: disable=protected-access

        result = (
            self._name == other._name
            and self._instrument.unit == other._instrument.unit
            # The aggregation class is being used here instead of data point
            # type since they are functionally equivalent.
            and self._aggregation.__class__ == other._aggregation.__class__
        )
        if not result:
            return result

        if isinstance(self._aggregation, _SumAggregation):
            # if result is True the two aggregation are of the same type
            self._aggregation = cast(_SumAggregation, self._aggregation)
            other._aggregation = cast(_SumAggregation, other._aggregation)

            result = (
                self._aggregation._instrument_is_monotonic
                == other._aggregation._instrument_is_monotonic
                and self._aggregation._instrument_aggregation_temporality
                == other._aggregation._instrument_aggregation_temporality
            )

        return result

    # pylint: disable=protected-access
    def consume_measurement(
        self, measurement: Measurement, should_sample_exemplar: bool = True
    ) -> None:
        if self._view._attribute_keys is not None:
            attributes = {}

            for key, value in (measurement.attributes or {}).items():
                if key in self._view._attribute_keys:
                    attributes[key] = value
        elif measurement.attributes is not None:
            attributes = dict(measurement.attributes)
        else:
            attributes = {}

        aggr_key = frozenset(attributes.items())

        if aggr_key not in self._attributes_aggregation:
            with self._lock:
                if aggr_key not in self._attributes_aggregation:
                    if not isinstance(
                        self._view._aggregation, DefaultAggregation
                    ):
                        aggregation = (
                            self._view._aggregation._create_aggregation(
                                self._instrument,
                                attributes,
                                self._view._exemplar_reservoir_factory,
                                time_ns(),
                            )
                        )
                    else:
                        aggregation = self._instrument_class_aggregation[
                            self._instrument.__class__
                        ]._create_aggregation(
                            self._instrument,
                            attributes,
                            self._view._exemplar_reservoir_factory,
                            time_ns(),
                        )
                    self._attributes_aggregation[aggr_key] = aggregation

        self._attributes_aggregation[aggr_key].aggregate(
            measurement, should_sample_exemplar
        )

    def collect(
        self,
        collection_aggregation_temporality: AggregationTemporality,
        collection_start_nanos: int,
    ) -> Sequence[DataPointT] | None:
        data_points: list[DataPointT] = []
        with self._lock:
            for aggregation in self._attributes_aggregation.values():
                data_point = aggregation.collect(
                    collection_aggregation_temporality, collection_start_nanos
                )
                if data_point is not None:
                    data_points.append(data_point)

        # Returning here None instead of an empty list because the caller
        # does not consume a sequence and to be consistent with the rest of
        # collect methods that also return None.
        return data_points or None
