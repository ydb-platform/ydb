# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.trace.span import INVALID_SPAN
from opentelemetry.util.types import Attributes


class ExemplarFilter(ABC):
    """``ExemplarFilter`` determines which measurements are eligible for becoming an
    ``Exemplar``.

    Exemplar filters are used to filter measurements before attempting to store them
    in a reservoir.

    Reference:
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#exemplarfilter
    """

    @abstractmethod
    def should_sample(
        self,
        value: int | float,
        time_unix_nano: int,
        attributes: Attributes,
        context: Context,
    ) -> bool:
        """Returns whether or not a reservoir should attempt to filter a measurement.

        Args:
            value: The value of the measurement
            timestamp: A timestamp that best represents when the measurement was taken
            attributes: The complete set of measurement attributes
            context: The Context of the measurement
        """
        raise NotImplementedError(
            "ExemplarFilter.should_sample is not implemented"
        )


class AlwaysOnExemplarFilter(ExemplarFilter):
    """An ExemplarFilter which makes all measurements eligible for being an Exemplar.

    Reference:
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#alwayson
    """

    def should_sample(
        self,
        value: int | float,
        time_unix_nano: int,
        attributes: Attributes,
        context: Context,
    ) -> bool:
        """Returns whether or not a reservoir should attempt to filter a measurement.

        Args:
            value: The value of the measurement
            timestamp: A timestamp that best represents when the measurement was taken
            attributes: The complete set of measurement attributes
            context: The Context of the measurement
        """
        return True


class AlwaysOffExemplarFilter(ExemplarFilter):
    """An ExemplarFilter which makes no measurements eligible for being an Exemplar.

    Using this ExemplarFilter is as good as disabling Exemplar feature.

    Reference:
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#alwaysoff
    """

    def should_sample(
        self,
        value: int | float,
        time_unix_nano: int,
        attributes: Attributes,
        context: Context,
    ) -> bool:
        """Returns whether or not a reservoir should attempt to filter a measurement.

        Args:
            value: The value of the measurement
            timestamp: A timestamp that best represents when the measurement was taken
            attributes: The complete set of measurement attributes
            context: The Context of the measurement
        """
        return False


class TraceBasedExemplarFilter(ExemplarFilter):
    """An ExemplarFilter which makes those measurements eligible for being an Exemplar,
    which are recorded in the context of a sampled parent span.

    Reference:
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#tracebased
    """

    def should_sample(
        self,
        value: int | float,
        time_unix_nano: int,
        attributes: Attributes,
        context: Context,
    ) -> bool:
        """Returns whether or not a reservoir should attempt to filter a measurement.

        Args:
            value: The value of the measurement
            timestamp: A timestamp that best represents when the measurement was taken
            attributes: The complete set of measurement attributes
            context: The Context of the measurement
        """
        span = trace.get_current_span(context)
        if span == INVALID_SPAN:
            return False
        return span.get_span_context().trace_flags.sampled
