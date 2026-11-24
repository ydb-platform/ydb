# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import dataclasses

from opentelemetry.util.types import Attributes


@dataclasses.dataclass(frozen=True)
class Exemplar:
    """A representation of an exemplar, which is a sample input measurement.

    Exemplars also hold information about the environment when the measurement
    was recorded, for example the span and trace ID of the active span when the
    exemplar was recorded.

    Attributes:
        trace_id: (optional) The trace associated with a recording
        span_id: (optional) The span associated with a recording
        time_unix_nano: The time of the observation
        value: The recorded value
        filtered_attributes: A set of filtered attributes which provide additional insight into the Context when the observation was made.

    References:
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#exemplars
        https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/sdk.md#exemplar
    """

    filtered_attributes: Attributes
    value: int | float
    time_unix_nano: int
    span_id: int | None = None
    trace_id: int | None = None
