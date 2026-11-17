# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from opentelemetry.context import Context
from opentelemetry.util.types import Attributes

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics._internal.instrument import _Instrument


@dataclass(frozen=True)
class Measurement:
    """
    Represents a data point reported via the metrics API to the SDK.

    Attributes:
        value: Measured value
        time_unix_nano: The time the API call was made to record the Measurement
        instrument: The instrument that produced this `Measurement`.
        context: The active Context of the Measurement at API call time.
        attributes: Measurement attributes
    """

    value: int | float
    time_unix_nano: int
    instrument: _Instrument
    context: Context
    attributes: Attributes = None
