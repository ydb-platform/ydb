# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Sequence

from opentelemetry.context import Context
from opentelemetry.trace import Link, SpanKind, TraceState
from opentelemetry.util.types import Attributes

from ._composable import ComposableSampler, SamplingIntent
from ._trace_state import serialize_th
from ._util import INVALID_THRESHOLD, MAX_THRESHOLD, calculate_threshold


class ComposableTraceIDRatioBased(ComposableSampler):
    _threshold: int
    _description: str

    def __init__(self, ratio: float):
        threshold = calculate_threshold(ratio)
        if threshold == MAX_THRESHOLD:
            threshold_str = "max"
        else:
            threshold_str = serialize_th(threshold)
        if threshold != MAX_THRESHOLD:
            intent = SamplingIntent(threshold=threshold)
        else:
            intent = SamplingIntent(
                threshold=INVALID_THRESHOLD, threshold_reliable=False
            )
        self._intent = intent
        self._description = f"ComposableTraceIDRatioBased{{threshold={threshold_str}, ratio={ratio}}}"

    def sampling_intent(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> SamplingIntent:
        return self._intent

    def get_description(self) -> str:
        return self._description


def composable_traceid_ratio_based(
    ratio: float,
) -> ComposableSampler:
    """Returns a composable sampler that samples each span with a fixed ratio.

    - Returns a SamplingIntent with threshold determined by the configured sampling ratio
    - Sets threshold_reliable to true
    - Does not add any attributes

    Note:
        If the ratio is 0, it will behave as an ComposableAlwaysOff sampler instead.

    Args:
        ratio: The sampling ratio to use (between 0.0 and 1.0).
    """
    if not 0.0 <= ratio <= 1.0:
        raise ValueError("Sampling ratio must be between 0.0 and 1.0")

    return ComposableTraceIDRatioBased(ratio)
