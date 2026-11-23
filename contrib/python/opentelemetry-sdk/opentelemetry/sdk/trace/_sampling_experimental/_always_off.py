# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Sequence

from opentelemetry.context import Context
from opentelemetry.trace import Link, SpanKind, TraceState
from opentelemetry.util.types import Attributes

from ._composable import ComposableSampler, SamplingIntent
from ._util import INVALID_THRESHOLD

_intent = SamplingIntent(threshold=INVALID_THRESHOLD, threshold_reliable=False)


class _ComposableAlwaysOffSampler(ComposableSampler):
    def sampling_intent(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None = None,
    ) -> SamplingIntent:
        return _intent

    def get_description(self) -> str:
        return "ComposableAlwaysOff"


_always_off = _ComposableAlwaysOffSampler()


def composable_always_off() -> ComposableSampler:
    """Returns a composable sampler that does not sample any span.

    - Always returns a SamplingIntent with no threshold, indicating all spans should be dropped
    - Sets threshold_reliable to false
    - Does not add any attributes
    """
    return _always_off
