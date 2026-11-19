# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Sequence
from fnmatch import fnmatchcase
from typing import Protocol

from opentelemetry.context import Context
from opentelemetry.trace import (
    Link,
    SpanKind,
    TraceState,
    get_current_span,
)
from opentelemetry.util.types import AnyValue, Attributes

from ._composable import ComposableSampler, SamplingIntent
from ._util import INVALID_THRESHOLD


class PredicateT(Protocol):
    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool: ...

    def __str__(self) -> str: ...


class AttributePredicate:
    """An exact match of an attribute value"""

    def __init__(self, key: str, value: AnyValue):
        logging.warning(
            "This is deprecated, use AttributeValuesPredicate instead"
        )
        self.key = key
        self.value = value

    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        if not attributes:
            return False
        return attributes.get(self.key) == self.value

    def __str__(self):
        return f"{self.key}={self.value}"


class AlwaysMatchPredicate:
    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        return True

    def __str__(self) -> str:
        return "AlwaysMatch"


class AllPredicate:
    def __init__(self, predicates: Sequence[PredicateT]):
        self._predicates = tuple(predicates)

    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        return all(
            predicate(
                parent_ctx,
                name,
                span_kind,
                attributes,
                links,
                trace_state,
            )
            for predicate in self._predicates
        )

    def __str__(self) -> str:
        return " && ".join(str(predicate) for predicate in self._predicates)


class AttributeValuesPredicate:
    def __init__(self, key: str, values: Sequence[str]):
        self._key = key
        self._values = frozenset(values)

    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        if not attributes or self._key not in attributes:
            return False
        return any(
            str(value) in self._values
            for value in _attribute_values(attributes[self._key])
        )

    def __str__(self) -> str:
        values = ",".join(sorted(self._values))
        return f"{self._key} in [{values}]"


class AttributePatternsPredicate:
    def __init__(
        self,
        key: str,
        included: Sequence[str] | None = None,
        excluded: Sequence[str] | None = None,
    ):
        self._key = key
        self._included = tuple(included or ())
        self._excluded = tuple(excluded or ())

    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        if not attributes or self._key not in attributes:
            return False
        return any(
            self._matches_value(str(value))
            for value in _attribute_values(attributes[self._key])
        )

    def _matches_value(self, value: str) -> bool:
        included = not self._included or any(
            fnmatchcase(value, pattern) for pattern in self._included
        )
        excluded = any(
            fnmatchcase(value, pattern) for pattern in self._excluded
        )
        return included and not excluded

    def __str__(self) -> str:
        return f"{self._key} matches"


class SpanKindPredicate:
    def __init__(self, span_kinds: Sequence[SpanKind]):
        self._span_kinds = frozenset(span_kinds)

    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        return span_kind in self._span_kinds

    def __str__(self) -> str:
        kinds = ",".join(kind.name.lower() for kind in self._span_kinds)
        return f"span_kind in [{kinds}]"


class ParentPredicate:
    def __init__(self, parents: Sequence[str]):
        self._parents = frozenset(parents)

    def __call__(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None,
    ) -> bool:
        parent_span_context = get_current_span(parent_ctx).get_span_context()
        if not parent_span_context.is_valid:
            parent = "none"
        elif parent_span_context.is_remote:
            parent = "remote"
        else:
            parent = "local"
        return parent in self._parents

    def __str__(self) -> str:
        parents = ",".join(self._parents)
        return f"parent in [{parents}]"


def _attribute_values(value):
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return value
    return (value,)


RulesT = Sequence[tuple[PredicateT, ComposableSampler]]

_non_sampling_intent = SamplingIntent(
    threshold=INVALID_THRESHOLD, threshold_reliable=False
)


class _ComposableRuleBased(ComposableSampler):
    def __init__(self, rules: RulesT):
        # work on an internal copy of the rules
        self._rules = list(rules)

    def sampling_intent(
        self,
        parent_ctx: Context | None,
        name: str,
        span_kind: SpanKind | None,
        attributes: Attributes,
        links: Sequence[Link] | None,
        trace_state: TraceState | None = None,
    ) -> SamplingIntent:
        for predicate, sampler in self._rules:
            if predicate(
                parent_ctx=parent_ctx,
                name=name,
                span_kind=span_kind,
                attributes=attributes,
                links=links,
                trace_state=trace_state,
            ):
                return sampler.sampling_intent(
                    parent_ctx=parent_ctx,
                    name=name,
                    span_kind=span_kind,
                    attributes=attributes,
                    links=links,
                    trace_state=trace_state,
                )
        return _non_sampling_intent

    def get_description(self) -> str:
        rules_str = ",".join(
            f"({predicate}:{sampler.get_description()})"
            for predicate, sampler in self._rules
        )
        return f"ComposableRuleBased{{[{rules_str}]}}"


def composable_rule_based(
    rules: RulesT,
) -> ComposableSampler:
    """Returns a consistent sampler that:

    - Evaluates a series of rules based on predicates and returns the SamplingIntent from the first matching sampler
    - If no rules match, returns a non-sampling intent

    Args:
        rules: A list of (Predicate, ComposableSampler) pairs, where Predicate is a function that evaluates whether a rule applies
    """
    return _ComposableRuleBased(rules)
