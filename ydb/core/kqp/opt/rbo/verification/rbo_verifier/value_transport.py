"""Explicit transport rules for scalar values, physical state and proof hints.

Ordinary selection drops proof-only SUM hints and rejects hidden AVG state.
Exclusive task-copy selection must preserve physical AVG state; it may retain a
SUM hint only when every alternative reconstructs its authoritative scalar.
Completed-AVG certificates are node-local and may not cross either boundary.
"""

from __future__ import annotations

from typing import Callable, Iterable

from . import decimal, smt
from .scalar import DecimalAverageState, DecimalSumState, IntegralAverageState, Value


class ValueTransportError(ValueError):
    pass


def _select_terms(
    candidates: tuple[tuple[smt.Term, smt.Term], ...],
    fallback: smt.Term,
) -> smt.Term:
    """First true guard wins; fallback is explicit, including for absent rows."""

    for guard, term in reversed(candidates):
        fallback = smt.ite(guard, term, fallback)
    return fallback


def _finite_bound(values: tuple[Value, ...]) -> int | None:
    bounds = tuple(value.decimal_finite_abs_bound for value in values)
    return None if None in bounds else max(bound for bound in bounds if bound is not None)


def select_scalar(
    candidates: tuple[tuple[smt.Term, Value], ...],
    fallback: Value,
) -> Value:
    """Select ordinary payload, weakening bounds and discarding SUM proof hints."""

    values = tuple(value for _, value in candidates) + (fallback,)
    if any(value.type != fallback.type for value in values):
        raise ValueTransportError("scalar selection requires identical value types")
    if any(value.average_metadata is not None for value in values):
        raise ValueTransportError("scalar selection cannot select hidden AVG metadata")
    return Value(
        fallback.type,
        _select_terms(tuple((guard, value.is_null) for guard, value in candidates), fallback.is_null),
        _select_terms(tuple((guard, value.value) for guard, value in candidates), fallback.value),
        _finite_bound(values),
    )


def validated_decimal_sum_state(value: Value, nullable: bool) -> DecimalSumState | None:
    """Return a proof hint only when it exactly reconstructs its scalar."""

    state = value.decimal_sum_state
    if (
        not isinstance(state, DecimalSumState)
        or state.sum_type != value.type
        or state.finite_abs_bound != value.decimal_finite_abs_bound
        or value.is_null != (smt.not_(state.any_non_null) if nullable else smt.FALSE)
        or value.value != decimal.finish_sum_state(state)
    ):
        return None
    return state


def merge_exclusive_values(
    guards: tuple[smt.Term, ...],
    values: tuple[Value, ...],
    *,
    nullable: bool,
) -> Value:
    """Select copies of one occurrence under caller-proved exclusive guards.

    Physical AVG states must have one common layout. SUM summaries are optional
    proof hints: missing, stale or incompatible hints fall back to the scalar.
    The last payload is the fallback when no copy is present (unobservable).
    """

    if not values or len(values) != len(guards):
        raise ValueTransportError("exclusive value selection requires aligned nonempty inputs")
    if any(value.type != values[0].type for value in values[1:]):
        raise ValueTransportError("exclusive row compaction received different value types")

    def select(terms: Iterable[smt.Term]) -> smt.Term:
        terms = tuple(terms)
        return _select_terms(tuple(zip(guards[:-1], terms[:-1])), terms[-1])

    is_null = select(value.is_null for value in values)
    payload = select(value.value for value in values)
    bound = _finite_bound(values)
    average = _merge_average_states(values, select)

    sum_states = tuple(validated_decimal_sum_state(value, nullable) for value in values)
    summed = None
    if average is None and all(state is not None for state in sum_states):
        states = tuple(state for state in sum_states if state is not None)
        if all(state.sum_type == states[0].sum_type for state in states):
            summed = DecimalSumState(
                sum_type=states[0].sum_type,
                any_non_null=select(state.any_non_null for state in states),
                has_nan=select(state.has_nan for state in states),
                has_pos_inf=select(state.has_pos_inf for state in states),
                has_neg_inf=select(state.has_neg_inf for state in states),
                finite_total=select(state.finite_total for state in states),
                finite_abs_bound=max(state.finite_abs_bound for state in states),
            )
            is_null = smt.not_(summed.any_non_null) if nullable else smt.FALSE
            payload = decimal.finish_sum_state(summed)
            bound = summed.finite_abs_bound
    return Value(values[0].type, is_null, payload, bound, average, summed)


def _merge_average_states(
    values: tuple[Value, ...],
    select: Callable[[Iterable[smt.Term]], smt.Term],
) -> DecimalAverageState | IntegralAverageState | None:
    """AVG is physical state, so losing or changing its layout is an error."""

    states = tuple(value.average_metadata for value in values)
    if all(state is None for state in states):
        return None
    if any(state is None for state in states):
        raise ValueTransportError("exclusive row compaction mixed AVG state and scalar values")
    if any(type(state) is not type(states[0]) for state in states[1:]):
        raise ValueTransportError("exclusive row compaction received different AVG state types")
    first = states[0]
    if isinstance(first, DecimalAverageState):
        if any(state.sum_type != first.sum_type for state in states):
            raise ValueTransportError("exclusive row compaction received different Decimal AVG state layouts")
        return DecimalAverageState(
            sum_type=first.sum_type,
            sum=select(state.sum for state in states),
            count=select(state.count for state in states),
            finite_abs_bound=max(state.finite_abs_bound for state in states),
            count_bound=max(state.count_bound for state in states),
        )
    if isinstance(first, IntegralAverageState):
        return IntegralAverageState(
            count=select(state.count for state in states),
            minimum=select(state.minimum for state in states),
            maximum=select(state.maximum for state in states),
            count_bound=max(state.count_bound for state in states),
        )
    raise ValueTransportError("exclusive row compaction received unsupported AVG metadata")
