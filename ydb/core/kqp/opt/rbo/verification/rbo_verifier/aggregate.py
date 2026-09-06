"""Typed aggregate reductions over an already selected bag of values.

Parallel input tuples are slot-aligned; guards denote membership, including
SQL non-NULL/DISTINCT selection. This module does not select group representatives,
admit plan lineages, allocate choices, or transport row provenance. Partial states and their
headroom/exactness checks are explicit in the SUM/AVG helpers below.
"""

from __future__ import annotations

from typing import Callable

from . import decimal, floating, smt
from .errors import RelationError
from .ir import AggregateTrait
from .scalar import (
    DecimalAverageState,
    DecimalSumState,
    IntegralAverageCertificate,
    IntegralAverageState,
    Value,
)


GuardedValues = tuple[tuple[smt.Term, Value], ...]
ValueEquality = Callable[[Value, Value], smt.Term]
IntegralAverage = Callable[[smt.Term, smt.Term, smt.Term], smt.Term]


def variance_value(
    trait: AggregateTrait,
    phase: str,
    values: tuple[Value, ...],
    guards: tuple[smt.Term, ...],
    kernel: floating.Kernel,
) -> Value:
    """Literal Welford fold in the caller's shared visitation/flush segment.

    Empty/all-NULL groups are NULL; a singleton is finalized by the runtime
    divide-and-sqrt operations (a non-NULL NaN, not an invented NULL result).
    A physical final fold consumes the complete incoming three-field state.
    """
    def select(
        guard: smt.Term,
        left: floating.VarianceState,
        right: floating.VarianceState,
    ) -> floating.VarianceState:
        return floating.VarianceState(*(floating.Binary64(smt.ite(guard, a, b))
            for a, b in zip(floating.state_terms(left), floating.state_terms(right))))

    state = floating.VarianceState(floating.ZERO, floating.ZERO, floating.ZERO)
    seen = smt.FALSE
    for guard, value in zip(guards, values):
        if guard == smt.FALSE:
            continue
        if phase == "final":
            incoming = value.binary64_state
            if not isinstance(incoming, floating.VarianceState):
                raise RelationError("final stddev_samp requires its complete Welford state")
            updated = incoming if seen == smt.FALSE else kernel.variance_merge(incoming, state)
        else:
            if value.binary64_state is not None or value.average_metadata is not None:
                raise RelationError("stddev_samp raw input cannot contain physical aggregate state")
            item = kernel.from_bits(value.value) if value.type == "Double" else kernel.from_integer(value.value, value.type)
            incoming = kernel.variance_init(item)
            updated = incoming if seen == smt.FALSE else kernel.variance_update(state, item)
        state = select(guard, select(seen, updated, incoming), state)
        seen = smt.or_(seen, guard)
    return Value(
        trait.output_type,
        smt.not_(seen) if trait.output_nullable else smt.FALSE,
        smt.ZERO if phase == "intermediate" else kernel.stddev_sample_finish(state).bits,
        binary64_state=state if phase == "intermediate" else None,
    )


def non_null_membership(
    values: tuple[Value, ...],
    matches: tuple[smt.Term, ...],
    *,
    distinct: bool,
    equal: ValueEquality,
) -> tuple[smt.Term, ...]:
    """Select non-NULL inputs, retaining the first equal value for DISTINCT.

    The caller admits the DISTINCT pair-construction budget before calling.
    Equality is aggregate equality (not SQL predicate equality).
    """

    non_null = tuple(
        smt.and_(matches[index], smt.not_(value.is_null))
        for index, value in enumerate(values)
    )
    if not distinct:
        return non_null
    return tuple(
        smt.and_(
            guard,
            smt.not_(smt.or_(*(
                smt.and_(non_null[earlier], equal(value, values[earlier]))
                for earlier in range(index)
            ))),
        )
        for index, (guard, value) in enumerate(zip(non_null, values))
    )


def reduce(
    trait: AggregateTrait,
    phase: str,
    values: tuple[Value, ...],
    guards: tuple[smt.Term, ...],
    *,
    integral_average: IntegralAverage,
    carry_sum_state: bool = False,
) -> Value:
    """Reduce one selected group; phase describes physical AVG state lanes."""

    if trait.function == "count":
        return Value(
            trait.output_type,
            smt.FALSE,
            smt.add(*(smt.ite(guard, smt.ONE, smt.ZERO) for guard in guards)),
        )
    if trait.function in {"max", "min"}:
        return extremum(
            values,
            guards,
            trait.output_type,
            trait.output_nullable,
            maximum=trait.function == "max",
        )
    if trait.function == "sum":
        if decimal.is_type(trait.output_type):
            return decimal_sum(
                tuple(
                    (guard, value)
                    for guard, value in zip(guards, values)
                    if guard != smt.FALSE
                ),
                trait.output_type,
                trait.output_nullable,
                "Decimal sum",
                carry_state=carry_sum_state,
            )
        total = smt.add(*(
            smt.ite(guard, unwrap_sum(value), smt.ZERO)
            for guard, value in zip(guards, values)
        ))
        return Value(
            trait.output_type,
            smt.not_(smt.or_(*guards))
            if trait.output_nullable and not trait.unwrap else smt.FALSE,
            wrap_sum(total, trait.output_type),
        )
    if trait.function == "avg":
        assert trait.state is not None
        if trait.state.kind == "integral_double_v1":
            return integral_average_value(trait, phase, values, guards, integral_average)
        return decimal_average_value(trait, phase, values, guards)
    raise AssertionError(f"unsupported aggregate function {trait.function!r}")


def extremum(
    values: tuple[Value, ...],
    guards: tuple[smt.Term, ...],
    output_type: str,
    output_nullable: bool,
    *,
    maximum: bool,
) -> Value:
    guarded = tuple(
        (guard, value)
        for guard, value in zip(guards, values)
        if guard != smt.FALSE
    )
    terms = tuple((guard, value.value) for guard, value in guarded)
    is_null = smt.not_(smt.or_(*guards)) if output_nullable else smt.FALSE
    if not decimal.is_type(output_type):
        return Value(output_type, is_null, integral_extremum(terms, maximum=maximum))
    reducer = decimal.aggregate_max if maximum else decimal.aggregate_min
    return Value(
        output_type,
        is_null,
        reducer(terms),
        decimal_finite_abs_bound=max(
            (decimal_finite_abs_bound(value) for _, value in guarded),
            default=0,
        ),
    )


def _literal_sum_interval(value: smt.Term) -> tuple[int, int] | None:
    """Bound a literal/CASE sum without assuming any condition or symbol value.

    ITE takes the union of both value ranges; addition sums their endpoints.
    Unknown leaves fail closed. The local postorder memo handles deep/shared
    payload DAGs without walking Boolean conditions or carrying plan metadata.
    """

    intervals: dict[int, tuple[int, int] | None] = {}
    pending = [value]
    while pending:
        term = pending[-1]
        identity = id(term)
        if identity in intervals:
            pending.pop()
            continue
        if term.sort != smt.INT:
            intervals[identity] = None
        elif term.operation == "int" and type(term.atom) is int:
            intervals[identity] = (term.atom, term.atom)
        elif term.operation in {"ite", "+"}:
            children = term.arguments[1:] if term.operation == "ite" else term.arguments
            missing = [child for child in children if id(child) not in intervals]
            if missing:
                pending.extend(missing)
                continue
            ranges = [intervals[id(child)] for child in children]
            if any(interval is None for interval in ranges):
                intervals[identity] = None
            else:
                known = [interval for interval in ranges if interval is not None]
                intervals[identity] = (
                    (min(low for low, _ in known), max(high for _, high in known))
                    if term.operation == "ite"
                    else (sum(low for low, _ in known), sum(high for _, high in known))
                )
        else:
            intervals[identity] = None
    return intervals[id(value)]


def wrap_sum(value: smt.Term, scalar_type: str) -> smt.Term:
    # Modular wrapping is the identity when the whole raw sum is in range.
    # This is only an encoding simplification: unknown/overflowing intervals
    # retain the original wrap, independently of SQL NULL and row guards.
    if scalar_type in {"Int64", "Uint64"}:
        interval = _literal_sum_interval(value)
        lower = -(1 << 63) if scalar_type == "Int64" else 0
        upper = (1 << (63 if scalar_type == "Int64" else 64)) - 1
        if interval is not None and lower <= interval[0] <= interval[1] <= upper:
            return value
    modulus = 1 << 64
    if scalar_type == "Uint64":
        return smt.mod(value, modulus)
    if scalar_type == "Int64":
        sign = 1 << 63
        return smt.add(
            smt.mod(smt.add(value, smt.int_value(sign)), modulus),
            smt.int_value(-sign),
        )
    raise RelationError(f"sum output type {scalar_type!r} is not modeled")


def integral_extremum(
    guarded_values: tuple[tuple[smt.Term, smt.Term], ...],
    *,
    maximum: bool,
) -> smt.Term:
    """Balanced min/max fold; ties retain the left operand."""

    level = list(guarded_values)
    if not level:
        return smt.ZERO
    while len(level) > 1:
        next_level = []
        for index in range(0, len(level), 2):
            if index + 1 == len(level):
                next_level.append(level[index])
                continue
            left_present, left = level[index]
            right_present, right = level[index + 1]
            right_better = smt.lt(left, right) if maximum else smt.lt(right, left)
            choose_right = smt.and_(
                right_present,
                smt.or_(smt.not_(left_present), right_better),
            )
            next_level.append((
                smt.or_(left_present, right_present),
                smt.ite(choose_right, right, left),
            ))
        level = next_level
    return level[0][1]


def unwrap_sum(value: Value) -> smt.Term:
    """Recognize the exact modular encoding of a nested partial integer sum."""

    modulus = smt.int_value(1 << 64)
    term = value.value
    if (
        value.type == "Uint64"
        and term.operation == "mod"
        and term.arguments[1] == modulus
    ):
        return term.arguments[0]
    if value.type != "Int64" or term.operation != "+" or len(term.arguments) != 2:
        return term
    sign = smt.int_value(1 << 63)
    wrapped, offset = term.arguments
    if (
        offset != smt.int_value(-(1 << 63))
        or wrapped.operation != "mod"
        or wrapped.arguments[1] != modulus
    ):
        return term
    shifted = wrapped.arguments[0]
    if shifted.operation != "+" or len(shifted.arguments) != 2:
        return term
    raw, shift = shifted.arguments
    return raw if shift == sign else term


def decimal_finite_abs_bound(value: Value) -> int:
    if value.decimal_finite_abs_bound is not None:
        return value.decimal_finite_abs_bound
    decimal_type = decimal.parse_type(value.type)
    if decimal_type is None:
        raise RelationError(f"Decimal sum input type {value.type!r} is not modeled")
    return 10**decimal_type.precision - 1


def _require_sum_headroom(bound: int, output_type: str, operation: str) -> None:
    result_type = decimal.parse_type(output_type)
    assert result_type is not None
    if bound >= 10**result_type.precision:
        raise RelationError(
            f"{operation} may overflow its {output_type} accumulator "
            "within the current bound; non-associative overflow is not modeled"
        )


def decimal_sum(
    guarded_values: GuardedValues,
    output_type: str,
    output_nullable: bool,
    operation: str,
    *,
    carry_state: bool = False,
) -> Value:
    """Exact Decimal SUM, only where finite addition cannot overflow."""

    finite_abs_bound = sum(
        decimal_finite_abs_bound(value) for _guard, value in guarded_values
    )
    _require_sum_headroom(finite_abs_bound, output_type, operation)
    state = decimal.summarize_sum_with_headroom(
        tuple((guard, value.value) for guard, value in guarded_values),
        output_type,
        finite_abs_bound,
    )
    return finish_decimal_sum(state, output_nullable, carry_state=carry_state)


def combine_decimal_sum(
    guarded_states: tuple[tuple[smt.Term, DecimalSumState], ...],
    output_type: str,
    output_nullable: bool,
) -> Value:
    """Combine a complete lineage-admitted set of validated partial states."""

    finite_abs_bound = sum(state.finite_abs_bound for _guard, state in guarded_states)
    _require_sum_headroom(finite_abs_bound, output_type, "Decimal sum")
    state = decimal.combine_sum_states_with_headroom(guarded_states, output_type)
    return finish_decimal_sum(state, output_nullable, carry_state=False)


def finish_decimal_sum(
    state: DecimalSumState,
    output_nullable: bool,
    *,
    carry_state: bool,
) -> Value:
    return Value(
        state.sum_type,
        smt.not_(state.any_non_null) if output_nullable else smt.FALSE,
        decimal.finish_sum_state(state),
        decimal_finite_abs_bound=state.finite_abs_bound,
        decimal_sum_state=state if carry_state else None,
    )


def decimal_average_value(
    trait: AggregateTrait,
    phase: str,
    values: tuple[Value, ...],
    guards: tuple[smt.Term, ...],
) -> Value:
    """Translate scalar inputs or final-phase physical sum/count lanes."""

    assert trait.state is not None and trait.state.kind == "decimal"
    guarded_sums: list[tuple[smt.Term, smt.Term]] = []
    count_terms: list[smt.Term] = []
    finite_abs_bound = 0
    count_bound = 0
    for guard, value in zip(guards, values):
        if guard == smt.FALSE:
            continue
        if phase == "final":
            state = value.average_metadata
            if (
                not isinstance(state, DecimalAverageState)
                or state.sum_type != trait.state.sum_type
            ):
                raise RelationError(
                    "final avg input does not carry its validated "
                    "intermediate Decimal state"
                )
            guarded_sums.append((guard, state.sum))
            finite_abs_bound += state.finite_abs_bound
            count_bound += state.count_bound
            count_terms.append(smt.ite(guard, state.count, smt.ZERO))
        else:
            guarded_sums.append((guard, value.value))
            finite_abs_bound += decimal_finite_abs_bound(value)
            count_bound += 1
            count_terms.append(smt.ite(guard, smt.ONE, smt.ZERO))
    return finish_decimal_average(
        tuple(guarded_sums),
        tuple(count_terms),
        sum_type=trait.state.sum_type,
        count_type=trait.state.count_type,
        output_type=trait.output_type,
        output_nullable=trait.output_nullable,
        finite_abs_bound=finite_abs_bound,
        count_bound=count_bound,
        carry_state=phase == "intermediate",
        operation="Decimal avg",
    )


def finish_decimal_average(
    guarded_sums: tuple[tuple[smt.Term, smt.Term], ...],
    count_terms: tuple[smt.Term, ...],
    *,
    sum_type: str,
    count_type: str,
    output_type: str,
    output_nullable: bool,
    finite_abs_bound: int,
    count_bound: int,
    carry_state: bool,
    operation: str,
) -> Value:
    """Finish exact Decimal AVG after checking both accumulator bounds."""

    if len(guarded_sums) != len(count_terms):
        raise AssertionError("Decimal average sum/count contributions disagree")
    result = decimal.parse_type(output_type)
    assert decimal.parse_type(sum_type) is not None and result is not None
    _require_sum_headroom(finite_abs_bound, sum_type, f"{operation} sum")
    if count_bound >= 1 << 64:
        raise RelationError(
            f"{operation} count may wrap its Uint64 accumulator "
            "within the current bound"
        )
    total = decimal.sum_with_headroom(guarded_sums, sum_type, finite_abs_bound)
    count = smt.add(*count_terms)
    average = decimal.narrow_same_scale(
        decimal.divide(total, count, sum_type, count_type), sum_type, output_type,
    )
    return Value(
        output_type,
        smt.eq(count, smt.ZERO) if output_nullable else smt.FALSE,
        average,
        decimal_finite_abs_bound=min(finite_abs_bound, 10**result.precision - 1),
        average_metadata=(
            DecimalAverageState(
                sum_type=sum_type,
                sum=total,
                count=count,
                finite_abs_bound=finite_abs_bound,
                count_bound=count_bound,
            )
            if carry_state else None
        ),
    )


def integral_average_value(
    trait: AggregateTrait,
    phase: str,
    values: tuple[Value, ...],
    guards: tuple[smt.Term, ...],
    encode: IntegralAverage,
) -> Value:
    """Retain physical state for partial AVG, proof certificate otherwise."""

    assert trait.state is not None
    assert trait.state.kind == "integral_double_v1"
    assert trait.state.exact_when_count_at_most == 2
    count_terms: list[smt.Term] = []
    count_bound = 0
    minimum = smt.int_value((1 << 63) - 1)
    maximum = smt.int_value(-(1 << 63))
    for guard, value in zip(guards, values):
        if guard == smt.FALSE:
            continue
        if phase == "final":
            state = value.average_metadata
            if not isinstance(state, IntegralAverageState):
                raise RelationError(
                    "final integral avg input does not carry its validated "
                    "intermediate state"
                )
            member_count = state.count
            member_minimum = state.minimum
            member_maximum = state.maximum
            count_bound += state.count_bound
        else:
            member_count = smt.ONE
            member_minimum = value.value
            member_maximum = value.value
            count_bound += 1
        count_terms.append(smt.ite(guard, member_count, smt.ZERO))
        minimum = smt.ite(
            guard,
            smt.ite(smt.lt(member_minimum, minimum), member_minimum, minimum),
            minimum,
        )
        maximum = smt.ite(
            guard,
            smt.ite(smt.lt(maximum, member_maximum), member_maximum, maximum),
            maximum,
        )
    if count_bound >= 1 << 64:
        raise RelationError(
            "integral avg count may wrap its Uint64 accumulator "
            "within the current bound"
        )
    count = smt.add(*count_terms)
    return Value(
        trait.output_type,
        smt.not_(smt.or_(*guards)) if trait.output_nullable else smt.FALSE,
        encode(count, minimum, maximum),
        average_metadata=(
            IntegralAverageState(
                count=count,
                minimum=minimum,
                maximum=maximum,
                count_bound=count_bound,
            )
            if phase == "intermediate" else IntegralAverageCertificate(count)
        ),
    )
