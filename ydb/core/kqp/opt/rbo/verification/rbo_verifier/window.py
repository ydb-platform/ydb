"""Task-local window values for admitted partitions and unstable sort choices.

Callers validate the window shape, admit construction bounds, and allocate
constrained ordinals only for ROWS frames. ANSI Rank and whole-partition
SUM/AVG are independent of peer order. These kernels consume aligned values,
row presence, and (where needed) ordinals;
they neither choose task routing nor publish an observable output sequence.
"""

from __future__ import annotations

from typing import Callable

from . import aggregate, decimal, smt
from .scalar import Value


ValueComparison = Callable[[Value, Value], smt.Term]
ValueKey = tuple[Value, ...]


def rank_values(
    present: tuple[smt.Term, ...],
    keys: tuple[ValueKey, ...],
    partitions: tuple[tuple[Value, ...], ...],
    less: Callable[[ValueKey, ValueKey], smt.Term],
    not_distinct: ValueComparison,
) -> tuple[Value, ...]:
    """ANSI Rank is one plus strictly preceding rows in the same partition.

    Runtime AggrEquals makes NULLs and Decimal NaNs peers. Equal keys share a
    rank and leave gaps; their unstable physical ordering cannot change Rank.
    """

    result: list[Value] = []
    for candidate_key, candidate_partition in zip(keys, partitions):
        preceding = tuple(
            smt.ite(
                smt.and_(
                    other_present,
                    *(not_distinct(left, right) for left, right in zip(candidate_partition, partition)),
                    less(other_key, candidate_key),
                ),
                smt.ONE,
                smt.ZERO,
            )
            for other_present, other_key, partition in zip(present, keys, partitions)
        )
        result.append(Value("Uint64", smt.FALSE, smt.add(smt.ONE, *preceding)))
    return tuple(result)


def whole_partition_decimal(
    kind: str,
    output_type: str,
    values: tuple[Value, ...],
    present: tuple[smt.Term, ...],
    partitions: tuple[tuple[Value, ...], ...],
    candidate_partition: tuple[Value, ...],
    not_distinct: ValueComparison,
) -> Value:
    """Unordered SUM/AVG over the candidate's complete null-safe partition."""

    assert kind in {"window_sum", "window_avg"}
    guarded_values = tuple(
        (
            smt.and_(
                row_present,
                *(
                    not_distinct(candidate, key)
                    for candidate, key in zip(candidate_partition, partition)
                ),
                smt.not_(value.is_null),
            ),
            value,
        )
        for value, row_present, partition in zip(values, present, partitions)
    )
    if kind == "window_sum":
        return aggregate.decimal_sum(
            guarded_values, output_type, True, "Decimal window sum",
        )
    return aggregate.finish_decimal_average(
        tuple((guard, value.value) for guard, value in guarded_values),
        tuple(smt.ite(guard, smt.ONE, smt.ZERO) for guard, _value in guarded_values),
        sum_type=output_type,
        count_type="Uint64",
        output_type=output_type,
        output_nullable=True,
        finite_abs_bound=sum(
            aggregate.decimal_finite_abs_bound(value) for _guard, value in guarded_values
        ),
        count_bound=len(guarded_values),
        carry_state=False,
        operation="Decimal window avg",
    )


def rows_prefix_values(
    kind: str,
    output_type: str,
    values: tuple[Value, ...],
    present: tuple[smt.Term, ...],
    partitions: tuple[Value, ...],
    ordinals: tuple[smt.Term, ...],
    not_distinct: ValueComparison,
) -> tuple[Value, ...]:
    """SUM/MAX over ROWS UNBOUNDED PRECEDING .. CURRENT ROW.

    The current row is included by ordinal <=, not by comparing sort-key values:
    equal-key peers can have different frames in an unstable sort.
    """

    result: list[Value] = []
    for candidate_index, candidate_partition in enumerate(partitions):
        guarded_values = tuple(
            (
                smt.and_(
                    row_present,
                    not_distinct(candidate_partition, partition),
                    smt.not_(smt.lt(ordinals[candidate_index], ordinals[row_index])),
                    smt.not_(value.is_null),
                ),
                value,
            )
            for row_index, (row_present, partition, value) in enumerate(
                zip(present, partitions, values)
            )
        )
        if kind == "window_rows_sum":
            value = aggregate.decimal_sum(
                guarded_values, output_type, True, "q51 running Decimal SUM",
            )
        else:
            guards = tuple(guard for guard, _value in guarded_values)
            value = Value(
                output_type,
                smt.not_(smt.or_(*guards)),
                decimal.aggregate_max(
                    tuple((guard, item.value) for guard, item in guarded_values)
                ),
                decimal_finite_abs_bound=max(
                    (
                        aggregate.decimal_finite_abs_bound(item)
                        for _guard, item in guarded_values
                    ),
                    default=0,
                ),
            )
        result.append(value)
    return tuple(result)
