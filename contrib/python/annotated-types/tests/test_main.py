import math
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Iterator, Type, Union

if sys.version_info < (3, 9):
    from typing_extensions import Annotated, get_args, get_origin
else:
    from typing import Annotated, get_args, get_origin

import pytest

if TYPE_CHECKING:
    from _pytest.mark import ParameterSet

import annotated_types
from annotated_types.test_cases import Case, cases

Constraint = Union[annotated_types.BaseMetadata, slice]


def check_gt(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Gt)
    return val > constraint.gt


def check_lt(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Lt)
    return val < constraint.lt


def check_ge(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Ge)
    return val >= constraint.ge


def check_le(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Le)
    return val <= constraint.le


def check_multiple_of(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.MultipleOf)
    return val % constraint.multiple_of == 0


def check_min_len(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.MinLen)
    return len(val) >= constraint.min_length


def check_max_len(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.MaxLen)
    return len(val) <= constraint.max_length


def check_predicate(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Predicate)
    # this is a relatively pointless branch since Not is itself callable
    # but it serves to demonstrate that Not can be introspected
    # and the wrapped predicate can be extracted / matched
    if isinstance(constraint.func, annotated_types.Not):
        return not constraint.func.func(val)
    return constraint.func(val)


def check_timezone(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Timezone)
    assert isinstance(val, datetime)
    if isinstance(constraint.tz, str):
        return val.tzinfo is not None and constraint.tz == val.tzname()
    elif isinstance(constraint.tz, timezone):
        return val.tzinfo is not None and val.tzinfo == constraint.tz
    elif constraint.tz is None:
        return val.tzinfo is None
    # ellipsis
    return val.tzinfo is not None


def check_quantity(constraint: Constraint, val: Any) -> bool:
    assert isinstance(constraint, annotated_types.Unit)
    return isinstance(val, (float, int))


Validator = Callable[[Constraint, Any], bool]


VALIDATORS: Dict[Type[Constraint], Validator] = {
    annotated_types.Gt: check_gt,
    annotated_types.Lt: check_lt,
    annotated_types.Ge: check_ge,
    annotated_types.Le: check_le,
    annotated_types.MultipleOf: check_multiple_of,
    annotated_types.Predicate: check_predicate,
    annotated_types.MinLen: check_min_len,
    annotated_types.MaxLen: check_max_len,
    annotated_types.Timezone: check_timezone,
    annotated_types.Unit: check_quantity,
}


def get_constraints(tp: type) -> Iterator[Constraint]:
    origin = get_origin(tp)
    assert origin is Annotated
    args = iter(get_args(tp))
    next(args)
    for arg in args:
        if isinstance(arg, annotated_types.BaseMetadata):
            yield arg
        elif isinstance(arg, annotated_types.GroupedMetadata):
            yield from arg  # type: ignore
        elif isinstance(arg, slice):
            yield from annotated_types.Len(arg.start or 0, arg.stop)


def is_valid(tp: type, value: Any) -> bool:
    for constraint in get_constraints(tp):
        if not VALIDATORS[type(constraint)](constraint, value):
            return False
    return True


def extract_valid_testcases(case: Case) -> "Iterable[ParameterSet]":
    for example in case.valid_cases:
        yield pytest.param(case.annotation, example, id=f"{case.annotation} is valid for {repr(example)}")


def extract_invalid_testcases(case: Case) -> "Iterable[ParameterSet]":
    for example in case.invalid_cases:
        yield pytest.param(case.annotation, example, id=f"{case.annotation} is invalid for {repr(example)}")


@pytest.mark.parametrize(
    "annotation, example", [testcase for case in cases() for testcase in extract_valid_testcases(case)]
)
def test_valid_cases(annotation: type, example: Any) -> None:
    assert is_valid(annotation, example) is True


@pytest.mark.parametrize(
    "annotation, example", [testcase for case in cases() for testcase in extract_invalid_testcases(case)]
)
def test_invalid_cases(annotation: type, example: Any) -> None:
    assert is_valid(annotation, example) is False


def a_predicate_fn(x: object) -> bool:
    return not x


@pytest.mark.parametrize(
    "pred, repr_",
    [
        (annotated_types.Predicate(func=a_predicate_fn), "Predicate(a_predicate_fn)"),
        (annotated_types.Predicate(func=str.isascii), "Predicate(str.isascii)"),
        (annotated_types.Predicate(func=math.isfinite), "Predicate(math.isfinite)"),
        (annotated_types.Predicate(func=bool), "Predicate(bool)"),
        (annotated_types.Predicate(func := lambda _: True), f"Predicate({func!r})"),
    ],
)
def test_predicate_repr(pred: annotated_types.Predicate, repr_: str) -> None:
    assert repr(pred) == repr_
