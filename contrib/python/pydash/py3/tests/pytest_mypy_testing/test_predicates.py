import typing as t

import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_eq() -> None:
    reveal_type(_.eq(None, None))  # R: builtins.bool
    reveal_type(_.eq(None, ''))  # R: builtins.bool
    reveal_type(_.eq('a', 'a'))  # R: builtins.bool
    reveal_type(_.eq(1, str(1)))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_gt() -> None:
    reveal_type(_.gt(5, 3))  # R: builtins.bool
    # _.gt({}, {})  # E: Argument 1 to "gt" has incompatible type "Dict[<nothing>, <nothing>]"; expected "SupportsDunderGT[Dict[<nothing>, <nothing>]]"


@pytest.mark.mypy_testing
def test_mypy_gte() -> None:
    reveal_type(_.gte(5, 3))  # R: builtins.bool
    # _.gte({}, {})  # E: Argument 1 to "gte" has incompatible type "Dict[<nothing>, <nothing>]"; expected "SupportsDunderGE[Dict[<nothing>, <nothing>]]"


@pytest.mark.mypy_testing
def test_mypy_lt() -> None:
    reveal_type(_.lt(5, 3))  # R: builtins.bool
    # _.lt({}, {})  # E: Argument 1 to "lt" has incompatible type "Dict[<nothing>, <nothing>]"; expected "SupportsDunderLT[Dict[<nothing>, <nothing>]]"


@pytest.mark.mypy_testing
def test_mypy_lte() -> None:
    reveal_type(_.lte(5, 3))  # R: builtins.bool
    # _.lte({}, {})  # E: Argument 1 to "lte" has incompatible type "Dict[<nothing>, <nothing>]"; expected "SupportsDunderLE[Dict[<nothing>, <nothing>]]"


@pytest.mark.mypy_testing
def test_mypy_in_range() -> None:
    reveal_type(_.in_range(4, 2))  # R: builtins.bool
    reveal_type(_.in_range(3, 1, 2))  # R: builtins.bool
    reveal_type(_.in_range(3.5, 2.5))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_associative() -> None:
    reveal_type(_.is_associative([]))  # R: builtins.bool
    reveal_type(_.is_associative({}))  # R: builtins.bool
    reveal_type(_.is_associative(1))  # R: builtins.bool
    reveal_type(_.is_associative(True))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_blank() -> None:
    reveal_type(_.is_blank(''))  # R: builtins.bool
    reveal_type(_.is_blank(' \r\n '))  # R: builtins.bool
    reveal_type(_.is_blank(False))  # R: builtins.bool

    x: t.Any = ...
    if _.is_blank(x):
        reveal_type(x)  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_is_boolean() -> None:
    reveal_type(_.is_boolean(True))  # R: builtins.bool
    reveal_type(_.is_boolean(False))  # R: builtins.bool
    reveal_type(_.is_boolean(0))  # R: builtins.bool

    x: t.Any = ...
    if _.is_boolean(x):
        reveal_type(x)  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_builtin() -> None:
    reveal_type(_.is_builtin(1))  # R: builtins.bool
    reveal_type(_.is_builtin(list))  # R: builtins.bool
    reveal_type(_.is_builtin('foo'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_date() -> None:
    import datetime
    reveal_type(_.is_date(datetime.date.today()))  # R: builtins.bool
    reveal_type(_.is_date(datetime.datetime.today()))  # R: builtins.bool
    reveal_type(_.is_date('2014-01-01'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_decreasing() -> None:
    reveal_type(_.is_decreasing([5, 4, 4, 3]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_dict() -> None:
    reveal_type(_.is_dict({}))  # R: builtins.bool
    reveal_type(_.is_dict([]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_empty() -> None:
    reveal_type(_.is_empty(0))  # R: builtins.bool
    reveal_type(_.is_empty(1))  # R: builtins.bool
    reveal_type(_.is_empty(True))  # R: builtins.bool
    reveal_type(_.is_empty('foo'))  # R: builtins.bool
    reveal_type(_.is_empty(None))  # R: builtins.bool
    reveal_type(_.is_empty({}))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_equal() -> None:
    reveal_type(_.is_equal([1, 2, 3], [1, 2, 3]))  # R: builtins.bool
    reveal_type(_.is_equal('a', 'A'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_equal_with() -> None:
    reveal_type(_.is_equal_with([1, 2, 3], [1, 2, 3], None))  # R: builtins.bool
    reveal_type(_.is_equal_with('a', 'A', None))  # R: builtins.bool
    reveal_type(_.is_equal_with('a', 'A', lambda a, b: a.lower() == b.lower()))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_error() -> None:
    reveal_type(_.is_error(Exception()))  # R: builtins.bool
    reveal_type(_.is_error(Exception))  # R: builtins.bool
    reveal_type(_.is_error(None))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_even() -> None:
    reveal_type(_.is_even(2))  # R: builtins.bool
    reveal_type(_.is_even(False))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_float() -> None:
    reveal_type(_.is_float(1.0))  # R: builtins.bool
    reveal_type(_.is_float(1))  # R: builtins.bool

    x: t.Any = ...
    if _.is_float(x):
        reveal_type(x)  # R: builtins.float


@pytest.mark.mypy_testing
def test_mypy_is_function() -> None:
    reveal_type(_.is_function(list))  # R: builtins.bool
    reveal_type(_.is_function(lambda: True))  # R: builtins.bool
    reveal_type(_.is_function(1))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_increasing() -> None:
    reveal_type(_.is_increasing([1, 3, 5]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_indexed() -> None:
    reveal_type(_.is_indexed(''))  # R: builtins.bool
    reveal_type(_.is_indexed([]))  # R: builtins.bool
    reveal_type(_.is_indexed(()))  # R: builtins.bool
    reveal_type(_.is_indexed({}))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_instance_of() -> None:
    reveal_type(_.is_instance_of({}, dict))  # R: builtins.bool
    reveal_type(_.is_instance_of({}, list))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_integer() -> None:
    reveal_type(_.is_integer(1))  # R: builtins.bool
    reveal_type(_.is_integer(1.0))  # R: builtins.bool
    reveal_type(_.is_integer(True))  # R: builtins.bool

    x: t.Any = ...
    if _.is_integer(x):
        reveal_type(x)  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_is_iterable() -> None:
    reveal_type(_.is_iterable([]))  # R: builtins.bool
    reveal_type(_.is_iterable({}))  # R: builtins.bool
    reveal_type(_.is_iterable(()))  # R: builtins.bool
    reveal_type(_.is_iterable(5))  # R: builtins.bool
    reveal_type(_.is_iterable(True))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_json() -> None:
    reveal_type(_.is_json({}))  # R: builtins.bool
    reveal_type(_.is_json('{}'))  # R: builtins.bool
    reveal_type(_.is_json({"hello": 1, "world": 2}))  # R: builtins.bool
    reveal_type(_.is_json('{"hello": 1, "world": 2}'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_list() -> None:
    reveal_type(_.is_list([]))  # R: builtins.bool
    reveal_type(_.is_list({}))  # R: builtins.bool
    reveal_type(_.is_list(()))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_match() -> None:
    reveal_type(_.is_match({'a': 1, 'b': 2}, {'b': 2}))  # R: builtins.bool
    reveal_type(_.is_match({'a': 1, 'b': 2}, {'b': 3}))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_match_with() -> None:
    is_greeting = lambda val: val in ('hello', 'hi')
    customizer = lambda ov, sv: is_greeting(ov) and is_greeting(sv)
    obj = {'greeting': 'hello'}
    src = {'greeting': 'hi'}
    reveal_type(_.is_match_with(obj, src, customizer))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_monotone() -> None:
    import operator
    reveal_type(_.is_monotone([1, 1, 2, 3], operator.le))  # R: builtins.bool
    reveal_type(_.is_monotone([1, 1, 2, 3], operator.lt))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_nan() -> None:
    reveal_type(_.is_nan('a'))  # R: builtins.bool
    reveal_type(_.is_nan(1))  # R: builtins.bool
    reveal_type(_.is_nan(1.0))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_negative() -> None:
    reveal_type(_.is_negative(-1))  # R: builtins.bool
    reveal_type(_.is_negative(0))  # R: builtins.bool
    reveal_type(_.is_negative(1))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_none() -> None:
    reveal_type(_.is_none(None))  # R: builtins.bool
    reveal_type(_.is_none(False))  # R: builtins.bool

    x: t.Any = ...
    if _.is_none(x):
        reveal_type(x)  # R: None


@pytest.mark.mypy_testing
def test_mypy_is_number() -> None:
    reveal_type(_.is_number(1))  # R: builtins.bool
    reveal_type(_.is_number(1.0))  # R: builtins.bool
    reveal_type(_.is_number('a'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_object() -> None:
    reveal_type(_.is_object([]))  # R: builtins.bool
    reveal_type(_.is_object({}))  # R: builtins.bool
    reveal_type(_.is_object(()))  # R: builtins.bool
    reveal_type(_.is_object(1))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_odd() -> None:
    reveal_type(_.is_odd(3))  # R: builtins.bool
    reveal_type(_.is_odd('a'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_positive() -> None:
    reveal_type(_.is_positive(1))  # R: builtins.bool
    reveal_type(_.is_positive(0))  # R: builtins.bool
    reveal_type(_.is_positive(-1))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_reg_exp() -> None:
    import re
    reveal_type(_.is_reg_exp(re.compile('')))  # R: builtins.bool
    reveal_type(_.is_reg_exp(''))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_set() -> None:
    reveal_type(_.is_set(set([1, 2])))  # R: builtins.bool
    reveal_type(_.is_set([1, 2, 3]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_strictly_decreasing() -> None:
    reveal_type(_.is_strictly_decreasing([4, 3, 2, 1]))  # R: builtins.bool
    reveal_type(_.is_strictly_decreasing([4, 4, 2, 1]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_strictly_increasing() -> None:
    reveal_type(_.is_strictly_increasing([1, 2, 3, 4]))  # R: builtins.bool
    reveal_type(_.is_strictly_increasing([1, 1, 3, 4]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_string() -> None:
    reveal_type(_.is_string(''))  # R: builtins.bool
    reveal_type(_.is_string(1))  # R: builtins.bool

    x: t.Any = ...
    if _.is_string(x):
        reveal_type(x)  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_is_tuple() -> None:
    reveal_type(_.is_tuple(()))  # R: builtins.bool
    reveal_type(_.is_tuple({}))  # R: builtins.bool
    reveal_type(_.is_tuple([]))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_is_zero() -> None:
    reveal_type(_.is_zero(0))  # R: builtins.bool
    reveal_type(_.is_zero(1))  # R: builtins.bool

    x: t.Any = ...
    if _.is_zero(x):
        reveal_type(x)  # R: builtins.int
