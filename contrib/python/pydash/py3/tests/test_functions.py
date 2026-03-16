import time
from unittest import mock

import pytest

import pydash as _


parametrize = pytest.mark.parametrize


@parametrize(
    "case,expected",
    [
        ((lambda: 3, 2), 3),
        ((lambda: 3, -1), 3),
    ],
)
def test_after(case, expected):
    done = _.after(*case)

    for _x in range(case[1] - 1):
        ret = done()
        assert ret is None

    ret = done()
    assert ret == expected


@parametrize(
    "case,args,kwargs,expected",
    [
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, 1), (1, 2, 3, 4), {}, 1),
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, 2), (1, 2, 3, 4), {}, 3),
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, 3), (1, 2, 3, 4), {}, 6),
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, 1), (1, 2, 3, 4), {"d": 10}, 11),
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, 2), (1, 2, 3, 4), {"d": 10}, 13),
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, 3), (1, 2, 3, 4), {"d": 10}, 16),
        ((lambda a=0, b=0, c=0, d=0: a + b + c + d, None), (1, 2, 3, 4), {}, 10),
    ],
)
def test_ary(case, args, kwargs, expected):
    assert _.ary(*case)(*args, **kwargs) == expected


@parametrize(
    "case,expected",
    [
        ((lambda: 3, 2), 3),
        ((lambda: 3, -1), 3),
    ],
)
def test_before(case, expected):
    done = _.before(*case)

    for _x in range(case[1] - 1):
        ret = done()
        assert ret == expected

    ret = done()
    assert ret is None


@parametrize(
    "case,arg,expected",
    [
        ((_.is_boolean, _.is_empty), [False, True], True),
        ((_.is_boolean, _.is_empty), [False, None], False),
    ],
)
def test_conjoin(case, arg, expected):
    assert _.conjoin(*case)(arg) == expected


@parametrize(
    "case,arglist,expected",
    [
        ((lambda a, b, c: [a, b, c],), [(1, 2, 3)], [1, 2, 3]),
        ((lambda a, b, c: [a, b, c],), [(1, 2), (3,)], [1, 2, 3]),
        ((lambda a, b, c: [a, b, c],), [(1,), (2,), (3,)], [1, 2, 3]),
        ((lambda *a: sum(a), 3), [(1, 1, 1)], 3),
        ((lambda *a: sum(a), 3), [(1,), (1,), (1,)], 3),
    ],
)
def test_curry(case, arglist, expected):
    curried = _.curry(*case)

    # Run test twice to verify curried can be reused
    for _x in range(2):
        ret = curried
        for args in arglist:
            ret = ret(*args)

        assert ret == expected


def test_curry_arity_max_from_func():
    def func(data, accum, id):
        accum[id] = _.reduce_(data, lambda total, n: total + n)
        return accum

    ids = [1]
    data = [1, 2]

    curried_func_with_data = _.curry(func)(data)
    result = _.reduce_(ids, curried_func_with_data, {})

    assert result == {1: 3}


@parametrize(
    "case,arglist,expected",
    [
        ((lambda a, b, c: [a, b, c],), [(1, 2, 3)], [1, 2, 3]),
        ((lambda a, b, c: [a, b, c],), [(2, 3), (1,)], [1, 2, 3]),
        ((lambda a, b, c: [a, b, c],), [(3,), (2,), (1,)], [1, 2, 3]),
        ((lambda *a: sum(a), 3), [(1, 1, 1)], 3),
        ((lambda *a: sum(a), 3), [(1,), (1,), (1,)], 3),
    ],
)
def test_curry_right(case, arglist, expected):
    curried = _.curry_right(*case)

    # Run test twice to verify curried can be reused
    for _x in range(2):
        ret = curried
        for args in arglist:
            ret = ret(*args)

        assert ret == expected


def test_debounce():
    def func():
        return _.now()

    wait = 250
    debounced = _.debounce(func, wait)

    start = _.now()
    present = _.now()

    expected = debounced()

    while (present - start) <= wait + 100:
        result = debounced()
        present = _.now()

    assert result == expected

    time.sleep(wait / 1000.0)
    result = debounced()

    assert result > expected


def test_debounce_max_wait():
    def func():
        return _.now()

    wait = 250
    max_wait = 300
    debounced = _.debounce(func, wait, max_wait=max_wait)

    start = _.now()
    present = _.now()

    expected = debounced()

    while (present - start) <= (max_wait + 5):
        result = debounced()
        present = _.now()

    assert result > expected


@parametrize(
    "func,wait,args,kwargs,expected",
    [(lambda a, b, c: (a, b, c), 250, (1, 2), {"c": 3}, (1, 2, 3))],
)
def test_delay(mock_sleep, func, wait, args, kwargs, expected):
    result = _.delay(func, wait, *args, **kwargs)
    assert result == expected
    assert mock_sleep.call_args_list == [mock.call(pytest.approx(wait / 1000.0))]


@parametrize(
    "case,arg,expected",
    [
        ((_.is_boolean, _.is_empty), [False, True], True),
        ((_.is_boolean, _.is_empty), [False, None], True),
        ((_.is_string, _.is_number), ["one", 1, "two", 2], True),
        ((_.is_string, _.is_number), [True, False, None, []], False),
    ],
)
def test_disjoin(case, arg, expected):
    assert _.disjoin(*case)(arg) == expected


@parametrize(
    "case,args,expected",
    [
        (lambda args: args, (1, 2, 3), (3, 2, 1)),
        (lambda args: [i * 2 for i in args], (1, 2, 3), [6, 4, 2]),
    ],
)
def flip(case, args, expected):
    func = _.flip(case)
    assert func(args) == expected


@parametrize(
    "case,args,expected",
    [
        ((lambda x: "!!!" + x + "!!!", lambda x: f"Hi {x}"), ("Bob",), "Hi !!!Bob!!!"),
        ((lambda x: x + x, lambda x: x * x), (5,), 100),
    ],
)
def test_flow(case, args, expected):
    assert _.flow(*case)(*args) == expected


@parametrize(
    "case,args,expected",
    [
        ((lambda x: f"Hi {x}", lambda x: "!!!" + x + "!!!"), ("Bob",), "Hi !!!Bob!!!"),
        ((lambda x: x + x, lambda x: x * x), (5,), 50),
    ],
)
def test_flow_right(case, args, expected):
    assert _.flow_right(*case)(*args) == expected


@parametrize(
    "func,args,expected",
    [
        (lambda x: x + x, (2, 0), 2),
        (lambda x: x + x, (2, 1), 4),
        (lambda x: x + x, (2, 2), 8),
        (lambda x: x + x, (2, 3), 16),
    ],
)
def test_iterated(func, args, expected):
    assert _.iterated(func)(*args) == expected


@parametrize(
    "funcs,args,expected",
    [
        ((lambda a: a[0], lambda a: a[-1]), ("Foobar",), ["F", "r"]),
        (
            (lambda a, b: a[0] + b[-1], lambda a, b: a[-1] + b[0]),
            ("Foobar", "Barbaz"),
            ["Fz", "rB"],
        ),
    ],
)
def test_juxtapose(funcs, args, expected):
    assert _.juxtapose(*funcs)(*args) == expected


@parametrize(
    "func,args",
    [
        (lambda item: item, (True,)),
        (lambda item: item, (False,)),
    ],
)
def test_negate(func, args):
    assert _.negate(func)(*args) == (not func(*args))


@parametrize("case,arglist,expected", [(lambda a: a * a, [(2,), (4,)], 4)])
def test_once(case, arglist, expected):
    fn = _.once(case)
    for args in arglist:
        assert fn(*args) == expected


@parametrize(
    "func,transforms,args,expected",
    [
        (lambda a, b: [a, b], [lambda x: x**2, lambda x: x * 2], (5, 10), [25, 20]),
        (lambda a, b: [a, b], ([lambda x: x**2, lambda x: x * 2],), (5, 10), [25, 20]),
    ],
)
def test_over_args(func, transforms, args, expected):
    assert _.over_args(func, *transforms)(*args) == expected


@parametrize(
    "case,case_args,case_kwargs,args,expected",
    [
        (lambda a, b, c: a + b + c, ("a", "b"), {}, ("c",), "abc"),
        (lambda a, b, c: a + b + c, ("a",), {"c": "d"}, ("b",), "abd"),
    ],
)
def test_partial(case, case_args, case_kwargs, args, expected):
    assert _.partial(case, *case_args, **case_kwargs)(*args) == expected


def test_partial_as_iteratee():
    func = _.partial(lambda offset, value, *args: value + offset, 5)
    case = [1, 2, 3]
    expected = [6, 7, 8]
    assert _.map_(case, func) == expected


@parametrize(
    "case,case_args,case_kwargs,args,expected",
    [
        (lambda a, b, c: a + b + c, ("a", "b"), {}, ("c",), "cab"),
        (lambda a, b, c: a + b + c, ("a",), {"c": "d"}, ("b",), "bad"),
    ],
)
def test_partial_right(case, case_args, case_kwargs, args, expected):
    assert _.partial_right(case, *case_args, **case_kwargs)(*args) == expected


@parametrize(
    "case,args,kwargs,expected",
    [
        ((lambda a, b, c: [a, b, c], 2, 0, 1), ("b", "c", "a"), {}, ["a", "b", "c"]),
        ((lambda a, b, c: [a, b, c], [2, 0, 1]), ("b", "c", "a"), {}, ["a", "b", "c"]),
        ((lambda a, b, c: [a, b, c], 2, 1), ("b", "c", "a"), {}, ["a", "c", "b"]),
        ((lambda a, b, c: [a, b, c], 1), ("b", "c", "a"), {}, ["c", "b", "a"]),
        ((lambda a, b, c: [a, b, c], 3, 2, 0, 1), ("b", "c", "a"), {}, ["a", "b", "c"]),
    ],
)
def test_rearg(case, args, kwargs, expected):
    assert _.rearg(*case)(*args, **kwargs) == expected


@parametrize(
    "case,args,expected",
    [
        (lambda *args: args, ["a", "b", "c"], ("a", "b", "c")),
        (lambda *args: ",".join(args), ["a", "b", "c"], "a,b,c"),
        (lambda a, b, c: f"{a} {b} {c}", [1, 2, 3], "1 2 3"),
    ],
)
def test_spread(case, args, expected):
    assert _.spread(case)(args) == expected


def test_throttle():
    def func():
        return _.now()

    wait = 250
    throttled = _.throttle(func, wait)

    start = _.now()
    present = _.now()

    expected = throttled()

    while (present - start) < (wait - 50):
        result = throttled()
        present = _.now()

    assert result == expected

    time.sleep(100 / 1000.0)
    assert throttled() > expected


@parametrize(
    "case,args,kwargs,expected",
    [
        (lambda a=0, b=0, c=0, d=0: a + b + c + d, (1, 2, 3, 4), {}, 1),
        (lambda a=0, b=0, c=0, d=0: a + b + c + d, (1, 2, 3, 4), {"d": 10}, 11),
    ],
)
def test_unary(case, args, kwargs, expected):
    assert _.unary(case)(*args, **kwargs) == expected


@parametrize(
    "case,args,expected",
    [
        (
            (lambda a: a.strip(), lambda func, text: f"<p>{func(text)}</p>"),
            ("  hello world!  ",),
            "<p>hello world!</p>",
        )
    ],
)
def test_wrap(case, args, expected):
    assert _.wrap(*case)(*args) == expected


def test_flow_argcount():
    assert _.flow(lambda x, y: x + y, lambda x: x * 2)._argcount == 2


def test_flow_right_argcount():
    assert _.flow_right(lambda x: x * 2, lambda x, y: x + y)._argcount == 2


def test_juxtapose_argcount():
    assert _.juxtapose(lambda x, y, z: x + y + z, lambda x, y, z: x * y * z)._argcount == 3


def test_partial_argcount():
    assert _.partial(lambda x, y, z: x + y + z, 1, 2)._argcount == 1


def test_partial_right_argcount():
    assert _.partial_right(lambda x, y, z: x + y + z, 1, 2)._argcount == 1


def test_curry_argcount():
    assert _.curry(lambda x, y, z: x + y + z)(1)._argcount == 2


def test_curry_right_argcount():
    assert _.curry_right(lambda x, y, z: x + y + z)(1)._argcount == 2


def test_can_be_used_as_predicate_argcount_is_known():
    def is_positive(x: int) -> bool:
        return x > 0

    assert _.filter_([-1, 0, 1], _.negate(is_positive)) == [-1, 0]
