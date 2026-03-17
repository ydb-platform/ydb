# -*- coding: utf-8 -*-

from copy import deepcopy

import pydash as _

from .fixtures import parametrize


pydash_methods = _.filter_(dir(_), lambda m: callable(getattr(_, m, None)))


def test_chaining_methods():
    chain = _.chain([])

    for method in dir(_):
        if not callable(method):
            continue

        chained = getattr(chain, method)
        assert chained.method is getattr(_, method)


@parametrize(
    "value,methods", [([1, 2, 3, 4], [("without", (2, 3)), ("reject", (lambda x: x > 1,))])]
)
def test_chaining(value, methods):
    expected = deepcopy(value)
    actual = _.chain(deepcopy(value))

    for method, args in methods:
        expected = getattr(_, method)(expected, *args)
        actual = getattr(actual, method)(*args)

    assert actual.value() == expected


def test_chaining_invalid_method():
    raised = False

    try:
        _.chain([]).foobar
    except _.InvalidMethod:
        raised = True

    assert raised


def test_chaining_lazy():
    tracker = {"called": False}

    def interceptor(value):
        tracker["called"] = True
        return value.pop()

    chain = _.chain([1, 2, 3, 4, 5]).initial().tap(interceptor)

    assert not tracker["called"]

    chain = chain.last()

    assert not tracker["called"]

    result = chain.value()

    assert tracker["called"]
    assert result == 3


def test_chaining_late_value():
    square_sum = _.chain().power(2).sum()
    assert square_sum([1, 2, 3, 4]) == 30


def test_chaining_late_value_reuse():
    square_sum = _.chain().power(2).sum()
    assert square_sum([1, 2, 3, 4]) == 30
    assert square_sum([2]) == 4


def test_chaining_late_value_override():
    square_sum = _.chain([1, 2, 3, 4]).power(2).sum()
    assert square_sum([5, 6, 7, 8]) == 174


def test_chaining_plant():
    value = [1, 2, 3, 4]
    square_sum1 = _.chain(value).power(2).sum()

    def root_value(wrapper):
        if isinstance(wrapper._value, _.chaining.ChainWrapper):
            return root_value(wrapper._value)
        return wrapper._value

    assert root_value(square_sum1._value) == value

    test_value = [5, 6, 7, 8]
    square_sum2 = square_sum1.plant(test_value)

    assert root_value(square_sum1._value) == value
    assert root_value(square_sum2._value) == test_value

    assert square_sum1.value() == 30
    assert square_sum2.value() == 174


def test_chaining_commit():
    chain = _.chain([1, 2, 3, 4]).power(2).sum()
    committed = chain.commit()

    assert chain is not committed
    assert chain.value() == committed.value()


def test_dash_instance_chaining():
    value = [1, 2, 3, 4]
    from__ = _._(value).without(2, 3).reject(lambda x: x > 1)
    from_chain = _.chain(value).without(2, 3).reject(lambda x: x > 1)

    assert from__.value() == from_chain.value()


def test_dash_instance_methods():
    assert pydash_methods

    for method in pydash_methods:
        assert getattr(_._, method) is getattr(_, method)


def test_dash_suffixed_method_aliases():
    methods = _.filter_(pydash_methods, lambda m: m.endswith("_"))
    assert methods

    for method in methods:
        assert getattr(_._, method[:-1]) is getattr(_, method)


def test_dash_method_call():
    value = [1, 2, 3, 4, 5]
    assert _._.initial(value) == _.initial(value)


def test_dash_alias():
    assert _.py_ is _._


@parametrize(
    "case,expected",
    [
        ([1, 2, 3], "[1, 2, 3]"),
    ],
)
def test_chaining_value_to_string(case, expected):
    assert _.chain(case).to_string() == expected


@parametrize("value,interceptor,expected", [([1, 2, 3, 4, 5], lambda value: value.pop(), 3)])
def test_tap(value, interceptor, expected):
    actual = _.chain(value).initial().tap(interceptor).last().value()
    assert actual == expected


@parametrize("value,func,expected", [([1, 2, 3, 4, 5], lambda value: [sum(value)], 10)])
def test_thru(value, func, expected):
    assert _.chain(value).initial().thru(func).last().value()
