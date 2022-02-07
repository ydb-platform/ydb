#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'asatarin@yandex-team.ru'


class TestException(RuntimeError):
    def __init__(self, *args, **kwargs):
        super(TestException, self).__init__(*args, **kwargs)
        self.prop = "property"


def raises_exception():
    raise TestException()


def returns_value():
    return 'my_return_value'


def test_raises():
    """
    >>> from hamcrest import assert_that
    >>> from hamcrest import has_property
    >>> from hamcrest import not_, raises
    >>> raises(TestException).matches(raises_exception)
    True
    >>> raises(TestException, matcher=has_property("prop", "property")).matches(raises_exception)
    True
    >>> raises(TestException, matcher=has_property("prop", "fail")).matches(raises_exception)
    False
    >>> raises(TestException, matcher=not_(has_property("prop", "fail"))).matches(raises_exception)
    True
    >>> raises(TestException, matcher=not_(has_property("prop", "property"))).matches(raises_exception)
    False

    >>> assert_that(returns_value, raises(TestException), 'message')
    Traceback (most recent call last):
      ...
    AssertionError: message
    Expected: Expected a callable raising <class '__tests__.test_raises.TestException'>
         but: No exception raised and actual return value = 'my_return_value'
    <BLANKLINE>
    """
