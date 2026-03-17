# -*- coding: utf-8 -*-
import pytest

from validators import Max, Min


@pytest.mark.parametrize(('value',), [
    (None,),
    ('',),
    (12,),
    (Min,),
])
def test_max_is_greater_than_every_other_value(value):
    assert value < Max
    assert Max > value


def test_max_is_not_greater_than_itself():
    assert not (Max < Max)


def test_other_comparison_methods_for_max():
    assert Max <= Max
    assert Max == Max
    assert not (Max != Max)


@pytest.mark.parametrize(('value',), [
    (None,),
    ('',),
    (12,),
    (Max,),
])
def test_min_is_smaller_than_every_other_value(value):
    assert value > Min


def test_min_is_not_greater_than_itself():
    assert not (Min < Min)


def test_other_comparison_methods_for_min():
    assert Min <= Min
    assert Min == Min
    assert not (Min != Min)
