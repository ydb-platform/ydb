# -*- coding: utf-8 -*-
import pytest

from validators import domain, ValidationFailure


@pytest.mark.parametrize('value', [
    'example.com',
    'xn----gtbspbbmkef.xn--p1ai',
    'underscore_subdomain.example.com',
    'something.versicherung',
    'someThing.versicherung',
    '11.com',
    '3.cn',
    'a.cn',
    'sub1.sub2.sample.co.uk',
    'somerandomexample.xn--fiqs8s',
    'kräuter.com',
    'über.com'
])
def test_returns_true_on_valid_domain(value):
    assert domain(value)


@pytest.mark.parametrize('value', [
    'example.com/',
    'example.com:4444',
    'example.-com',
    'example.',
    '-example.com',
    'example-.com',
    '_example.com',
    'example_.com',
    'example',
    'a......b.com',
    'a.123',
    '123.123',
    '123.123.123',
    '123.123.123.123'
])
def test_returns_failed_validation_on_invalid_domain(value):
    assert isinstance(domain(value), ValidationFailure)
