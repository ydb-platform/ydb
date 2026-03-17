# -*- coding: utf-8 -*-
import pytest

from validators import ipv4, ipv6, ValidationFailure


@pytest.mark.parametrize(('address',), [
    ('127.0.0.1',),
    ('123.5.77.88',),
    ('12.12.12.12',),
])
def test_returns_true_on_valid_ipv4_address(address):
    assert ipv4(address)
    assert not ipv6(address)


@pytest.mark.parametrize(('address',), [
    ('abc.0.0.1',),
    ('1278.0.0.1',),
    ('127.0.0.abc',),
    ('900.200.100.75',),
])
def test_returns_failed_validation_on_invalid_ipv4_address(address):
    assert isinstance(ipv4(address), ValidationFailure)
