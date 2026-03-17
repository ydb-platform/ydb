# -*- coding: utf-8 -*-
import pytest

from validators import ipv4_cidr, ipv6_cidr, ValidationFailure


@pytest.mark.parametrize(('cidr',), [
    ('::1/0',),
    ('dead:beef:0:0:0:0:42:1/8',),
    ('abcd:ef::42:1/32',),
    ('0:0:0:0:0:ffff:1.2.3.4/64',),
    ('::192.168.30.2/128',),
])
def test_returns_true_on_valid_ipv6_cidr(cidr):
    assert ipv6_cidr(cidr)
    assert not ipv4_cidr(cidr)


@pytest.mark.parametrize(('cidr',), [
    ('abc.0.0.1',),
    ('abcd:1234::123::1',),
    ('1:2:3:4:5:6:7:8:9',),
    ('abcd::1ffff',),
    ('1.1.1.1',),
    ('::1',),
    ('::1/129',),
    ('::1/-1',),
    ('::1/foo',),
])
def test_returns_failed_validation_on_invalid_ipv6_cidr(cidr):
    assert isinstance(ipv6_cidr(cidr), ValidationFailure)
