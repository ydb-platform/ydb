# -*- coding: utf-8 -*-
import pytest

from validators import mac_address, ValidationFailure


@pytest.mark.parametrize(('address',), [
    ('01:23:45:67:ab:CD',),
])
def test_returns_true_on_valid_mac_address(address):
    assert mac_address(address)


@pytest.mark.parametrize(('address',), [
    ('00:00:00:00:00',),
    ('01:23:45:67:89:',),
    ('01:23:45:67:89:gh',),
    ('123:23:45:67:89:00',),
])
def test_returns_failed_validation_on_invalid_mac_address(address):
    assert isinstance(mac_address(address), ValidationFailure)
