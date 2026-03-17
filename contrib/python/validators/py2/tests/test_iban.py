# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize('value', [
    'GB82WEST12345698765432',
    'NO9386011117947'
])
def test_returns_true_on_valid_iban(value):
    assert validators.iban(value)


@pytest.mark.parametrize('value', [
    'GB81WEST12345698765432',
    'NO9186011117947'
])
def test_returns_failed_validation_on_invalid_iban(value):
    result = validators.iban(value)
    assert isinstance(result, validators.ValidationFailure)
