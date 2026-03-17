# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize('value', [
    'd41d8cd98f00b204e9800998ecf8427e',
    'D41D8CD98F00B204E9800998ECF8427E'
])
def test_returns_true_on_valid_md5(value):
    assert validators.md5(value)


@pytest.mark.parametrize('value', [
    'z41d8cd98f00b204e9800998ecf8427e',
    'z8cd98f00b204e9800998ecf8427e',
    'z4aaaa1d8cd98f00b204e9800998ecf8427e'
])
def test_returns_failed_validation_on_invalid_md5(value):
    result = validators.md5(value)
    assert isinstance(result, validators.ValidationFailure)
