# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize('value', [
    'da39a3ee5e6b4b0d3255bfef95601890afd80709',
    'DA39A3EE5E6B4B0D3255BFEF95601890AFD80709'
])
def test_returns_true_on_valid_sha1(value):
    assert validators.sha1(value)


@pytest.mark.parametrize('value', [
    'za39a3ee5e6b4b0d3255bfef95601890afd80709',
    'da39e5e6b4b0d3255bfef95601890afd80709',
    'daaaa39a3ee5e6b4b0d3255bfef95601890afd80709'
])
def test_returns_failed_validation_on_invalid_sha1(value):
    result = validators.sha1(value)
    assert isinstance(result, validators.ValidationFailure)
