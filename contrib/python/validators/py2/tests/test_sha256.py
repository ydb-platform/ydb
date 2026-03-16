# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize('value', [
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    'E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855'
])
def test_returns_true_on_valid_sha256(value):
    assert validators.sha256(value)


@pytest.mark.parametrize('value', [
    'z3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    'ec44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    'eaaaa3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
])
def test_returns_failed_validation_on_invalid_sha256(value):
    result = validators.sha256(value)
    assert isinstance(result, validators.ValidationFailure)
