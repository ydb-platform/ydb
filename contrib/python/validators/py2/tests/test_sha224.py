# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize('value', [
    'd14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f',
    'D14A028C2A3A2BC9476102BB288234C415A2B01F828EA62AC5B3E42F'
])
def test_returns_true_on_valid_sha224(value):
    assert validators.sha224(value)


@pytest.mark.parametrize('value', [
    'z14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f',
    'd028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f',
    'daaa14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f'
])
def test_returns_failed_validation_on_invalid_sha224(value):
    result = validators.sha224(value)
    assert isinstance(result, validators.ValidationFailure)
