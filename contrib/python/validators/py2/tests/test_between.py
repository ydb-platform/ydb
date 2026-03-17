# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize(('value', 'min', 'max'), [
    (12, 11, 13),
    (12, None, 14),
    (12, 11, None),
    (12, 12, 12)
])
def test_returns_true_on_valid_range(value, min, max):
    assert validators.between(value, min=min, max=max)


@pytest.mark.parametrize(('value', 'min', 'max'), [
    (12, 13, 12),
    (12, None, None),
])
def test_raises_assertion_error_for_invalid_args(value, min, max):
    with pytest.raises(AssertionError):
        assert validators.between(value, min=min, max=max)


@pytest.mark.parametrize(('value', 'min', 'max'), [
    (12, 13, 14),
    (12, None, 11),
    (12, 13, None)
])
def test_returns_failed_validation_on_invalid_range(value, min, max):
    result = validators.between(value, min=min, max=max)
    assert isinstance(result, validators.ValidationFailure)
