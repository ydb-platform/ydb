# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize(('value', 'min', 'max'), [
    ('password', 2, 10),
    ('password', None, 10),
    ('password', 2, None),
    ('password', 8, 8)
])
def test_returns_true_on_valid_length(value, min, max):
    assert validators.length(value, min=min, max=max)


@pytest.mark.parametrize(('value', 'min', 'max'), [
    ('something', 13, 12),
    ('something', -1, None),
    ('something', -1, None),
    ('something', -3, -2)
])
def test_raises_assertion_error_for_invalid_args(value, min, max):
    with pytest.raises(AssertionError):
        assert validators.length(value, min=min, max=max)


@pytest.mark.parametrize(('value', 'min', 'max'), [
    ('something', 13, 14),
    ('something', None, 6),
    ('something', 13, None)
])
def test_returns_failed_validation_on_invalid_range(value, min, max):
    assert isinstance(
        validators.length(value, min=min, max=max),
        validators.ValidationFailure
    )
