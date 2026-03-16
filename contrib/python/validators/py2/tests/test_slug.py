# -*- coding: utf-8 -*-
import pytest

from validators import slug, ValidationFailure


@pytest.mark.parametrize('value', [
    '123-12312-asdasda',
    '123____123',
    'dsadasd-dsadas',
])
def test_returns_true_on_valid_slug(value):
    assert slug(value)


@pytest.mark.parametrize('value', [
    'some.slug',
    '1231321%',
    '   21312',
    '123asda&',
])
def test_returns_failed_validation_on_invalid_slug(value):
    assert isinstance(slug(value), ValidationFailure)
