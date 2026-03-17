"""Test suite prance.util.exceptions."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2018 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()

import pytest

from prance.util import exceptions
from prance import ValidationError


def test_reraise_without_value_no_extra_message():
    with pytest.raises(ValidationError) as caught:
        exceptions.raise_from(ValidationError, None)

    # The first is obvious from pytest.raises. The rest tests
    # known attributes
    assert caught.type == ValidationError
    assert str(caught.value) == ""


def test_reraise_without_value_extra_message():
    with pytest.raises(ValidationError) as caught:
        exceptions.raise_from(ValidationError, None, "asdf")

    # The first is obvious from pytest.raises. The rest tests
    # known attributes
    assert caught.type == ValidationError
    assert str(caught.value) == "asdf"


def test_reraise_with_value_no_extra_message():
    with pytest.raises(ValidationError) as caught:
        try:
            raise RuntimeError("foo")
        except RuntimeError as inner:
            exceptions.raise_from(ValidationError, inner)

    # The first is obvious from pytest.raises. The rest tests
    # known attributes
    assert caught.type == ValidationError
    assert str(caught.value) == "foo"


def test_reraise_with_value_extra_message():
    with pytest.raises(ValidationError) as caught:
        try:
            raise RuntimeError("foo")
        except RuntimeError as inner:
            exceptions.raise_from(ValidationError, inner, "asdf")

    # The first is obvious from pytest.raises. The rest tests
    # known attributes
    assert caught.type == ValidationError
    assert str(caught.value) == "foo -- asdf"


def test_reraise_with_empty_value_string_extra_message():
    with pytest.raises(ValidationError) as caught:
        try:
            raise RuntimeError()
        except RuntimeError as inner:
            exceptions.raise_from(ValidationError, inner, "asdf")

    # The first is obvious from pytest.raises. The rest tests
    # known attributes
    assert caught.type == ValidationError
    assert str(caught.value) == "asdf"
