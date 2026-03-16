import attr
import pytest

import grafanalib.validators as validators


def create_attribute():
    return attr.Attribute(
        name='x',
        default=None,
        validator=None,
        repr=True,
        cmp=None,
        eq=True,
        order=False,
        hash=True,
        init=True,
        inherited=False)


def test_is_in():
    item = 1
    choices = (1, 2, 3)
    val = validators.is_in(choices)
    res = val(None, create_attribute(), item)
    assert res is None


def test_is_in_raises():
    item = 0
    choices = (1, 2, 3)
    val = validators.is_in(choices)
    with pytest.raises(ValueError):
        val(None, create_attribute(), item)


@pytest.mark.parametrize("item", (
    '24h', '7d', '1M', '+24h', '-24h', '60s', '2m'))
def test_is_interval(item):
    assert validators.is_interval(None, create_attribute(), item) is None


def test_is_interval_raises():
    with pytest.raises(ValueError):
        validators.is_interval(None, create_attribute(), '1')


@pytest.mark.parametrize("color", (
    "#111111", "#ffffff"))
def test_is_color_code(color):
    res = validators.is_color_code(None, create_attribute(), color)
    assert res is None


@pytest.mark.parametrize("color", (
    "111111", "#gggggg", "#1111111", "#11111"))
def test_is_color_code_raises(color):
    with pytest.raises(ValueError):
        validators.is_color_code(None, create_attribute(), color)


def test_list_of():
    etype = int
    check = (1, 2, 3)
    val = validators.is_list_of(etype)
    res = val(None, create_attribute(), check)
    assert res is None


def test_list_of_raises():
    etype = int
    check = ("a")
    with pytest.raises(ValueError):
        val = validators.is_list_of(etype)
        val(None, create_attribute(), check)
