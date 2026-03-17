# -*- coding: utf-8 -*-
"""
This example shows how to implement a function that accepts two positional
arguments or two keyword arguments. A warning message should be emitted
if `x` and `y` are used instead of `width` and `height`.
"""
import warnings

import pytest

from deprecated.params import deprecated_params


@deprecated_params(
    {
        "x": "use `width` instead or `x`",
        "y": "use `height` instead or `y`",
    },
)
def area(*args, **kwargs):
    def _area_impl(width, height):
        return width * height

    if args:
        # positional arguments (no checking)
        return _area_impl(*args)
    elif set(kwargs) == {"width", "height"}:
        # nominal case: no warning
        return _area_impl(kwargs["width"], kwargs["height"])
    elif set(kwargs) == {"x", "y"}:
        # old case: deprecation warning
        return _area_impl(kwargs["x"], kwargs["y"])
    else:
        raise TypeError("invalid arguments")


@pytest.mark.parametrize(
    "args, kwargs, expected",
    [
        pytest.param((4, 6), {}, [], id="positional arguments: no warning"),
        pytest.param((), {"width": 3, "height": 6}, [], id="correct keyword arguments"),
        pytest.param(
            (),
            {"x": 2, "y": 7},
            ['use `width` instead or `x`', 'use `height` instead or `y`'],
            id="wrong keyword arguments",
        ),
        pytest.param(
            (),
            {"x": 2, "height": 7},
            [],
            id="invalid arguments is raised",
            marks=pytest.mark.xfail(raises=TypeError, strict=True),
        ),
    ],
)
def test_area(args, kwargs, expected):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        area(*args, **kwargs)
    actual = [str(warn.message) for warn in warns]
    assert actual == expected
