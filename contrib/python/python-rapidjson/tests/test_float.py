# -*- coding: utf-8 -*-
# :Project:   python-rapidjson -- Tests on floats
# :Author:    John Anderson <sontek@gmail.com>
# :License:   MIT License
# :Copyright: © 2015 John Anderson
# :Copyright: © 2016, 2017, 2020, 2024 Lele Gaifax
#

from decimal import Decimal
import math

import pytest

import rapidjson as rj


def test_infinity_f():
    inf = float("inf")

    dumped = rj.dumps(inf)
    loaded = rj.loads(dumped)
    assert loaded == inf

    dumped = rj.dumps(inf, allow_nan=True)
    loaded = rj.loads(dumped, allow_nan=True)
    assert loaded == inf

    with pytest.raises(ValueError):
        rj.dumps(inf, number_mode=None)

    with pytest.raises(ValueError):
        rj.dumps(inf, allow_nan=False)

    d = Decimal(inf)
    assert d.is_infinite()

    with pytest.raises(ValueError):
        rj.dumps(d, number_mode=rj.NM_DECIMAL)

    dumped = rj.dumps(d, number_mode=rj.NM_DECIMAL, allow_nan=True)
    loaded = rj.loads(dumped, number_mode=rj.NM_DECIMAL|rj.NM_NAN)
    assert loaded == inf
    assert loaded.is_infinite()


def test_infinity_c():
    inf = float("inf")

    dumped = rj.Encoder()(inf)
    loaded = rj.Decoder()(dumped)
    assert loaded == inf

    with pytest.raises(ValueError):
        rj.Encoder(number_mode=None)(inf)

    d = Decimal(inf)
    assert d.is_infinite()

    with pytest.raises(ValueError):
        rj.Encoder(number_mode=rj.NM_DECIMAL)(d)

    dumped = rj.Encoder(number_mode=rj.NM_DECIMAL|rj.NM_NAN)(d)
    loaded = rj.Decoder(number_mode=rj.NM_DECIMAL|rj.NM_NAN)(dumped)
    assert loaded == inf
    assert loaded.is_infinite()


def test_negative_infinity_f():
    inf = float("-infinity")
    dumped = rj.dumps(inf)
    loaded = rj.loads(dumped)
    assert loaded == inf

    dumped = rj.dumps(inf, allow_nan=True)
    loaded = rj.loads(dumped, allow_nan=True)
    assert loaded == inf

    with pytest.raises(ValueError):
        rj.dumps(inf, number_mode=None)

    with pytest.raises(ValueError):
        rj.dumps(inf, allow_nan=False)

    d = Decimal(inf)
    assert d.is_infinite()

    with pytest.raises(ValueError):
        rj.dumps(d, number_mode=rj.NM_DECIMAL)

    dumped = rj.dumps(d, number_mode=rj.NM_DECIMAL|rj.NM_NAN)
    loaded = rj.loads(dumped, number_mode=rj.NM_DECIMAL, allow_nan=True)
    assert loaded == inf
    assert loaded.is_infinite()


def test_negative_infinity_c():
    inf = float("-infinity")
    dumped = rj.Encoder()(inf)
    loaded = rj.Decoder()(dumped)
    assert loaded == inf

    with pytest.raises(ValueError):
        rj.Encoder(number_mode=None)(inf)

    d = Decimal(inf)
    assert d.is_infinite()

    with pytest.raises(ValueError):
        rj.Encoder(number_mode=rj.NM_DECIMAL)(d)

    dumped = rj.Encoder(number_mode=rj.NM_DECIMAL|rj.NM_NAN)(d)
    loaded = rj.Decoder(number_mode=rj.NM_DECIMAL|rj.NM_NAN)(dumped)
    assert loaded == inf
    assert loaded.is_infinite()


def test_nan_f():
    nan = float("nan")
    dumped = rj.dumps(nan)
    loaded = rj.loads(dumped)

    assert math.isnan(nan)
    assert math.isnan(loaded)

    with pytest.raises(ValueError):
        rj.dumps(nan, number_mode=None)

    with pytest.raises(ValueError):
        rj.dumps(nan, allow_nan=False)

    d = Decimal(nan)
    assert d.is_nan()

    with pytest.raises(ValueError):
        rj.dumps(d, number_mode=rj.NM_DECIMAL)

    dumped = rj.dumps(d, number_mode=rj.NM_DECIMAL|rj.NM_NAN)
    loaded = rj.loads(dumped, number_mode=rj.NM_DECIMAL|rj.NM_NAN)
    assert loaded.is_nan()


def test_nan_c():
    nan = float("nan")
    dumped = rj.Encoder()(nan)
    loaded = rj.Decoder()(dumped)

    assert math.isnan(nan)
    assert math.isnan(loaded)

    with pytest.raises(ValueError):
        rj.Encoder(number_mode=None)(nan)

    d = Decimal(nan)
    assert d.is_nan()

    with pytest.raises(ValueError):
        rj.Encoder(number_mode=rj.NM_DECIMAL)(d)

    dumped = rj.Encoder(number_mode=rj.NM_DECIMAL|rj.NM_NAN)(d)
    loaded = rj.Decoder(number_mode=rj.NM_DECIMAL|rj.NM_NAN)(dumped)
    assert loaded.is_nan()


def test_issue_213():
    class CustomRepr(float):
        def __repr__(self):
            stdrepr = super().__repr__()
            return f'CustomRepr({stdrepr})'

    f = CustomRepr(123.45)
    dumped = rj.dumps(f)
    loaded = rj.loads(dumped)
    assert loaded == 123.45
