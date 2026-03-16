"""Number tests."""

from __future__ import annotations

import math
import typing

import pytest

import humanize
from humanize import number


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ("1", "1st"),
        ("2", "2nd"),
        ("3", "3rd"),
        ("4", "4th"),
        ("11", "11th"),
        ("12", "12th"),
        ("13", "13th"),
        ("101", "101st"),
        ("102", "102nd"),
        ("103", "103rd"),
        ("111", "111th"),
        ("something else", "something else"),
        (None, "None"),
        (math.nan, "NaN"),
        (math.inf, "+Inf"),
        (-math.inf, "-Inf"),
        ("nan", "NaN"),
        ("-inf", "-Inf"),
    ],
)
def test_ordinal(test_input: str, expected: str) -> None:
    assert humanize.ordinal(test_input) == expected


@pytest.mark.parametrize(
    "test_args, expected",
    [
        ([100], "100"),
        ([1000], "1,000"),
        ([10123], "10,123"),
        ([10311], "10,311"),
        ([1_000_000], "1,000,000"),
        ([1_234_567.25], "1,234,567.25"),
        (["100"], "100"),
        (["1000"], "1,000"),
        (["10123"], "10,123"),
        (["10311"], "10,311"),
        (["1000000"], "1,000,000"),
        (["1234567.1234567"], "1,234,567.1234567"),
        (["1234567.1234567", 0], "1,234,567"),
        (["1234567.1234567", 1], "1,234,567.1"),
        (["1234567.1234567", 10], "1,234,567.1234567000"),
        (["1234567", 1], "1,234,567.0"),
        ([None], "None"),
        ([14308.40], "14,308.4"),
        ([14308.40, None], "14,308.4"),
        ([14308.40, 1], "14,308.4"),
        ([14308.40, 2], "14,308.40"),
        ([14308.40, 3], "14,308.400"),
        ([1234.5454545], "1,234.5454545"),
        ([1234.5454545, None], "1,234.5454545"),
        ([1234.5454545, 0], "1,235"),
        ([1234.5454545, 1], "1,234.5"),
        ([1234.5454545, 2], "1,234.55"),
        ([1234.5454545, 3], "1,234.545"),
        ([1234.5454545, 10], "1,234.5454545000"),
        ([math.nan], "NaN"),
        ([math.inf], "+Inf"),
        ([-math.inf], "-Inf"),
        (["nan"], "NaN"),
        (["-inf"], "-Inf"),
    ],
)
def test_intcomma(
    test_args: list[int] | list[float] | list[str], expected: str
) -> None:
    assert humanize.intcomma(*test_args) == expected


def test_intword_powers() -> None:
    # make sure that powers & human_powers have the same number of items
    assert len(number.powers) == len(number.human_powers)


@pytest.mark.parametrize(
    "test_args, expected",
    [
        (["0"], "0"),
        (["100"], "100"),
        (["-100"], "-100"),
        (["1000"], "1.0 thousand"),
        (["12400"], "12.4 thousand"),
        (["12490"], "12.5 thousand"),
        (["1000000"], "1.0 million"),
        (["-1000000"], "-1.0 million"),
        (["1200000"], "1.2 million"),
        (["1290000"], "1.3 million"),
        (["999999999"], "1.0 billion"),
        (["1000000000"], "1.0 billion"),
        (["-1000000000"], "-1.0 billion"),
        (["2000000000"], "2.0 billion"),
        (["999999999999"], "1.0 trillion"),
        (["1000000000000"], "1.0 trillion"),
        (["6000000000000"], "6.0 trillion"),
        (["-6000000000000"], "-6.0 trillion"),
        (["999999999999999"], "1.0 quadrillion"),
        (["1000000000000000"], "1.0 quadrillion"),
        (["1300000000000000"], "1.3 quadrillion"),
        (["-1300000000000000"], "-1.3 quadrillion"),
        (["3500000000000000000000"], "3.5 sextillion"),
        (["8100000000000000000000000000000000"], "8.1 decillion"),
        (["-8100000000000000000000000000000000"], "-8.1 decillion"),
        ([1_000_000_000_000_000_000_000_000_000_000_000_000], "1000.0 decillion"),
        ([1_100_000_000_000_000_000_000_000_000_000_000_000], "1100.0 decillion"),
        ([2_100_000_000_000_000_000_000_000_000_000_000_000], "2100.0 decillion"),
        ([2e100], "2.0 googol"),
        ([None], "None"),
        (["1230000", "%0.2f"], "1.23 million"),
        ([10**101], "10.0 googol"),
        ([math.nan], "NaN"),
        ([math.inf], "+Inf"),
        ([-math.inf], "-Inf"),
        (["nan"], "NaN"),
        (["-inf"], "-Inf"),
        (["1234567", "%.0f"], "1 million"),
        (["1234567", "%.1f"], "1.2 million"),
        (["1234567", "%.2f"], "1.23 million"),
        (["1234567", "%.3f"], "1.235 million"),
        (["999500", "%.0f"], "1 million"),
        (["999499", "%.0f"], "999 thousand"),
    ],
)
def test_intword(test_args: list[str], expected: str) -> None:
    assert humanize.intword(*test_args) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (0, "zero"),
        (1, "one"),
        (2, "two"),
        (4, "four"),
        (5, "five"),
        (9, "nine"),
        (10, "10"),
        ("7", "seven"),
        (None, "None"),
        (math.nan, "NaN"),
        (math.inf, "+Inf"),
        (-math.inf, "-Inf"),
        ("nan", "NaN"),
        ("-inf", "-Inf"),
    ],
)
def test_apnumber(test_input: int | str, expected: str) -> None:
    assert humanize.apnumber(test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (1, "1"),
        (2.0, "2"),
        (4.0 / 3.0, "1 1/3"),
        (5.0 / 6.0, "5/6"),
        ("7", "7"),
        ("8.9", "8 9/10"),
        ("ten", "ten"),
        (None, "None"),
        (1 / 3, "1/3"),
        (1.5, "1 1/2"),
        (0.3, "3/10"),
        (0.333, "333/1000"),
        (math.nan, "NaN"),
        (math.inf, "+Inf"),
        (-math.inf, "-Inf"),
        ("nan", "NaN"),
        ("-inf", "-Inf"),
    ],
)
def test_fractional(test_input: float | str, expected: str) -> None:
    assert humanize.fractional(test_input) == expected


@pytest.mark.parametrize(
    "test_args, expected",
    [
        ([1000], "1.00 x 10³"),
        ([-1000], "-1.00 x 10³"),
        ([5.5], "5.50 x 10⁰"),
        ([5781651000], "5.78 x 10⁹"),
        (["1000"], "1.00 x 10³"),
        (["99"], "9.90 x 10¹"),
        ([0.3], "3.00 x 10⁻¹"),
        (["foo"], "foo"),
        ([None], "None"),
        ([1000, 1], "1.0 x 10³"),
        ([0.3, 1], "3.0 x 10⁻¹"),
        ([1000, 0], "1 x 10³"),
        ([0.3, 0], "3 x 10⁻¹"),
        ([1e20], "1.00 x 10²⁰"),
        ([2e-20], "2.00 x 10⁻²⁰"),
        ([-3e20], "-3.00 x 10²⁰"),
        ([-4e-20], "-4.00 x 10⁻²⁰"),
        ([math.nan], "NaN"),
        ([math.inf], "+Inf"),
        ([-math.inf], "-Inf"),
        (["nan"], "NaN"),
        (["-inf"], "-Inf"),
    ],
)
def test_scientific(test_args: list[typing.Any], expected: str) -> None:
    assert humanize.scientific(*test_args) == expected


@pytest.mark.parametrize(
    "test_args, expected",
    [
        ([1], "1"),
        ([None], None),
        ([0.0001, "{:.0%}"], "0%"),
        ([0.0001, "{:.0%}", 0.01], "<1%"),
        ([0.9999, "{:.0%}", None, 0.99], ">99%"),
        ([0.0001, "{:.0%}", 0.01, None, "under ", None], "under 1%"),
        ([0.9999, "{:.0%}", None, 0.99, None, "above "], "above 99%"),
        ([1, humanize.intword, 1e6, None, "under "], "under 1.0 million"),
        ([math.nan], "NaN"),
        ([math.inf], "+Inf"),
        ([-math.inf], "-Inf"),
    ],
)
def test_clamp(test_args: list[typing.Any], expected: str) -> None:
    assert humanize.clamp(*test_args) == expected


@pytest.mark.parametrize(
    "test_args, expected",
    [
        ([0], "0.00"),
        ([1, "Hz"], "1.00 Hz"),
        ([1.0, "W"], "1.00 W"),
        ([3, "C"], "3.00 C"),
        ([3, "W", 5], "3.0000 W"),
        ([1.23456], "1.23"),
        ([12.3456], "12.3"),
        ([123.456], "123"),
        ([1234.56], "1.23 k"),
        ([12345, "", 6], "12.3450 k"),
        ([200_000], "200 k"),
        ([1e25, "m"], "10.0 Ym"),
        ([1e26, "m"], "100 Ym"),
        ([1e27, "A"], "1.00 RA"),
        ([1.234e28, "A"], "12.3 RA"),
        ([1.234e-28, "A"], "123 qA"),
        ([1.235e29, "A"], "124 RA"),
        ([2.56e-30, "V"], "2.56 qV"),
        ([2.596e32, "F"], "260 QF"),
        ([1e50], "1.00 x 10⁵⁰"),
        ([1e-50], "1.00 x 10⁻⁵⁰"),
        ([-1500, "V"], "-1.50 kV"),
        ([0.12], "120 m"),
        ([0.012], "12.0 m"),
        ([0.0012], "1.20 m"),
        ([0.00012], "120 μ"),
        ([1e-23], "10.0 y"),
        ([1e-24], "1.00 y"),
        ([1e-25], "100 r"),
        ([1e-26], "10.0 r"),
        ([1, "°"], "1.00°"),
        ([0.1, "°"], "100m°"),
        ([100], "100"),
        ([0.1], "100 m"),
        ([1.5123, "", 0], "2"),
        ([10.5123, "", 0], "11"),
        ([10.5123, "", 1], "11"),
        ([10.5123, "", 2], "11"),
        ([10.5123, "", 3], "10.5"),
        ([1, "", 0], "1"),
        ([10, "", 0], "10"),
        ([100, "", 0], "100"),
        ([1000, "", 0], "1 k"),
        ([1, "", 1], "1"),
        ([10, "", 1], "10"),
        ([100, "", 1], "100"),
        ([1000, "", 1], "1 k"),
        ([1, "", 2], "1.0"),
        ([10, "", 2], "10"),
        ([100, "", 2], "100"),
        ([1000, "", 2], "1.0 k"),
        ([1, "", 3], "1.00"),
        ([10, "", 3], "10.0"),
        ([100, "", 3], "100"),
        ([1000, "", 3], "1.00 k"),
        ([math.nan], "NaN"),
        ([math.nan, "m"], "NaN"),
        ([math.inf], "+Inf"),
        ([-math.inf], "-Inf"),
    ],
    ids=str,
)
def test_metric(test_args: list[typing.Any], expected: str) -> None:
    assert humanize.metric(*test_args) == expected
