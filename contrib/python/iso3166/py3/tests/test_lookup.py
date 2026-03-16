# -*- coding: utf-8 -*-

from typing import List, Union
import pytest
import iso3166
from iso3166 import countries


def check_lookup(
    alpha2: str,
    matching_keys: List[Union[int, str]],
    missing_keys: List[Union[int, str]],
) -> None:
    for k in matching_keys:
        assert countries[k].alpha2 == alpha2
        assert countries.get(k).alpha2 == alpha2
        assert k in countries

    for k in missing_keys:
        with pytest.raises(KeyError):
            countries.get(k)

        with pytest.raises(KeyError):
            countries[k]

        assert countries.get(k, None) is None


def test_length() -> None:
    assert len(countries) == len(iso3166._records)


def test_empty_string() -> None:
    check_lookup("US", ["us", "US"], [""])


def test_alpha2() -> None:
    check_lookup("US", ["us", "US"], ["zz"])


def test_alpha3() -> None:
    check_lookup("US", ["usa", "USA"], ["zzz"])


def test_name() -> None:
    check_lookup(
        "US",
        ["united states of america", "United STates of America"],
        ["zzzzz"],
    )


def test_numeric() -> None:
    check_lookup("US", [840, "840"], [111, "111"])

    with pytest.raises(KeyError):
        countries.get("000")


def test_alt_name() -> None:
    check_lookup("TW", ["taiwan", "Taiwan, province of china"], ["zzzzz"])
    check_lookup("PS", ["palestine", "palestine, state of"], ["zzzz"])


def test_none_default() -> None:
    assert countries.get("NOTUS", None) is None


def test_data() -> None:
    assert len(list(countries)) > 0

    for country in countries:
        assert len(country.alpha2) == 2
        assert country.alpha2.upper() == country.alpha2

        assert len(country.alpha3) == 3
        assert country.alpha3.upper() == country.alpha3

        assert len(country.numeric) == 3
        assert country.numeric == ("%03d" % int(country.numeric))
        assert int(country.numeric) > 0

        assert len(country.name) > 3
        assert len(country.apolitical_name) > 3
