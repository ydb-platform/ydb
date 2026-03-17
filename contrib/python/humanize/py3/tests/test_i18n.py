"""Internationalisation tests."""

from __future__ import annotations

import datetime as dt
import importlib

import pytest
from freezegun import freeze_time

import humanize

with freeze_time("2020-02-02"):
    NOW = dt.datetime.now(tz=dt.timezone.utc)


@freeze_time("2020-02-02")
def test_i18n() -> None:
    three_seconds = NOW - dt.timedelta(seconds=3)
    one_min_three_seconds = dt.timedelta(milliseconds=67_000)

    assert humanize.naturaltime(three_seconds) == "3 seconds ago"
    assert humanize.ordinal(5) == "5th"
    assert humanize.precisedelta(one_min_three_seconds) == "1 minute and 7 seconds"

    try:
        humanize.i18n.activate("ru_RU")
        assert humanize.naturaltime(three_seconds) == "3 секунды назад"
        assert humanize.ordinal(5) == "5ый"
        assert humanize.precisedelta(one_min_three_seconds) == "1 минута и 7 секунд"

    except FileNotFoundError:
        pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")

    finally:
        humanize.i18n.deactivate()
        assert humanize.naturaltime(three_seconds) == "3 seconds ago"
        assert humanize.ordinal(5) == "5th"
        assert humanize.precisedelta(one_min_three_seconds) == "1 minute and 7 seconds"


def test_intcomma() -> None:
    number = 10_000_000

    assert humanize.intcomma(number) == "10,000,000"

    try:
        humanize.i18n.activate("de_DE")
        assert humanize.intcomma(number) == "10.000.000"
        assert humanize.intcomma(1_234_567.8901) == "1.234.567,8901"
        assert humanize.intcomma(1_234_567.89) == "1.234.567,89"
        assert humanize.intcomma("1234567,89") == "1.234.567,89"
        assert humanize.intcomma("1.234.567,89") == "1.234.567,89"
        assert humanize.intcomma("1.234.567,8") == "1.234.567,8"

        humanize.i18n.activate("fr_FR")
        assert humanize.intcomma(number) == "10 000 000"
        assert humanize.intcomma(1_234_567.89) == "1 234 567.89"
        assert humanize.intcomma("1 234 567.89") == "1 234 567.89"

        humanize.i18n.activate("pt_BR")
        assert humanize.intcomma(number) == "10.000.000"

    except FileNotFoundError:
        pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")

    finally:
        humanize.i18n.deactivate()
        assert humanize.intcomma(number) == "10,000,000"


def test_naturaldelta() -> None:
    seconds = 1234 * 365 * 24 * 60 * 60

    assert humanize.naturaldelta(seconds) == "1,234 years"

    try:
        humanize.i18n.activate("fr_FR")
        assert humanize.naturaldelta(seconds) == "1 234 ans"
        humanize.i18n.activate("es_ES")
        assert humanize.naturaldelta(seconds) == "1,234 años"

    except FileNotFoundError:
        pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")

    finally:
        humanize.i18n.deactivate()
        assert humanize.naturaldelta(seconds) == "1,234 years"


@pytest.mark.parametrize(
    "locale, number, expected_result",
    [
        # Italian uses comma as decimal separator
        ("it_IT", 1_000_000, "1,0 milione"),
        ("it_IT", 1_200_000, "1,2 milioni"),
        ("it_IT", 1_000_000_000, "1,0 miliardo"),
        ("it_IT", 3_500_000_000, "3,5 miliardi"),
        # Spanish uses dot as decimal separator
        ("es_ES", 1_000_000, "1.0 millón"),
        ("es_ES", 3_500_000, "3.5 millones"),
        ("es_ES", 1_000_000_000, "1.0 billón"),
        ("es_ES", 1_200_000_000, "1.2 billones"),
        ("es_ES", 1_000_000_000_000, "1.0 trillón"),
        ("es_ES", 6_700_000_000_000, "6.7 trillones"),
        ("fr_FR", "1_000", "1.0 mille"),
        ("fr_FR", "12_400", "12.4 milles"),
        ("fr_FR", "12_490", "12.5 milles"),
        ("fr_FR", "1_000_000", "1.0 million"),
        ("fr_FR", "-1_000_000", "-1.0 million"),
        ("fr_FR", "1_200_000", "1.2 millions"),
        ("fr_FR", "1_290_000", "1.3 millions"),
        ("fr_FR", "999_999_999", "1.0 milliard"),
        ("fr_FR", "1_000_000_000", "1.0 milliard"),
        ("fr_FR", "-1_000_000_000", "-1.0 milliard"),
        ("fr_FR", "2_000_000_000", "2.0 milliards"),
        ("fr_FR", "999_999_999_999", "1.0 billion"),
        ("fr_FR", "1_000_000_000_000", "1.0 billion"),
        ("fr_FR", "6_000_000_000_000", "6.0 billions"),
        ("fr_FR", "-6_000_000_000_000", "-6.0 billions"),
        ("fr_FR", "999_999_999_999_999", "1.0 billiard"),
        ("fr_FR", "1_000_000_000_000_000", "1.0 billiard"),
        ("fr_FR", "1_300_000_000_000_000", "1.3 billiards"),
        ("fr_FR", "-1_300_000_000_000_000", "-1.3 billiards"),
        ("fr_FR", "3_500_000_000_000_000_000_000", "3.5 trilliards"),
        ("fr_FR", "8_100_000_000_000_000_000_000_000_000_000_000", "8.1 quintilliards"),
        (
            "fr_FR",
            "-8_100_000_000_000_000_000_000_000_000_000_000",
            "-8.1 quintilliards",
        ),
        (
            "fr_FR",
            1_000_000_000_000_000_000_000_000_000_000_000_000,
            "1000.0 quintilliards",
        ),
        (
            "fr_FR",
            1_100_000_000_000_000_000_000_000_000_000_000_000,
            "1100.0 quintilliards",
        ),
        (
            "fr_FR",
            2_100_000_000_000_000_000_000_000_000_000_000_000,
            "2100.0 quintilliards",
        ),
    ],
)
def test_intword_i18n(locale: str, number: int, expected_result: str) -> None:
    try:
        humanize.i18n.activate(locale)
    except FileNotFoundError:
        pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")
    else:
        assert humanize.intword(number) == expected_result
    finally:
        humanize.i18n.deactivate()


@pytest.mark.parametrize(
    "locale, expected_result",
    [
        ("ar", "5خامس"),
        ("ar_SA", "5خامس"),
        ("fr", "5e"),
        ("fr_FR", "5e"),
        ("pt", "5º"),
        ("pt_BR", "5º"),
        ("pt_PT", "5º"),
    ],
)
def test_langauge_codes(locale: str, expected_result: str) -> None:
    try:
        humanize.i18n.activate(locale)
    except FileNotFoundError:
        pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")
    else:
        assert humanize.ordinal(5) == expected_result
    finally:
        humanize.i18n.deactivate()


@pytest.mark.parametrize(
    "locale, number, gender, expected_result",
    [
        ("fr_FR", 1, "male", "1er"),
        ("fr_FR", 1, "female", "1ère"),
        ("fr_FR", 2, "male", "2e"),
        ("es_ES", 1, "male", "1º"),
        ("es_ES", 5, "female", "5ª"),
        ("it_IT", 3, "male", "3º"),
        ("it_IT", 8, "female", "8ª"),
    ],
)
def test_ordinal_genders(
    locale: str, number: int, gender: str, expected_result: str
) -> None:
    try:
        humanize.i18n.activate(locale)
    except FileNotFoundError:
        pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")
    else:
        assert humanize.ordinal(number, gender=gender) == expected_result
    finally:
        humanize.i18n.deactivate()


def test_default_locale_path_defined__spec__() -> None:
    i18n = importlib.import_module("humanize.i18n")
    assert i18n._get_default_locale_path() is not None


def test_default_locale_path_none__spec__(monkeypatch: pytest.MonkeyPatch) -> None:
    i18n = importlib.import_module("humanize.i18n")
    monkeypatch.setattr(i18n, "__spec__", None)
    assert i18n._get_default_locale_path() is None


def test_default_locale_path_undefined__file__(monkeypatch: pytest.MonkeyPatch) -> None:
    i18n = importlib.import_module("humanize.i18n")
    monkeypatch.delattr(i18n, "__spec__")
    assert i18n._get_default_locale_path() is None


class TestActivate:
    expected_msg = (
        "Humanize cannot determinate the default location of the"
        " 'locale' folder. You need to pass the path explicitly."
    )

    def test_default_locale_path_null__spec__(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        i18n = importlib.import_module("humanize.i18n")
        monkeypatch.setattr(i18n, "__spec__", None)

        with pytest.raises(FileNotFoundError, match=self.expected_msg):
            i18n.activate("ru_RU")

    def test_default_locale_path_undefined__spec__(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        i18n = importlib.import_module("humanize.i18n")
        monkeypatch.delattr(i18n, "__spec__")

        with pytest.raises(FileNotFoundError, match=self.expected_msg):
            i18n.activate("ru_RU")

    @freeze_time("2020-02-02")
    def test_en_locale(self) -> None:
        three_seconds = NOW - dt.timedelta(seconds=3)
        test_str = humanize.naturaltime(three_seconds)

        humanize.i18n.activate("en_US")
        assert test_str == humanize.naturaltime(three_seconds)

        humanize.i18n.activate("en_GB")
        assert test_str == humanize.naturaltime(three_seconds)

        humanize.i18n.deactivate()

    @freeze_time("2020-02-02")
    def test_none_locale(self) -> None:
        three_seconds = NOW - dt.timedelta(seconds=3)

        try:
            humanize.i18n.activate("fr")
            assert humanize.naturaltime(three_seconds) == "il y a 3 secondes"

            humanize.i18n.activate(None)
            test_str = humanize.naturaltime(three_seconds)
            assert test_str == "3 seconds ago"
        except FileNotFoundError:
            pytest.skip("Generate .mo with scripts/generate-translation-binaries.sh")

        finally:
            humanize.i18n.deactivate()

        assert test_str == humanize.naturaltime(three_seconds)
