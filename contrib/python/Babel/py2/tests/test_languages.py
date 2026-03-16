# -- encoding: UTF-8 --
from babel.languages import get_official_languages, get_territory_language_info


def test_official_languages():
    assert get_official_languages("FI") == ("fi", "sv")
    assert get_official_languages("SE") == ("sv",)
    assert get_official_languages("CH") == ("de", "fr", "it")
    assert get_official_languages("CH", de_facto=True) == ("de", "gsw", "fr", "it")
    assert get_official_languages("CH", regional=True) == ("de", "fr", "it", "rm")


def test_get_language_info():
    assert (
        set(get_territory_language_info("HU")) ==
        {"hu", "fr", "en", "de", "ro", "hr", "sk", "sl"}
    )
