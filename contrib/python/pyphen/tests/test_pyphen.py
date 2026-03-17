"""

Pyphen Tests
============

Tests can be launched with Pytest.

"""


from pathlib import Path

import pyphen
import yatest.common as yc


def test_inserted():
    """Test the ``inserted`` method."""
    dic = pyphen.Pyphen(lang='nl_NL')
    assert dic.inserted('lettergrepen') == 'let-ter-gre-pen'


def test_wrap():
    """Test the ``wrap`` method."""
    dic = pyphen.Pyphen(lang='nl_NL')
    assert dic.wrap('autobandventieldopje', 11) == (
        'autoband-', 'ventieldopje')


def test_iterate():
    """Test the ``iterate`` method."""
    dic = pyphen.Pyphen(lang='nl_NL')
    assert tuple(dic.iterate('Amsterdam')) == (
        ('Amster', 'dam'), ('Am', 'sterdam'))


def test_fallback_dict():
    """Test the ``iterate`` method with a fallback dict."""
    dic = pyphen.Pyphen(lang='nl_NL-variant')
    assert tuple(dic.iterate('Amsterdam')) == (
        ('Amster', 'dam'), ('Am', 'sterdam'))


def test_missing_dict():
    """Test a missing dict."""
    try:
        pyphen.Pyphen(lang='mi_SS')
    except KeyError:
        pass
    else:  # pragma: no cover
        raise Exception('Importing a missing dict must raise a KeyError')


def test_personal_dict():
    """Test a personal dict."""
    dic = pyphen.Pyphen(lang='fr')
    assert dic.inserted('autobandventieldopje') != 'au-to-band-ven-tiel-dop-je'
    pyphen.LANGUAGES['fr'] = pyphen.LANGUAGES['nl_NL']
    dic = pyphen.Pyphen(lang='fr')
    assert dic.inserted('autobandventieldopje') == 'au-to-band-ven-tiel-dop-je'


def test_dict_from_filename():
    """Test a dict open from filename."""
    dic_path = Path(yc.source_path(__file__)).parents[1] / 'pyphen' / 'dictionaries' / 'hyph_fr.dic'
    dic = pyphen.Pyphen(filename=str(dic_path))
    assert dic.inserted('bonjour') == 'bon-jour'


def test_dict_from_path():
    """Test a dict open from path."""
    dic_path = Path(yc.source_path(__file__)).parents[1] / 'pyphen' / 'dictionaries' / 'hyph_fr.dic'
    dic = pyphen.Pyphen(filename=dic_path)
    assert dic.inserted('bonjour') == 'bon-jour'


def test_left_right():
    """Test the ``left`` and ``right`` parameters."""
    dic = pyphen.Pyphen(lang='nl_NL')
    assert dic.inserted('lettergrepen') == 'let-ter-gre-pen'
    dic = pyphen.Pyphen(lang='nl_NL', left=4)
    assert dic.inserted('lettergrepen') == 'letter-gre-pen'
    dic = pyphen.Pyphen(lang='nl_NL', right=4)
    assert dic.inserted('lettergrepen') == 'let-ter-grepen'
    dic = pyphen.Pyphen(lang='nl_NL', left=4, right=4)
    assert dic.inserted('lettergrepen') == 'letter-grepen'


def test_filename():
    """Test the ``filename`` parameter."""
    dic = pyphen.Pyphen(filename=pyphen.LANGUAGES['nl_NL'])
    assert dic.inserted('lettergrepen') == 'let-ter-gre-pen'


def test_alternative():
    """Test the alternative parser."""
    dic = pyphen.Pyphen(lang='hu', left=1, right=1)
    assert tuple(dic.iterate('kulissza')) == (
        ('kulisz', 'sza'), ('ku', 'lissza'))
    assert dic.inserted('kulissza') == 'ku-lisz-sza'


def test_upper():
    """Test uppercase."""
    dic = pyphen.Pyphen(lang='nl_NL')
    assert dic.inserted('LETTERGREPEN') == 'LET-TER-GRE-PEN'


def test_upper_alternative():
    """Test uppercase with alternative parser."""
    dic = pyphen.Pyphen(lang='hu', left=1, right=1)
    assert tuple(dic.iterate('KULISSZA')) == (
        ('KULISZ', 'SZA'), ('KU', 'LISSZA'))
    assert dic.inserted('KULISSZA') == 'KU-LISZ-SZA'


def test_all_dictionaries():
    """Test that all included dictionaries can be parsed."""
    for lang in pyphen.LANGUAGES:
        pyphen.Pyphen(lang=lang)


def test_fallback():
    """Test the language fallback algorithm."""
    assert pyphen.language_fallback('en') == 'en'
    assert pyphen.language_fallback('en_US') == 'en_US'
    assert pyphen.language_fallback('en_FR') == 'en'
    assert pyphen.language_fallback('sr-Latn') == 'sr_Latn'
    assert pyphen.language_fallback('SR-LATN') == 'sr_Latn'
    assert pyphen.language_fallback('sr-Cyrl') == 'sr'
    assert pyphen.language_fallback('fr-Latn-FR') == 'fr'
    assert pyphen.language_fallback('en-US_variant1-x') == 'en_US'
