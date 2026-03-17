#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2025 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at https://github.com/python-babel/babel/blob/master/LICENSE.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at https://github.com/python-babel/babel/commits/master/.

import pytest

from babel import core
from babel.core import Locale, default_locale


def test_locale_provides_access_to_cldr_locale_data():
    locale = Locale('en', 'US')
    assert locale.display_name == 'English (United States)'
    assert locale.number_symbols["latn"]['decimal'] == '.'


def test_locale_repr():
    assert repr(Locale('en', 'US')) == "Locale('en', territory='US')"
    assert (repr(Locale('de', 'DE')) == "Locale('de', territory='DE')")
    assert (repr(Locale('zh', 'CN', script='Hans')) == "Locale('zh', territory='CN', script='Hans')")


def test_locale_comparison():
    en_US = Locale('en', 'US')
    en_US_2 = Locale('en', 'US')
    fi_FI = Locale('fi', 'FI')
    bad_en_US = Locale('en_US')
    assert en_US == en_US
    assert en_US == en_US_2
    assert en_US != fi_FI
    assert not (en_US != en_US_2)
    assert en_US is not None
    assert en_US != bad_en_US
    assert fi_FI != bad_en_US


def test_can_return_default_locale(monkeypatch):
    monkeypatch.setenv('LC_MESSAGES', 'fr_FR.UTF-8')
    assert Locale('fr', 'FR') == Locale.default('LC_MESSAGES')


def test_ignore_invalid_locales_in_lc_ctype(monkeypatch):
    # This is a regression test specifically for a bad LC_CTYPE setting on
    # MacOS X 10.6 (#200)
    monkeypatch.setenv('LC_CTYPE', 'UTF-8')
    # must not throw an exception
    default_locale('LC_CTYPE')


def test_zone_aliases_and_territories():
    aliases = core.get_global('zone_aliases')
    territories = core.get_global('zone_territories')
    assert aliases['GMT'] == 'Etc/GMT'
    assert aliases['UTC'] == 'Etc/UTC'
    assert territories['Europe/Berlin'] == 'DE'
    # Check that the canonical (IANA) names are used in `territories`,
    # but that aliases are still available.
    assert territories['Africa/Asmara'] == 'ER'
    assert aliases['Africa/Asmera'] == 'Africa/Asmara'
    assert territories['Europe/Kyiv'] == 'UA'
    assert aliases['Europe/Kiev'] == 'Europe/Kyiv'


def test_hash():
    locale_a = Locale('en', 'US')
    locale_b = Locale('en', 'US')
    locale_c = Locale('fi', 'FI')
    assert hash(locale_a) == hash(locale_b)
    assert hash(locale_a) != hash(locale_c)


class TestLocaleClass:

    def test_attributes(self):
        locale = Locale('en', 'US')
        assert locale.language == 'en'
        assert locale.territory == 'US'

    def test_default(self, monkeypatch):
        for name in ['LANGUAGE', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES']:
            monkeypatch.setenv(name, '')
        monkeypatch.setenv('LANG', 'fr_FR.UTF-8')
        default = Locale.default('LC_MESSAGES')
        assert (default.language, default.territory) == ('fr', 'FR')

    def test_negotiate(self):
        de_DE = Locale.negotiate(['de_DE', 'en_US'], ['de_DE', 'de_AT'])
        assert (de_DE.language, de_DE.territory) == ('de', 'DE')
        de = Locale.negotiate(['de_DE', 'en_US'], ['en', 'de'])
        assert (de.language, de.territory) == ('de', None)
        nothing = Locale.negotiate(['de_DE', 'de'], ['en_US'])
        assert nothing is None

    def test_negotiate_custom_separator(self):
        de_DE = Locale.negotiate(['de-DE', 'de'], ['en-us', 'de-de'], sep='-')
        assert (de_DE.language, de_DE.territory) == ('de', 'DE')

    def test_parse(self):
        locale = Locale.parse('de-DE', sep='-')
        assert locale.display_name == 'Deutsch (Deutschland)'

        de_DE = Locale.parse(locale)
        assert (de_DE.language, de_DE.territory) == ('de', 'DE')

    def test_parse_likely_subtags(self):
        locale = Locale.parse('zh-TW', sep='-')
        assert locale.language == 'zh'
        assert locale.territory == 'TW'
        assert locale.script == 'Hant'

        locale = Locale.parse('zh_CN')
        assert locale.language == 'zh'
        assert locale.territory == 'CN'
        assert locale.script == 'Hans'

        locale = Locale.parse('zh_SG')
        assert locale.language == 'zh'
        assert locale.territory == 'SG'
        assert locale.script == 'Hans'

        locale = Locale.parse('und_AT')
        assert locale.language == 'de'
        assert locale.territory == 'AT'

        locale = Locale.parse('und_UK')
        assert locale.language == 'en'
        assert locale.territory == 'GB'
        assert locale.script is None

    def test_get_display_name(self):
        zh_CN = Locale('zh', 'CN', script='Hans')
        assert zh_CN.get_display_name('en') == 'Chinese (Simplified, China)'

    def test_display_name_property(self):
        assert Locale('en').display_name == 'English'
        assert Locale('en', 'US').display_name == 'English (United States)'
        assert Locale('sv').display_name == 'svenska'

    def test_english_name_property(self):
        assert Locale('de').english_name == 'German'
        assert Locale('de', 'DE').english_name == 'German (Germany)'

    def test_languages_property(self):
        assert Locale('de', 'DE').languages['ja'] == 'Japanisch'

    def test_scripts_property(self):
        assert Locale('en', 'US').scripts['Hira'] == 'Hiragana'

    def test_territories_property(self):
        assert Locale('es', 'CO').territories['DE'] == 'Alemania'

    def test_variants_property(self):
        assert (Locale('de', 'DE').variants['1901'] ==
                'Alte deutsche Rechtschreibung')

    def test_currencies_property(self):
        assert Locale('en').currencies['COP'] == 'Colombian Peso'
        assert Locale('de', 'DE').currencies['COP'] == 'Kolumbianischer Peso'

    def test_currency_symbols_property(self):
        assert Locale('en', 'US').currency_symbols['USD'] == '$'
        assert Locale('es', 'CO').currency_symbols['USD'] == 'US$'

    def test_number_symbols_property(self):
        assert Locale('fr', 'FR').number_symbols["latn"]['decimal'] == ','
        assert Locale('ar', 'IL').number_symbols["arab"]['percentSign'] == '٪\u061c'
        assert Locale('ar', 'IL').number_symbols["latn"]['percentSign'] == '\u200e%\u200e'

    def test_other_numbering_systems_property(self):
        assert Locale('fr', 'FR').other_numbering_systems['native'] == 'latn'
        assert 'traditional' not in Locale('fr', 'FR').other_numbering_systems

        assert Locale('el', 'GR').other_numbering_systems['native'] == 'latn'
        assert Locale('el', 'GR').other_numbering_systems['traditional'] == 'grek'

    def test_default_numbering_systems_property(self):
        assert Locale('en', 'GB').default_numbering_system == 'latn'
        assert Locale('ar', 'EG').default_numbering_system == 'arab'

    @pytest.mark.all_locales
    def test_all_locales_have_default_numbering_system(self, locale):
        locale = Locale.parse(locale)
        assert locale.default_numbering_system

    def test_decimal_formats(self):
        assert Locale('en', 'US').decimal_formats[None].pattern == '#,##0.###'

    def test_currency_formats_property(self):
        assert (Locale('en', 'US').currency_formats['standard'].pattern ==
                '\xa4#,##0.00')
        assert (Locale('en', 'US').currency_formats['accounting'].pattern ==
                '\xa4#,##0.00;(\xa4#,##0.00)')

    def test_percent_formats_property(self):
        assert Locale('en', 'US').percent_formats[None].pattern == '#,##0%'

    def test_scientific_formats_property(self):
        assert Locale('en', 'US').scientific_formats[None].pattern == '#E0'

    def test_periods_property(self):
        assert Locale('en', 'US').periods['am'] == 'AM'

    def test_days_property(self):
        assert Locale('de', 'DE').days['format']['wide'][3] == 'Donnerstag'

    def test_months_property(self):
        assert Locale('de', 'DE').months['format']['wide'][10] == 'Oktober'

    def test_quarters_property(self):
        assert Locale('de', 'DE').quarters['format']['wide'][1] == '1. Quartal'

    def test_eras_property(self):
        assert Locale('en', 'US').eras['wide'][1] == 'Anno Domini'
        assert Locale('en', 'US').eras['abbreviated'][0] == 'BC'

    def test_time_zones_property(self):
        time_zones = Locale('en', 'US').time_zones
        assert (time_zones['Europe/London']['long']['daylight'] ==
                'British Summer Time')
        assert time_zones['America/St_Johns']['city'] == 'St. John\u2019s'

    def test_meta_zones_property(self):
        meta_zones = Locale('en', 'US').meta_zones
        assert (meta_zones['Europe_Central']['long']['daylight'] ==
                'Central European Summer Time')

    def test_zone_formats_property(self):
        assert Locale('en', 'US').zone_formats['fallback'] == '%(1)s (%(0)s)'
        assert Locale('pt', 'BR').zone_formats['region'] == 'Hor\xe1rio %s'

    def test_first_week_day_property(self):
        assert Locale('de', 'DE').first_week_day == 0
        assert Locale('en', 'US').first_week_day == 6

    def test_weekend_start_property(self):
        assert Locale('de', 'DE').weekend_start == 5

    def test_weekend_end_property(self):
        assert Locale('de', 'DE').weekend_end == 6

    def test_min_week_days_property(self):
        assert Locale('de', 'DE').min_week_days == 4

    def test_date_formats_property(self):
        assert Locale('en', 'US').date_formats['short'].pattern == 'M/d/yy'
        assert Locale('fr', 'FR').date_formats['long'].pattern == 'd MMMM y'

    def test_time_formats_property(self):
        assert Locale('en', 'US').time_formats['short'].pattern == 'h:mm\u202fa'
        assert Locale('fr', 'FR').time_formats['long'].pattern == 'HH:mm:ss z'

    def test_datetime_formats_property(self):
        assert Locale('en').datetime_formats['full'] == "{1}, {0}"
        assert Locale('th').datetime_formats['medium'] == '{1} {0}'

    def test_datetime_skeleton_property(self):
        assert Locale('en').datetime_skeletons['Md'].pattern == "M/d"
        assert Locale('th').datetime_skeletons['Md'].pattern == 'd/M'

    def test_plural_form_property(self):
        assert Locale('en').plural_form(1) == 'one'
        assert Locale('en').plural_form(0) == 'other'
        assert Locale('fr').plural_form(0) == 'one'
        assert Locale('ru').plural_form(100) == 'many'


def test_default_locale(monkeypatch):
    for name in ['LANGUAGE', 'LANG', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES']:
        monkeypatch.setenv(name, '')
    monkeypatch.setenv('LANG', 'fr_FR.UTF-8')
    assert default_locale('LC_MESSAGES') == 'fr_FR'
    monkeypatch.setenv('LC_MESSAGES', 'POSIX')
    assert default_locale('LC_MESSAGES') == 'en_US_POSIX'

    for value in ['C', 'C.UTF-8', 'POSIX']:
        monkeypatch.setenv('LANGUAGE', value)
        assert default_locale() == 'en_US_POSIX'


def test_default_locale_multiple_args(monkeypatch):
    for name in ['LANGUAGE', 'LANG', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES', 'LC_MONETARY', 'LC_NUMERIC']:
        monkeypatch.setenv(name, '')
    assert default_locale(["", 0, None]) is None
    monkeypatch.setenv('LANG', 'en_US')
    assert default_locale(('LC_MONETARY', 'LC_NUMERIC')) == 'en_US'  # No LC_MONETARY or LC_NUMERIC set
    monkeypatch.setenv('LC_NUMERIC', 'fr_FR.UTF-8')
    assert default_locale(('LC_MONETARY', 'LC_NUMERIC')) == 'fr_FR'  # LC_NUMERIC set
    monkeypatch.setenv('LC_MONETARY', 'fi_FI.UTF-8')
    assert default_locale(('LC_MONETARY', 'LC_NUMERIC')) == 'fi_FI'  # LC_MONETARY set, it takes precedence


def test_default_locale_bad_arg():
    with pytest.raises(TypeError):
        default_locale(42)


def test_negotiate_locale():
    assert (core.negotiate_locale(['de_DE', 'en_US'], ['de_DE', 'de_AT']) ==
            'de_DE')
    assert core.negotiate_locale(['de_DE', 'en_US'], ['en', 'de']) == 'de'
    assert (core.negotiate_locale(['de_DE', 'en_US'], ['de_de', 'de_at']) ==
            'de_DE')
    assert (core.negotiate_locale(['de_DE', 'en_US'], ['de_de', 'de_at']) ==
            'de_DE')
    assert (core.negotiate_locale(['ja', 'en_US'], ['ja_JP', 'en_US']) ==
            'ja_JP')
    assert core.negotiate_locale(['no', 'sv'], ['nb_NO', 'sv_SE']) == 'nb_NO'


def test_parse_locale():
    assert core.parse_locale('zh_CN') == ('zh', 'CN', None, None)
    assert core.parse_locale('zh_Hans_CN') == ('zh', 'CN', 'Hans', None)
    assert core.parse_locale('zh-CN', sep='-') == ('zh', 'CN', None, None)

    with pytest.raises(ValueError, match="'not_a_LOCALE_String' is not a valid locale identifier"):
        core.parse_locale('not_a_LOCALE_String')

    assert core.parse_locale('it_IT@euro') == ('it', 'IT', None, None, 'euro')
    assert core.parse_locale('it_IT@something') == ('it', 'IT', None, None, 'something')

    assert core.parse_locale('en_US.UTF-8') == ('en', 'US', None, None)
    assert (core.parse_locale('de_DE.iso885915@euro') ==
            ('de', 'DE', None, None, 'euro'))

    with pytest.raises(ValueError, match="empty"):
        core.parse_locale("")


@pytest.mark.parametrize('filename', [
    'babel/global.dat',
    'babel/locale-data/root.dat',
    'babel/locale-data/en.dat',
    'babel/locale-data/en_US.dat',
    'babel/locale-data/en_US_POSIX.dat',
    'babel/locale-data/zh_Hans_CN.dat',
    'babel/locale-data/zh_Hant_TW.dat',
    'babel/locale-data/es_419.dat',
])
def test_compatible_classes_in_global_and_localedata(filename):
    import pickle

    class Unpickler(pickle.Unpickler):

        def find_class(self, module, name):
            # *.dat files must have compatible classes between Python 2 and 3
            if module.split('.')[0] == 'babel':
                return pickle.Unpickler.find_class(self, module, name)
            raise pickle.UnpicklingError(f"global '{module}.{name}' is forbidden")

    import yatest.common as yc
    with open(yc.source_path(f"contrib/python/Babel/py3/{filename}"), 'rb') as f:
        assert Unpickler(f).load()


def test_issue_601_no_language_name_but_has_variant():
    # kw_GB has a variant for Finnish but no actual language name for Finnish,
    # so `get_display_name()` previously crashed with a TypeError as it attempted
    # to concatenate " (Finnish)" to None.
    # Instead, it's better to return None altogether, as we can't reliably format
    # part of a language name.

    assert Locale.parse('fi_FI').get_display_name('kw_GB') is None


def test_issue_814():
    loc = Locale.parse('ca_ES_valencia')
    assert loc.variant == "VALENCIA"
    assert loc.get_display_name() == 'català (Espanya, valencià)'


def test_issue_1112():
    """
    Test that an alternate spelling of `Türkei` doesn't inadvertently
    get imported from `de_AT` to replace the parent's non-alternate spelling.
    """
    assert (
        Locale.parse('de').territories['TR'] ==
        Locale.parse('de_AT').territories['TR'] ==
        Locale.parse('de_CH').territories['TR'] ==
        Locale.parse('de_DE').territories['TR'] ==
        'Türkei'
    )


def test_language_alt_official_not_used():
    # If there exists an official and customary language name, the customary
    # name should be used.
    #
    # For example, here 'Muscogee' should be used instead of 'Mvskoke':
    # <language type="mus">Muscogee</language>
    # <language type="mus" alt="official">Mvskoke</language>

    locale = Locale('mus')
    assert locale.get_display_name() == 'Mvskoke'
    assert locale.get_display_name(Locale('en')) == 'Muscogee'


def test_locale_parse_empty():
    with pytest.raises(ValueError, match="Empty") as ei:
        Locale.parse("")
    assert isinstance(ei.value.args[0], str)
    with pytest.raises(TypeError, match="Empty"):
        Locale.parse(None)
    with pytest.raises(TypeError, match="Empty"):
        Locale.parse(False)  # weird...!


def test_get_cldr_version():
    assert core.get_cldr_version() == "47"
