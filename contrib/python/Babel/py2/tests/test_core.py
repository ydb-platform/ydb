# -*- coding: utf-8 -*-
#
# Copyright (C) 2007-2011 Edgewall Software, 2013-2021 the Babel team
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution. The terms
# are also available at http://babel.edgewall.org/wiki/License.
#
# This software consists of voluntary contributions made by many
# individuals. For the exact contribution history, see the revision
# history and logs, available at http://babel.edgewall.org/log/.

import pytest

from babel import core
from babel.core import default_locale, Locale


def test_locale_provides_access_to_cldr_locale_data():
    locale = Locale('en', 'US')
    assert u'English (United States)' == locale.display_name
    assert u'.' == locale.number_symbols['decimal']


def test_locale_repr():
    assert repr(Locale('en', 'US')) == "Locale('en', territory='US')"
    assert ("Locale('de', territory='DE')" == repr(Locale('de', 'DE')))
    assert ("Locale('zh', territory='CN', script='Hans')" ==
            repr(Locale('zh', 'CN', script='Hans')))


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


def test_can_return_default_locale(os_environ):
    os_environ['LC_MESSAGES'] = 'fr_FR.UTF-8'
    assert Locale('fr', 'FR') == Locale.default('LC_MESSAGES')


def test_ignore_invalid_locales_in_lc_ctype(os_environ):
    # This is a regression test specifically for a bad LC_CTYPE setting on
    # MacOS X 10.6 (#200)
    os_environ['LC_CTYPE'] = 'UTF-8'
    # must not throw an exception
    default_locale('LC_CTYPE')


def test_get_global():
    assert core.get_global('zone_aliases')['GMT'] == 'Etc/GMT'
    assert core.get_global('zone_aliases')['UTC'] == 'Etc/UTC'
    assert core.get_global('zone_territories')['Europe/Berlin'] == 'DE'


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

    def test_default(self, os_environ):
        for name in ['LANGUAGE', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES']:
            os_environ[name] = ''
        os_environ['LANG'] = 'fr_FR.UTF-8'
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
        l = Locale.parse('de-DE', sep='-')
        assert l.display_name == 'Deutsch (Deutschland)'

        de_DE = Locale.parse(l)
        assert (de_DE.language, de_DE.territory) == ('de', 'DE')

    def test_parse_likely_subtags(self):
        l = Locale.parse('zh-TW', sep='-')
        assert l.language == 'zh'
        assert l.territory == 'TW'
        assert l.script == 'Hant'

        l = Locale.parse('zh_CN')
        assert l.language == 'zh'
        assert l.territory == 'CN'
        assert l.script == 'Hans'

        l = Locale.parse('zh_SG')
        assert l.language == 'zh'
        assert l.territory == 'SG'
        assert l.script == 'Hans'

        l = Locale.parse('und_AT')
        assert l.language == 'de'
        assert l.territory == 'AT'

        l = Locale.parse('und_UK')
        assert l.language == 'en'
        assert l.territory == 'GB'
        assert l.script is None

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
        assert Locale('fr', 'FR').number_symbols['decimal'] == ','

    def test_decimal_formats(self):
        assert Locale('en', 'US').decimal_formats[None].pattern == '#,##0.###'

    def test_currency_formats_property(self):
        assert (Locale('en', 'US').currency_formats['standard'].pattern ==
                u'\xa4#,##0.00')
        assert (Locale('en', 'US').currency_formats['accounting'].pattern ==
                u'\xa4#,##0.00;(\xa4#,##0.00)')

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
        assert time_zones['America/St_Johns']['city'] == u'St. John\u2019s'

    def test_meta_zones_property(self):
        meta_zones = Locale('en', 'US').meta_zones
        assert (meta_zones['Europe_Central']['long']['daylight'] ==
                'Central European Summer Time')

    def test_zone_formats_property(self):
        assert Locale('en', 'US').zone_formats['fallback'] == '%(1)s (%(0)s)'
        assert Locale('pt', 'BR').zone_formats['region'] == u'Hor\xe1rio %s'

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
        assert Locale('en', 'US').time_formats['short'].pattern == 'h:mm a'
        assert Locale('fr', 'FR').time_formats['long'].pattern == 'HH:mm:ss z'

    def test_datetime_formats_property(self):
        assert Locale('en').datetime_formats['full'] == u"{1} 'at' {0}"
        assert Locale('th').datetime_formats['medium'] == u'{1} {0}'

    def test_datetime_skeleton_property(self):
        assert Locale('en').datetime_skeletons['Md'].pattern == u"M/d"
        assert Locale('th').datetime_skeletons['Md'].pattern == u'd/M'

    def test_plural_form_property(self):
        assert Locale('en').plural_form(1) == 'one'
        assert Locale('en').plural_form(0) == 'other'
        assert Locale('fr').plural_form(0) == 'one'
        assert Locale('ru').plural_form(100) == 'many'


def test_default_locale(os_environ):
    for name in ['LANGUAGE', 'LC_ALL', 'LC_CTYPE', 'LC_MESSAGES']:
        os_environ[name] = ''
    os_environ['LANG'] = 'fr_FR.UTF-8'
    assert default_locale('LC_MESSAGES') == 'fr_FR'

    os_environ['LC_MESSAGES'] = 'POSIX'
    assert default_locale('LC_MESSAGES') == 'en_US_POSIX'

    for value in ['C', 'C.UTF-8', 'POSIX']:
        os_environ['LANGUAGE'] = value
        assert default_locale() == 'en_US_POSIX'


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

    with pytest.raises(ValueError) as excinfo:
        core.parse_locale('not_a_LOCALE_String')
    assert (excinfo.value.args[0] ==
            "'not_a_LOCALE_String' is not a valid locale identifier")

    assert core.parse_locale('it_IT@euro') == ('it', 'IT', None, None)
    assert core.parse_locale('en_US.UTF-8') == ('en', 'US', None, None)
    assert (core.parse_locale('de_DE.iso885915@euro') ==
            ('de', 'DE', None, None))


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
    # Use pickle module rather than cPickle since cPickle.Unpickler is a method
    # on Python 2
    import pickle

    class Unpickler(pickle.Unpickler):

        def find_class(self, module, name):
            # *.dat files must have compatible classes between Python 2 and 3
            if module.split('.')[0] == 'babel':
                return pickle.Unpickler.find_class(self, module, name)
            raise pickle.UnpicklingError("global '%s.%s' is forbidden" %
                                         (module, name))

    import yatest.common as yc
    with open(yc.source_path("contrib/python/Babel/py2/{}".format(filename)), 'rb') as f:
        Unpickler(f).load()


def test_issue_601_no_language_name_but_has_variant():
    # kw_GB has a variant for Finnish but no actual language name for Finnish,
    # so `get_display_name()` previously crashed with a TypeError as it attempted
    # to concatenate " (Finnish)" to None.
    # Instead, it's better to return None altogether, as we can't reliably format
    # part of a language name.

    assert Locale.parse('fi_FI').get_display_name('kw_GB') == None
