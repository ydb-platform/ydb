# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from itertools import chain
import regex as re
import six
from parameterized import parameterized

from dateparser.languages import default_loader
from dateparser.data import language_locale_dict
from tests import BaseTestCase

DEFAULT_MONTH_PATTERN = re.compile(r'^M?\d+$', re.U)
INVALID_AM_PM_PATTERN = re.compile(r'^[AaPp]\.?\s+[Mm]$')

all_locale_shortnames = list(chain(language_locale_dict.keys(), *language_locale_dict.values()))
all_locales = list(default_loader.get_locales(locales=all_locale_shortnames,
                                              allow_conflicting_locales=True))
all_locale_params = [[locale] for locale in all_locales]

VALID_KEYS = [
    'name', 'date_order', 'skip', 'pertain', 'simplifications', 'no_word_spacing', 'ago',
    'in', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
    'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september',
    'october', 'november', 'december', 'year', 'month', 'week', 'day', 'hour', 'minute',
    'second', 'am', 'pm', 'relative-type', 'relative-type-regex', 'sentence_splitter_group']

NECESSARY_KEYS = ['name', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday',
                  'sunday', 'january', 'february', 'march', 'april', 'may', 'june', 'july',
                  'august', 'september', 'october', 'november', 'december', 'year', 'month',
                  'week', 'day', 'hour', 'minute', 'second']

MONTH_NAMES = ['january', 'february', 'march', 'april', 'may', 'june', 'july',
               'august', 'september', 'october', 'november', 'december']


def is_invalid_translation(translation):
    return (not (translation and isinstance(translation, six.text_type))) or '.' in translation


def is_invalid_month_translation(translation):
    return ((not (translation and isinstance(translation, six.text_type))) or
            '.' in translation or DEFAULT_MONTH_PATTERN.match(translation))


def is_invalid_am_pm_translation(translation):
    return ((not (translation and isinstance(translation, six.text_type))) or
            '.' in translation or INVALID_AM_PM_PATTERN.match(translation))


def is_invalid_simplification(simplification):
    if not isinstance(simplification, dict) or len(simplification) != 1:
        return True
    key, value = list(simplification.items())[0]
    return not isinstance(key, six.text_type) or not isinstance(value, six.text_type)


def is_invalid_relative_mapping(relative_mapping):
    key, value = relative_mapping
    if not (key and value and isinstance(key, six.text_type) and isinstance(value, list)):
        return True
    return not all([isinstance(x, six.text_type) for x in value])


def is_invalid_relative_regex_mapping(relative_regex_mapping):
    key, value = relative_regex_mapping
    if not (key and value and isinstance(key, six.text_type) and isinstance(value, list)):
        return True
    if '\\1' not in key:
        return True
    return not (all([isinstance(x, six.text_type) for x in value]) and
                all(['(\\d+)' in x for x in value]))


class TestLocaleInfo(BaseTestCase):
    def setUp(self):
        super(TestLocaleInfo, self).setUp()
        self.info = NotImplemented
        self.shortname = NotImplemented

    @parameterized.expand(all_locale_params)
    def test_extra_keys(self, locale):
        self.given_locale_info(locale)
        extra_keys = list(set(self.info.keys()) - set(VALID_KEYS))
        self.assertFalse(
            extra_keys, 'Extra keys found for {}: {}'.format(self.shortname, extra_keys))

    @parameterized.expand(all_locale_params)
    def test_necessary_keys(self, locale):
        self.given_locale_info(locale)
        self.then_keys_present_in_info(NECESSARY_KEYS)
        self.then_translations_present_in_info(NECESSARY_KEYS)

    @parameterized.expand(all_locale_params)
    def test_name(self, locale):
        self.given_locale_info(locale)
        self.then_name_is_valid()

    @parameterized.expand(all_locale_params)
    def test_date_order(self, locale):
        self.given_locale_info(locale)
        self.then_date_order_is_valid()

    @parameterized.expand(all_locale_params)
    def test_january_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('january')

    @parameterized.expand(all_locale_params)
    def test_february_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('february')

    @parameterized.expand(all_locale_params)
    def test_march_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('march')

    @parameterized.expand(all_locale_params)
    def test_april_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('april')

    @parameterized.expand(all_locale_params)
    def test_may_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('may')

    @parameterized.expand(all_locale_params)
    def test_june_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('june')

    @parameterized.expand(all_locale_params)
    def test_july_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('july')

    @parameterized.expand(all_locale_params)
    def test_august_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('august')

    @parameterized.expand(all_locale_params)
    def test_september_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('september')

    @parameterized.expand(all_locale_params)
    def test_october_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('october')

    @parameterized.expand(all_locale_params)
    def test_november_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('november')

    @parameterized.expand(all_locale_params)
    def test_december_translation(self, locale):
        self.given_locale_info(locale)
        self.then_month_translations_are_valid('december')

    @parameterized.expand(all_locale_params)
    def test_am_translation(self, locale):
        self.given_locale_info(locale)
        self.then_am_pm_translations_are_valid('am')

    @parameterized.expand(all_locale_params)
    def test_pm_translation(self, locale):
        self.given_locale_info(locale)
        self.then_am_pm_translations_are_valid('pm')

    @parameterized.expand(all_locale_params)
    def test_sunday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('sunday')

    @parameterized.expand(all_locale_params)
    def test_monday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('monday')

    @parameterized.expand(all_locale_params)
    def test_tuesday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('tuesday')

    @parameterized.expand(all_locale_params)
    def test_wednesday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('wednesday')

    @parameterized.expand(all_locale_params)
    def test_thursday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('thursday')

    @parameterized.expand(all_locale_params)
    def test_friday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('friday')

    @parameterized.expand(all_locale_params)
    def test_saturday_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('saturday')

    @parameterized.expand(all_locale_params)
    def test_year_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('year')

    @parameterized.expand(all_locale_params)
    def test_month_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('month')

    @parameterized.expand(all_locale_params)
    def test_week_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('week')

    @parameterized.expand(all_locale_params)
    def test_day_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('day')

    @parameterized.expand(all_locale_params)
    def test_hour_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('hour')

    @parameterized.expand(all_locale_params)
    def test_minute_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('minute')

    @parameterized.expand(all_locale_params)
    def test_second_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('second')

    @parameterized.expand(all_locale_params)
    def test_ago_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('ago')

    @parameterized.expand(all_locale_params)
    def test_in_translation(self, locale):
        self.given_locale_info(locale)
        self.then_translations_are_valid('in')

    @parameterized.expand(all_locale_params)
    def test_skip_tokens(self, locale):
        self.given_locale_info(locale)
        self.then_skip_pertain_tokens_are_valid('skip')

    @parameterized.expand(all_locale_params)
    def test_pertain_tokens(self, locale):
        self.given_locale_info(locale)
        self.then_skip_pertain_tokens_are_valid('pertain')

    @parameterized.expand(all_locale_params)
    def test_simplifications(self, locale):
        self.given_locale_info(locale)
        self.then_simplifications_are_valid()

    @parameterized.expand(all_locale_params)
    def test_relative_type(self, locale):
        self.given_locale_info(locale)
        self.then_relative_type_is_valid()

    @parameterized.expand(all_locale_params)
    def test_relative_type_regex(self, locale):
        self.given_locale_info(locale)
        self.then_relative_type_regex_is_valid()

    @parameterized.expand(all_locale_params)
    def test_no_word_spacing(self, locale):
        self.given_locale_info(locale)
        self.then_no_word_spacing_is_valid()

    def given_locale_info(self, locale):
        self.info = locale.info
        self.shortname = locale.shortname

    def then_keys_present_in_info(self, keys_list):
        absent_keys = list(set(keys_list) - set(self.info.keys()))
        self.assertFalse(
            absent_keys, '{} not found in locale {}'.format(', '.join(absent_keys), self.shortname))

    def then_translations_present_in_info(self, keys_list):
        no_translation_keys = [key for key in keys_list if not self.info.get(key)]
        self.assertFalse(no_translation_keys, 'Translations for {} not found in locale {}'.format(
                ', '.join(no_translation_keys), self.shortname))

    def then_name_is_valid(self):
        name = self.info['name']
        self.assertIsInstance(name, six.text_type,
                              'Invalid type for name: {} for locale {}'.format(
                               type(name).__name__, self.shortname))
        self.assertEqual(name, self.shortname,
                         'Invalid name: {} for locale {}'.format(name, self.shortname))

    def then_date_order_is_valid(self):
        if 'date_order' in self.info:
            date_order = self.info['date_order']
            self.assertIsInstance(date_order, six.text_type,
                                  'Invalid type for date_order: {} for locale {}'.format(
                                   type(date_order).__name__, self.shortname))
            self.assertIn(date_order, ['DMY', 'DYM', 'MDY', 'MYD', 'YDM', 'YMD'],
                          'Invalid date_order {} for {}'.format(date_order, self.shortname))

    def then_month_translations_are_valid(self, month):
        if month in self.info:
            month_translations = self.info[month]
            self.assertIsInstance(month_translations, list,
                                  'Invalid type for {}: {} for locale {}'.format(
                                   month, type(month_translations).__name__, self.shortname))
            invalid_translations = list(filter(is_invalid_month_translation, month_translations))
            self.assertFalse(invalid_translations,
                             'Invalid translations for {}: {} for locale {}'.format(
                              month, ', '.join(map(repr, invalid_translations)), self.shortname))

    def then_am_pm_translations_are_valid(self, key):
        if key in self.info:
            translations_list = self.info[key]
            self.assertIsInstance(translations_list, list,
                                  'Invalid type for {}: {} for locale {}'.format(
                                   key, type(translations_list).__name__, self.shortname))
            invalid_translations = list(filter(is_invalid_am_pm_translation, translations_list))
            self.assertFalse(invalid_translations,
                             'Invalid translations for {}: {} for locale {}'.format(
                              key, ', '.join(map(repr, invalid_translations)), self.shortname))

    def then_translations_are_valid(self, key):
        if key in self.info:
            translations_list = self.info[key]
            self.assertIsInstance(translations_list, list,
                                  'Invalid type for {}: {} for locale {}'.format(
                                   key, type(translations_list).__name__, self.shortname))
            invalid_translations = list(filter(is_invalid_translation, translations_list))
            self.assertFalse(invalid_translations,
                             'Invalid translations for {}: {} for locale {}'.format(
                              key, ', '.join(map(repr, invalid_translations)), self.shortname))

    def then_skip_pertain_tokens_are_valid(self, key):
        if key in self.info:
            tokens_list = self.info[key]
            self.assertIsInstance(tokens_list, list,
                                  'Invalid type for {}: {} for locale {}'.format(
                                   key, type(tokens_list).__name__, self.shortname))
            invalid_tokens = [token for token in tokens_list if not token or
                              not isinstance(token, six.text_type)]
            self.assertFalse(invalid_tokens,
                             'Invalid tokens for {}: {} for locale {}'.format(
                              key, ', '.join(map(repr, invalid_tokens)), self.shortname))

    def then_simplifications_are_valid(self):
        if 'simplifications' in self.info:
            simplifications_list = self.info['simplifications']
            self.assertIsInstance(simplifications_list, list,
                                  'Invalid type for simplifications: {} for locale {}'.format(
                                   type(simplifications_list).__name__, self.shortname))
            invalid_simplifications = list(filter(is_invalid_simplification, simplifications_list))
            self.assertFalse(invalid_simplifications,
                             'Invalid simplifications {} for locale {}'.format(
                              ', '.join(map(repr, invalid_simplifications)), self.shortname) +
                             ', each simplification must be a single unicode to unicode mapping')

    def then_relative_type_is_valid(self):
        if 'relative-type' in self.info:
            relative_type_dict = self.info['relative-type']
            self.assertIsInstance(relative_type_dict, dict,
                                  'Invalid type for relative-type: {} for locale {}'.format(
                                   type(relative_type_dict).__name__, self.shortname))
            invalid_relative_type = list(filter(is_invalid_relative_mapping,
                                                relative_type_dict.items()))
            self.assertFalse(invalid_relative_type,
                             'Invalid relative-type mappings {} for locale {}'.format(
                              ', '.join(map(repr, invalid_relative_type)), self.shortname) +
                             ', each mapping must be a unicode to list (of unicode) mapping')

    def then_relative_type_regex_is_valid(self):
        if 'relative-type-regex' in self.info:
            relative_type_dict = self.info['relative-type-regex']
            self.assertIsInstance(relative_type_dict, dict,
                                  'Invalid type for relative-type-regex: {} for locale {}'.format(
                                   type(relative_type_dict).__name__, self.shortname))
            invalid_relative_type_regex = list(filter(is_invalid_relative_regex_mapping,
                                                      relative_type_dict.items()))
            self.assertFalse(invalid_relative_type_regex,
                             'Invalid relative-type-regex mappings {} for locale {}'.format(
                              ', '.join(map(repr, invalid_relative_type_regex)), self.shortname) +
                             ', each mapping must be a unicode to list (of unicode) mapping')

    def then_no_word_spacing_is_valid(self):
        if 'no_word_spacing' in self.info:
            no_word_spacing = self.info['no_word_spacing']
            self.assertIsInstance(no_word_spacing, six.text_type,
                                  'Invalid type for no_word_spacing: {} for locale {}'.format(
                                   type(no_word_spacing).__name__, self.shortname))
            self.assertIn(no_word_spacing, ['True', 'False'],
                          'Invalid no_word_spacing {} for {}'.format(no_word_spacing, self.shortname))
