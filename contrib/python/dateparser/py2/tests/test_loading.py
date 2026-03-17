# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from parameterized import parameterized, param
from operator import attrgetter
import six
import regex as re

from dateparser.languages.loader import default_loader
from tests import BaseTestCase


class TestLoading(BaseTestCase):
    def setUp(self):
        super(TestLoading, self).setUp()
        self.locale_generator = NotImplemented

    @classmethod
    def setUpClass(cls):
        cls.data_loader = default_loader
        cls.data_loader._loaded_locales = {}
        cls.data_loader._loaded_languages = {}

    @parameterized.expand([
        param(given_languages=['es', 'fr', 'en'],
              given_locales=None,
              given_region=None,
              loaded_languages=['en', 'es', 'fr'],
              loaded_locales=['en', 'es', 'fr'],
              expected_locales=['es', 'fr', 'en']),

        param(given_languages=None,
              given_locales=['fr-BF', 'ar-SO', 'asa'],
              given_region=None,
              loaded_languages=['en', 'es', 'fr', 'ar', 'asa'],
              loaded_locales=['en', 'es', 'fr', 'fr-BF', 'ar-SO', 'asa'],
              expected_locales=['fr-BF', 'ar-SO', 'asa']),

        param(given_languages=None,
              given_locales=['nl-CW', 'so-KE', 'sr-Latn-XK', 'zgh'],
              given_region=None,
              loaded_languages=['en', 'es', 'fr', 'ar', 'asa',
                                'nl', 'so', 'sr-Latn', 'zgh'],
              loaded_locales=['en', 'es', 'fr', 'fr-BF', 'ar-SO', 'asa',
                              'nl-CW', 'so-KE', 'sr-Latn-XK', 'zgh'],
              expected_locales=['nl-CW', 'so-KE', 'sr-Latn-XK', 'zgh']),

        param(given_languages=['pt', 'zh-Hant', 'zh-Hans'],
              given_locales=None,
              given_region='MO',
              loaded_languages=['en', 'es', 'fr', 'ar', 'asa', 'nl', 'so',
                                'sr-Latn', 'zgh', 'pt', 'zh-Hant', 'zh-Hans'],
              loaded_locales=['en', 'es', 'fr', 'fr-BF', 'ar-SO', 'asa',
                              'nl-CW', 'so-KE', 'sr-Latn-XK', 'zgh',
                              'pt-MO', 'zh-Hant-MO', 'zh-Hans-MO'],
              expected_locales=['pt-MO', 'zh-Hant-MO', 'zh-Hans-MO']),

        param(given_languages=['pt', 'he'],
              given_locales=['ru-UA', 'ckb-IR', 'sq-XK'],
              given_region='MO',
              loaded_languages=['en', 'es', 'fr', 'ar', 'asa', 'nl', 'so',
                                'sr-Latn', 'zgh', 'pt', 'zh-Hant', 'zh-Hans',
                                'ru', 'ckb', 'sq'],
              loaded_locales=['en', 'es', 'fr', 'fr-BF', 'ar-SO', 'asa', 'nl-CW',
                              'so-KE', 'sr-Latn-XK', 'zgh', 'pt-MO', 'zh-Hant-MO',
                              'zh-Hans-MO', 'ru-UA', 'ckb-IR', 'sq-XK'],
              expected_locales=['ru-UA', 'ckb-IR', 'sq-XK']),

        param(given_languages=['da', 'ja'],
              given_locales=['shi-Latn', 'teo-KE', 'ewo', 'vun'],
              given_region='AO',
              loaded_languages=['en', 'es', 'fr', 'ar', 'asa', 'nl', 'so',
                                'sr-Latn', 'zgh', 'pt', 'zh-Hant', 'zh-Hans',
                                'ru', 'ckb', 'sq', 'shi-Latn', 'teo', 'ewo', 'vun'],
              loaded_locales=['en', 'es', 'fr', 'fr-BF', 'ar-SO', 'asa', 'nl-CW',
                              'so-KE', 'sr-Latn-XK', 'zgh', 'pt-MO', 'zh-Hant-MO',
                              'zh-Hans-MO', 'ru-UA', 'ckb-IR', 'sq-XK', 'shi-Latn',
                              'teo-KE', 'ewo', 'vun'],
              expected_locales=['shi-Latn', 'teo-KE', 'ewo', 'vun']),
    ])
    def test_loading(self, given_languages, given_locales, given_region,
                     loaded_languages, loaded_locales, expected_locales):
        self.load_data(languages=given_languages, locales=given_locales, region=given_region)
        self.then_locales_are_yielded_in_order(expected_locales)
        self.then_loaded_languages_are(loaded_languages)
        self.then_loaded_locales_are(loaded_locales)

    def load_data(self, languages, locales, region):
        self.locale_generator = self.data_loader.get_locales(
            languages=languages, locales=locales, region=region, use_given_order=True)

    def then_loaded_languages_are(self, loaded_languages):
        six.assertCountEqual(self, loaded_languages, self.data_loader._loaded_languages.keys())

    def then_loaded_locales_are(self, loaded_locales):
        six.assertCountEqual(self, loaded_locales, self.data_loader._loaded_locales.keys())

    def then_locales_are_yielded_in_order(self, expected_locales):
        self.assertEqual(list(map(attrgetter('shortname'),
                         list(self.locale_generator))), expected_locales)


class TestLocaleDataLoader(BaseTestCase):
    UNKNOWN_LANGUAGES_EXCEPTION_RE = re.compile(r"Unknown language\(s\): (.+)")
    UNKNOWN_LOCALES_EXCEPTION_RE = re.compile(r"Unknown locale\(s\): (.+)")

    def setUp(self):
        super(TestLocaleDataLoader, self).setUp()
        self.data_loader = default_loader
        self.data_loader._loaded_languages = {}
        self.data_loader._loaded_locales = {}

    @parameterized.expand([
        param(given_locales=['es-MX', 'ar-EG', 'fr-DJ']),
        param(given_locales=['pt-MO', 'ru-KZ', 'es-CU']),
        param(given_locales=['zh-Hans-HK', 'en-VG', 'my']),
        param(given_locales=['tl', 'nl-SX', 'de-BE']),
    ])
    def test_loading_with_given_order(self, given_locales):
        self.load_data(given_locales, use_given_order=True)
        self.then_locales_are_yielded_in_order(given_locales)

    @parameterized.expand([
        param(given_locales=['os-RU', 'ln-CF', 'ee-TG'],
              expected_locales=['ee-TG', 'ln-CF', 'os-RU']),
        param(given_locales=['khq', 'ff-CM'],
              expected_locales=['ff-CM', 'khq']),
        param(given_locales=['en-CC', 'fr-BE', 'ar-KW'],
              expected_locales=['en-CC', 'ar-KW', 'fr-BE']),
    ])
    def test_loading_without_given_order(self, given_locales, expected_locales):
        self.load_data(given_locales, use_given_order=False)
        self.then_locales_are_yielded_in_order(expected_locales)

    @parameterized.expand([
        param(given_locales=['sw-KE', 'ru-UA', 'he']),
        param(given_locales=['de-IT', 'ta-MY', 'pa-Arab']),
        param(given_locales=['bn-IN', 'pt-ST', 'ko-KP', 'ta']),
        param(given_locales=['fr-NE', 'ar-SY']),
    ])
    def test_get_locale_map_with_given_order(self, given_locales):
        self.given_locale_map(locales=given_locales, use_given_order=True)
        self.then_locale_map_in_order(given_locales)

    @parameterized.expand([
        param(given_locales=['en-FJ', 'pt-CV', 'fr-RW'],
              expected_locales=['en-FJ', 'fr-RW', 'pt-CV']),
        param(given_locales=['pt-AO', 'hi', 'zh-Hans-SG', 'vi'],
              expected_locales=['zh-Hans-SG', 'hi', 'pt-AO', 'vi']),
        param(given_locales=['gsw-FR', 'es-BZ', 'ca-IT', 'qu-EC'],
              expected_locales=['es-BZ', 'qu-EC', 'ca-IT', 'gsw-FR']),
    ])
    def test_get_locale_map_without_given_order(self, given_locales, expected_locales):
        self.given_locale_map(locales=given_locales, use_given_order=False)
        self.then_locale_map_in_order(expected_locales)

    @parameterized.expand([
        param(given_languages=['es', 'ar-001', 'xx'],
              unknown_languages=['ar-001', 'xx']),
        param(given_languages=['sr-Latn', 'sq', 'ii-Latn'],
              unknown_languages=['ii-Latn']),
        param(given_languages=['vol', 'bc'],
              unknown_languages=['vol', 'bc']),
    ])
    def test_error_raised_for_unknown_languages(self, given_languages, unknown_languages):
        self.given_locale_map(languages=given_languages)
        self.then_error_for_unknown_languages_raised(unknown_languages)

    @parameterized.expand([
        param(given_locales=['es-AB', 'ar-001', 'fr-DJ'],
              unknown_locales=['es-AB', 'ar-001']),
        param(given_locales=['ru-MD', 'my-MY', 'zz'],
              unknown_locales=['my-MY', 'zz']),
        param(given_locales=['nl-SX', 'be-BE', 'ca-FR'],
              unknown_locales=['be-BE']),
    ])
    def test_error_raised_for_unknown_locales(self, given_locales, unknown_locales):
        self.given_locale_map(locales=given_locales)
        self.then_error_for_unknown_locales_raised(unknown_locales)

    @parameterized.expand([
        param(given_locales=['en-TK', 'en-TO', 'zh']),
        param(given_locales=['es-PY', 'es-IC', 'ja', 'es-DO']),
        param(given_locales=['ca-TA', 'ca', 'fr']),
    ])
    def test_error_raised_for_conflicting_locales(self, given_locales):
        self.given_locale_map(locales=given_locales)
        self.then_error_was_raised(
            ValueError, "Locales should not have same language and different region")

    @parameterized.expand([
        param(given_locales=['en-TK', 'en-TO', 'zh']),
        param(given_locales=['es-PY', 'es-IC', 'ja', 'es-DO']),
        param(given_locales=['af-NA', 'da', 'af']),
    ])
    def test_conflicting_locales_load_if_allow_conflicting_locales(self, given_locales):
        self.load_data(given_locales, use_given_order=True, allow_conflicting_locales=True)
        self.then_locales_are_yielded_in_order(given_locales)

    def load_data(self, given_locales, use_given_order=False, allow_conflicting_locales=False):
        self.locale_generator = self.data_loader.get_locales(
            locales=given_locales, use_given_order=use_given_order,
            allow_conflicting_locales=allow_conflicting_locales)

    def given_locale_map(self, languages=None, locales=None, use_given_order=True):
        try:
            self.locale_map = self.data_loader.get_locale_map(
                languages=languages, locales=locales, use_given_order=use_given_order)
        except Exception as error:
            self.error = error

    def then_locales_are_yielded_in_order(self, expected_locales):
        self.assertEqual(list(map(attrgetter('shortname'),
                         list(self.locale_generator))), expected_locales)

    def then_locale_map_in_order(self, expected_locales):
        self.assertEqual(list(self.locale_map.keys()), expected_locales)

    def then_error_for_unknown_languages_raised(self, unknown_languages):
        self.assertIsInstance(self.error, ValueError)
        match = self.UNKNOWN_LANGUAGES_EXCEPTION_RE.match(str(self.error))
        self.assertTrue(match)
        languages = match.group(1).split(", ")
        six.assertCountEqual(self, languages, [repr(l) for l in unknown_languages])

    def then_error_for_unknown_locales_raised(self, unknown_locales):
        self.assertIsInstance(self.error, ValueError)
        match = self.UNKNOWN_LOCALES_EXCEPTION_RE.match(str(self.error))
        self.assertTrue(match)
        locales = match.group(1).split(", ")
        six.assertCountEqual(self, locales, [repr(l) for l in unknown_locales])
