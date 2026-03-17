# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import six
from parameterized import parameterized, param
from datetime import datetime, tzinfo

from tests import BaseTestCase

from dateparser.conf import settings
from dateparser.conf import apply_settings

from dateparser import parse


def test_function(settings=None):
    return settings


class TimeZoneSettingsTest(BaseTestCase):
    def setUp(self):
        super(TimeZoneSettingsTest, self).setUp()
        self.given_ds = NotImplemented
        self.result = NotImplemented
        self.timezone = NotImplemented
        self.confs = NotImplemented

    @parameterized.expand([
        param('12 Feb 2015 10:30 PM +0100', datetime(2015, 2, 12, 22, 30), r'UTC\+01:00'),
        param('12 Feb 2015 4:30 PM EST', datetime(2015, 2, 12, 16, 30), 'EST'),
        param('12 Feb 2015 8:30 PM PKT', datetime(2015, 2, 12, 20, 30), 'PKT'),
        param('12 Feb 2015 8:30 PM ACT', datetime(2015, 2, 12, 20, 30), 'ACT'),
        ])
    def test_should_return_and_assert_tz(self, ds, dt, tz):
        self.given(ds)
        self.given_configurations({})
        self.when_date_is_parsed()
        self.then_date_is_tz_aware()
        self.then_date_is(dt)
        self.then_timezone_is(tz)

    @parameterized.expand([
        param('12 Feb 2015 4:30 PM EST', datetime(2015, 2, 12, 16, 30), 'EST'),
        param('12 Feb 2015 8:30 PM PKT', datetime(2015, 2, 12, 20, 30), 'PKT'),
        param('12 Feb 2015 8:30 PM ACT', datetime(2015, 2, 12, 20, 30), 'ACT'),
        param('12 Feb 2015 8:30 PM', datetime(2015, 2, 12, 20, 30), ''),
        ])
    def test_only_return_explicit_timezone(self, ds, dt, tz):
        self.given(ds)
        self.given_configurations({})
        self.when_date_is_parsed()
        self.then_date_is(dt)
        if tz:
            self.then_date_is_tz_aware()
            self.then_timezone_is(tz)
        else:
            self.then_date_is_not_tz_aware()

    @parameterized.expand([
        param('12 Feb 2015 4:30 PM EST', datetime(2015, 2, 12, 16, 30),),
        param('12 Feb 2015 8:30 PM PKT', datetime(2015, 2, 12, 20, 30),),
        param('12 Feb 2015 8:30 PM ACT', datetime(2015, 2, 12, 20, 30),),
        param('12 Feb 2015 8:30 PM +0100', datetime(2015, 2, 12, 20, 30),),
        ])
    def test_should_return_naive_if_RETURN_AS_TIMEZONE_AWARE_IS_FALSE(self, ds, dt):
        self.given(ds)
        self.given_configurations({'RETURN_AS_TIMEZONE_AWARE': False})
        self.when_date_is_parsed()
        self.then_date_is(dt)
        self.then_date_is_not_tz_aware()

    def then_timezone_is(self, tzname):
        self.assertEqual(self.result.tzinfo.tzname(''), tzname)

    def given(self, ds):
        self.given_ds = ds

    def given_configurations(self, confs):
        if 'TIMEZONE' not in confs:
            confs.update({'TIMEZONE': 'local'})

        self.confs = settings.replace(**confs)

    def when_date_is_parsed(self):
        self.result = parse(self.given_ds, settings=(self.confs or {}))

    def then_date_is_tz_aware(self):
        self.assertIsInstance(self.result.tzinfo, tzinfo)

    def then_date_is_not_tz_aware(self):
        self.assertIsNone(self.result.tzinfo)

    def then_date_is(self, date):
        dtc = self.result.replace(tzinfo=None)
        self.assertEqual(dtc, date)


class SettingsTest(BaseTestCase):

    def setUp(self):
        super(SettingsTest, self).setUp()
        self.default_settings = settings

    def test_apply_settings_should_return_default_settings_when_no_settings_are_supplied_to_the_decorated_function(self):
        test_func = apply_settings(test_function)
        self.assertEqual(test_func(), self.default_settings)

    def test_apply_settings_should_return_non_default_settings_when_settings_are_supplied_to_the_decorated_function(self):
        test_func = apply_settings(test_function)
        self.assertNotEqual(test_func(settings={'PREFER_DATES_FROM': 'past'}), self.default_settings)

    def test_apply_settings_should_not_create_new_settings_when_same_settings_are_supplied_to_the_decorated_function_more_than_once(self):
        test_func = apply_settings(test_function)
        settings_once = test_func(settings={'PREFER_DATES_FROM': 'past'})
        settings_twice = test_func(settings={'PREFER_DATES_FROM': 'past'})
        self.assertEqual(settings_once, settings_twice)

    def test_apply_settings_should_return_default_settings_when_called_with_no_settings_after_once_called_with_settings_supplied_to_the_decorated_function(self):
        test_func = apply_settings(test_function)
        settings_once = test_func(settings={'PREFER_DATES_FROM': 'past'})
        settings_twice = test_func()
        self.assertNotEqual(settings_once, self.default_settings)
        self.assertEqual(settings_twice, self.default_settings)


class InvalidSettingsTest(BaseTestCase):

    def setUp(self):
        super(InvalidSettingsTest, self).setUp()

    def test_error_is_raised_when_none_is_passed_in_settings(self):
        test_func = apply_settings(test_function)
        with six.assertRaisesRegex(self, TypeError, r'Invalid.*None\}'):
            test_func(settings={'PREFER_DATES_FROM': None})

        with six.assertRaisesRegex(self, TypeError, r'Invalid.*None\}'):
            test_func(settings={'TIMEZONE': None})

        with six.assertRaisesRegex(self, TypeError, r'Invalid.*None\}'):
            test_func(settings={'TO_TIMEZONE': None})

    def test_error_is_raised_for_invalid_type_settings(self):
        test_func = apply_settings(test_function)
        try:
            test_func(settings=['current_period', False, 'current'])
        except Exception as error:
            self.error = error
            self.then_error_was_raised(TypeError, ["settings can only be either dict or instance of Settings class"])
