from datetime import datetime, tzinfo

import pytest
from parameterized import param, parameterized

from dateparser import DateDataParser, parse
from dateparser.conf import SettingValidationError, apply_settings, settings
from tests import BaseTestCase


def test_function(settings=None):
    return settings


class TimeZoneSettingsTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.given_ds = NotImplemented
        self.result = NotImplemented
        self.timezone = NotImplemented
        self.confs = NotImplemented

    @parameterized.expand(
        [
            param(
                "12 Feb 2015 10:30 PM +0100",
                datetime(2015, 2, 12, 22, 30),
                r"UTC\+01:00",
            ),
            param("12 Feb 2015 4:30 PM EST", datetime(2015, 2, 12, 16, 30), "EST"),
            param("12 Feb 2015 8:30 PM PKT", datetime(2015, 2, 12, 20, 30), "PKT"),
            param("12 Feb 2015 8:30 PM ACT", datetime(2015, 2, 12, 20, 30), "ACT"),
        ]
    )
    def test_should_return_and_assert_tz(self, ds, dt, tz):
        self.given(ds)
        self.given_configurations({})
        self.when_date_is_parsed()
        self.then_date_is_tz_aware()
        self.then_date_is(dt)
        self.then_timezone_is(tz)

    @parameterized.expand(
        [
            param("12 Feb 2015 4:30 PM EST", datetime(2015, 2, 12, 16, 30), "EST"),
            param("12 Feb 2015 8:30 PM PKT", datetime(2015, 2, 12, 20, 30), "PKT"),
            param("12 Feb 2015 8:30 PM ACT", datetime(2015, 2, 12, 20, 30), "ACT"),
            param("12 Feb 2015 8:30 PM", datetime(2015, 2, 12, 20, 30), ""),
        ]
    )
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

    @parameterized.expand(
        [
            param(
                "12 Feb 2015 4:30 PM EST",
                datetime(2015, 2, 12, 16, 30),
            ),
            param(
                "12 Feb 2015 8:30 PM PKT",
                datetime(2015, 2, 12, 20, 30),
            ),
            param(
                "12 Feb 2015 8:30 PM ACT",
                datetime(2015, 2, 12, 20, 30),
            ),
            param(
                "12 Feb 2015 8:30 PM +0100",
                datetime(2015, 2, 12, 20, 30),
            ),
        ]
    )
    def test_should_return_naive_if_RETURN_AS_TIMEZONE_AWARE_IS_FALSE(self, ds, dt):
        self.given(ds)
        self.given_configurations({"RETURN_AS_TIMEZONE_AWARE": False})
        self.when_date_is_parsed()
        self.then_date_is(dt)
        self.then_date_is_not_tz_aware()

    def then_timezone_is(self, tzname):
        self.assertEqual(self.result.tzinfo.tzname(""), tzname)

    def given(self, ds):
        self.given_ds = ds

    def given_configurations(self, confs):
        if "TIMEZONE" not in confs:
            confs.update({"TIMEZONE": "local"})

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
        super().setUp()
        self.default_settings = settings

    def test_apply_settings_should_return_default_settings_when_no_settings_are_supplied_to_the_decorated_function(
        self,
    ):
        test_func = apply_settings(test_function)
        self.assertEqual(test_func(), self.default_settings)

    def test_apply_settings_should_return_non_default_settings_when_settings_are_supplied_to_the_decorated_function(
        self,
    ):
        test_func = apply_settings(test_function)
        self.assertNotEqual(
            test_func(settings={"PREFER_DATES_FROM": "past"}), self.default_settings
        )

    def test_apply_settings_shouldnt_create_new_settings_when_same_settings_are_supplied_to_the_decorated_function_more_than_once(  # noqa E501
        self,
    ):
        test_func = apply_settings(test_function)
        settings_once = test_func(settings={"PREFER_DATES_FROM": "past"})
        settings_twice = test_func(settings={"PREFER_DATES_FROM": "past"})
        self.assertEqual(settings_once, settings_twice)

    def test_apply_settings_should_return_default_settings_when_called_with_no_settings_after_once_called_with_settings_supplied_to_the_decorated_function(  # noqa E501
        self,
    ):
        test_func = apply_settings(test_function)
        settings_once = test_func(settings={"PREFER_DATES_FROM": "past"})
        settings_twice = test_func()
        self.assertNotEqual(settings_once, self.default_settings)
        self.assertEqual(settings_twice, self.default_settings)


class InvalidSettingsTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_error_is_raised_when_none_is_passed_in_settings(self):
        test_func = apply_settings(test_function)
        with self.assertRaisesRegex(TypeError, r"Invalid.*None\}"):
            test_func(settings={"PREFER_DATES_FROM": None})

        with self.assertRaisesRegex(TypeError, r"Invalid.*None\}"):
            test_func(settings={"TIMEZONE": None})

        with self.assertRaisesRegex(TypeError, r"Invalid.*None\}"):
            test_func(settings={"TO_TIMEZONE": None})

    def test_error_is_raised_for_invalid_type_settings(self):
        test_func = apply_settings(test_function)
        try:
            test_func(settings=["current_period", False, "current"])
        except Exception as error:
            self.error = error
            self.then_error_was_raised(
                TypeError,
                ["settings can only be either dict or instance of Settings class"],
            )

    def test_check_settings_wrong_setting_name(self):
        with self.assertRaisesRegex(
            SettingValidationError, r".* is not a valid setting"
        ):
            DateDataParser(settings={"AAAAA": "foo"})

    @parameterized.expand(
        [
            param("DATE_ORDER", 2, "YYY", "MDY"),
            param(
                "TIMEZONE", False, "", "Europe/Madrid"
            ),  # should we check valid timezones?
            param(
                "TO_TIMEZONE", True, "", "Europe/Madrid"
            ),  # should we check valid timezones?
            param("RETURN_AS_TIMEZONE_AWARE", "false", "", True),
            param("PREFER_DAY_OF_MONTH", False, "current_period", "current"),
            param("PREFER_DATES_FROM", True, "current", "current_period"),
            param("RELATIVE_BASE", "yesterday", "", datetime.now()),
            param("SKIP_TOKENS", "foo", "", ["foo"]),
            param("REQUIRE_PARTS", "day", "", ["month", "day"]),
            param("PARSERS", "absolute-time", "", ["absolute-time", "no-spaces-time"]),
            param("STRICT_PARSING", "true", "", True),
            param("RETURN_TIME_AS_PERIOD", "false", "", True),
            param("PREFER_LOCALE_DATE_ORDER", "true", "", False),
            param("NORMALIZE", "true", "", True),
            param("FUZZY", "true", "", False),
            param("PREFER_LOCALE_DATE_ORDER", "false", "", True),
            param("DEFAULT_LANGUAGES", "en", "", ["en"]),
            param("LANGUAGE_DETECTION_CONFIDENCE_THRESHOLD", "1", "", 0.5),
        ]
    )
    def test_check_settings(self, setting, wrong_type, wrong_value, valid_value):
        with self.assertRaisesRegex(
            SettingValidationError,
            r'"{}" must be .*, not "{}".'.format(setting, type(wrong_type).__name__),
        ):
            DateDataParser(settings={setting: wrong_type})

        if wrong_value:
            with self.assertRaisesRegex(
                SettingValidationError,
                r'"{}" is not a valid value for "{}", it should be: .*'.format(
                    str(wrong_value).replace("[", "\\[").replace("]", "\\]"), setting
                ),
            ):
                DateDataParser(settings={setting: wrong_value})

        # check that a valid value doesn't raise an error
        assert DateDataParser(settings={setting: valid_value})

    def test_check_settings_extra_check_require_parts(self):
        with self.assertRaisesRegex(
            SettingValidationError,
            r'"REQUIRE_PARTS" setting contains invalid values: time',
        ):
            DateDataParser(settings={"REQUIRE_PARTS": ["time", "day"]})
        with self.assertRaisesRegex(
            SettingValidationError,
            r'There are repeated values in the "REQUIRE_PARTS" setting',
        ):
            DateDataParser(settings={"REQUIRE_PARTS": ["month", "day", "month"]})

    def test_check_settings_extra_check_parsers(self):
        with self.assertRaisesRegex(
            SettingValidationError,
            r'Found unknown parsers in the "PARSERS" setting: no-spaces',
        ):
            DateDataParser(settings={"PARSERS": ["absolute-time", "no-spaces"]})

        with self.assertRaisesRegex(
            SettingValidationError,
            r'There are repeated values in the "PARSERS" setting',
        ):
            DateDataParser(
                settings={"PARSERS": ["absolute-time", "timestamp", "absolute-time"]}
            )

    def test_check_settings_extra_check_confidence_threshold(self):
        with self.assertRaisesRegex(
            SettingValidationError,
            r"1.1 is not a valid value for "
            r"LANGUAGE_DETECTION_CONFIDENCE_THRESHOLD. It can take values "
            r"between 0 and 1",
        ):
            DateDataParser(settings={"LANGUAGE_DETECTION_CONFIDENCE_THRESHOLD": 1.1})

    def test_check_settings_extra_check_default_languages(self):
        with self.assertRaisesRegex(
            SettingValidationError,
            "Found invalid languages in the 'DEFAULT_LANGUAGES' setting: 'abcd'",
        ):
            DateDataParser(settings={"DEFAULT_LANGUAGES": ["abcd"]})


@pytest.mark.parametrize(
    "date_string,expected_result",
    [
        # Note that these results are "valid" but probably they shouldn't be considered
        ("2020", datetime(1900, 1, 1, 20, 2)),
        ("333", datetime(1900, 1, 1, 3, 3, 3)),
        ("1000", datetime(1900, 1, 1, 10, 0)),
        ("100000", datetime(1900, 1, 1, 10, 0)),
    ],
)
def test_no_spaces_strict_parsing(date_string, expected_result):
    parser = DateDataParser(
        settings={"PARSERS": ["no-spaces-time"], "STRICT_PARSING": False}
    )
    assert parser.get_date_data(date_string)["date_obj"] == expected_result

    parser = DateDataParser(
        settings={"PARSERS": ["no-spaces-time"], "STRICT_PARSING": True}
    )
    assert parser.get_date_data(date_string)["date_obj"] is None


def detect_languages(text, confidence_threshold):
    if confidence_threshold > 0.5:
        return ["en"]
    else:
        return ["fr"]


def test_confidence_threshold_setting_is_applied():
    ddp = DateDataParser(
        detect_languages_function=detect_languages,
        settings={"LANGUAGE_DETECTION_CONFIDENCE_THRESHOLD": 0.6},
    )
    assert ddp.get_date_data("21/06/2020").locale == "en"

    ddp2 = DateDataParser(
        detect_languages_function=detect_languages,
        settings={"LANGUAGE_DETECTION_CONFIDENCE_THRESHOLD": 0.4},
    )
    assert ddp2.get_date_data("21/06/2020").locale == "fr"
