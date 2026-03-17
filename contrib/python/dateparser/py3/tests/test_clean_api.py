from datetime import date, datetime

from parameterized import param, parameterized
from pytz import utc

import dateparser
from tests import BaseTestCase


class TestParseFunction(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.result = NotImplemented

    @parameterized.expand(
        [
            param(date_string="24 de Janeiro de 2014", expected_date=date(2014, 1, 24)),
            param(date_string="2 de Enero de 2013", expected_date=date(2013, 1, 2)),
            param(date_string="January 25, 2014", expected_date=date(2014, 1, 25)),
        ]
    )
    def test_parse_dates_in_different_languages(self, date_string, expected_date):
        self.when_date_is_parsed_with_defaults(date_string)
        self.then_parsed_date_is(expected_date)

    @parameterized.expand(
        [
            param(
                date_string="May 5, 2000 13:00",
                expected_date=datetime(2000, 5, 5, 13, 0),
            ),
            param(
                date_string="August 8, 2018 5 PM",
                expected_date=datetime(2018, 8, 8, 17, 0),
            ),
            param(
                date_string="February 26, 1981 5 am UTC",
                expected_date=datetime(1981, 2, 26, 5, 0, tzinfo=utc),
            ),
        ]
    )
    def test_parse_dates_with_specific_time(self, date_string, expected_date):
        self.when_date_is_parsed_with_defaults(date_string)
        self.then_parsed_date_and_time_is(expected_date)

    @parameterized.expand(
        [
            param(
                date_string="May 5, 2000 13:00",
                expected_date=datetime(2000, 5, 5, 13, 0),
                relative=datetime(2000, 1, 1, 0, 0, tzinfo=utc),
            ),
            param(
                date_string="August 8, 2018 5 PM",
                expected_date=datetime(2018, 8, 8, 17, 0),
                relative=datetime(1900, 5, 5, 0, 0, tzinfo=utc),
            ),
            param(
                date_string="February 26, 1981 5 am UTC",
                expected_date=datetime(1981, 2, 26, 5, 0, tzinfo=utc),
                relative=datetime(1981, 2, 26, 5, 0, tzinfo=utc),
            ),
        ]
    )
    def test_parse_dates_with_specific_time_and_settings(
        self, date_string, expected_date, relative
    ):
        self.when_date_is_parsed_with_settings(
            date_string, settings={"RELATIVE_BASE": relative}
        )
        self.then_parsed_date_and_time_is(expected_date)

    @parameterized.expand(
        [
            param(
                date_string="24 de Janeiro de 2014",
                languages=["pt"],
                expected_date=date(2014, 1, 24),
            ),
        ]
    )
    def test_dates_which_match_languages_are_parsed(
        self, date_string, languages, expected_date
    ):
        self.when_date_is_parsed(date_string, languages=languages)
        self.then_parsed_date_is(expected_date)

    @parameterized.expand(
        [
            param(date_string="January 24, 2014", languages=["pt"]),
        ]
    )
    def test_dates_which_do_not_match_languages_are_not_parsed(
        self, date_string, languages
    ):
        self.when_date_is_parsed(date_string, languages=languages)
        self.then_date_was_not_parsed()

    @parameterized.expand(
        [
            param(
                date_string="24 de Janeiro de 2014",
                locales=["pt-TL"],
                expected_date=date(2014, 1, 24),
            ),
        ]
    )
    def test_dates_which_match_locales_are_parsed(
        self, date_string, locales, expected_date
    ):
        self.when_date_is_parsed(date_string, locales=locales)
        self.then_parsed_date_is(expected_date)

    @parameterized.expand(
        [
            param(
                date_string="0:4",
                locales=["fr-PF"],
                languages=["en"],
                region="",
                date_formats=["%a", "%a", "%a", "%a"],
                expected_date=datetime(1969, 1, 31, 13, 4),
            )
        ]
    )
    def test_dates_parse_utc_offset_does_not_throw(
        self, date_string, locales, languages, region, date_formats, expected_date
    ):
        """
        Bug discovered by OSSFuzz that caused an exception in pytz to halt parsing
        Regression test to ensure that this is not reintroduced
        """
        self.when_date_is_parsed_with_args_and_settings(
            date_string,
            languages=languages,
            locales=locales,
            region=region,
            date_formats=date_formats,
            settings={
                "CACHE_SIZE_LIMIT": 1000,
                "DATE_ORDER": "YDM",
                "DEFAULT_LANGUAGES": [
                    "mzn",
                    "as",
                    "af",
                    "fur",
                    "sr-Cyrl",
                    "kw",
                    "ne",
                    "en",
                    "vi",
                    "teo",
                    "sr",
                    "cgg",
                ],
                "LANGUAGE_DETECTION_CONFIDENCE_THRESHOLD": 0.18823535008398845,
                "NORMALIZE": True,
                "PARSERS": ["custom-formats", "absolute-time"],
                "PREFER_DATES_FROM": "past",
                "PREFER_DAY_OF_MONTH": "first",
                "PREFER_LOCALE_DATE_ORDER": True,
                "PREFER_MONTH_OF_YEAR": "current",
                "RELATIVE_BASE": datetime(
                    year=1970, month=1, day=1, hour=0, minute=0, second=0
                ),
                "REQUIRE_PARTS": [],
                "RETURN_AS_TIMEZONE_AWARE": False,
                "RETURN_TIME_AS_PERIOD": False,
                "SKIP_TOKENS": [],
                "STRICT_PARSING": False,
                "TIMEZONE": "America/Hermosillo",
                "TO_TIMEZONE": "Asia/Almaty",
            },
        )
        self.then_parsed_date_and_time_is(expected_date)

    @parameterized.expand(
        [
            param(date_string="January 24, 2014", locales=["pt-AO"]),
        ]
    )
    def test_dates_which_do_not_match_locales_are_not_parsed(
        self, date_string, locales
    ):
        self.when_date_is_parsed(date_string, locales=locales)
        self.then_date_was_not_parsed()

    def when_date_is_parsed_with_defaults(self, date_string):
        self.result = dateparser.parse(date_string)

    def when_date_is_parsed(self, date_string, languages=None, locales=None):
        self.result = dateparser.parse(
            date_string, languages=languages, locales=locales
        )

    def when_date_is_parsed_with_settings(self, date_string, settings=None):
        self.result = dateparser.parse(date_string, settings=settings)

    def when_date_is_parsed_with_args_and_settings(
        self,
        date_string,
        languages=None,
        locales=None,
        region=None,
        date_formats=None,
        settings=None,
    ):
        self.result = dateparser.parse(
            date_string,
            languages=languages,
            locales=locales,
            region=region,
            date_formats=date_formats,
            settings=settings,
        )

    def then_parsed_date_is(self, expected_date):
        self.assertEqual(
            self.result, datetime.combine(expected_date, datetime.min.time())
        )

    def then_parsed_date_and_time_is(self, expected_date):
        self.assertEqual(self.result, expected_date)

    def then_date_was_not_parsed(self):
        self.assertIsNone(self.result)
