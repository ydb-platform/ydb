from datetime import datetime, time
import warnings

from parameterized import param, parameterized

from dateparser.conf import apply_settings
from dateparser.parser import _no_spaces_parser, _parser, time_parser, tokenizer
from tests import BaseTestCase


class TestTokenizer(BaseTestCase):
    @parameterized.expand(
        [
            param(
                date_string="11 april 2010",
                expected_tokens=["11", " ", "april", " ", "2010"],
                expected_types=[0, 2, 1, 2, 0],
            ),
            param(
                date_string="Tuesday 11 april 2010",
                expected_tokens=["Tuesday", " ", "11", " ", "april", " ", "2010"],
                expected_types=[1, 2, 0, 2, 1, 2, 0],
            ),
            param(
                date_string="11/12-2013",
                expected_tokens=["11", "/", "12", "-", "2013"],
                expected_types=[0, 2, 0, 2, 0],
            ),
            param(
                date_string="11/12-2013",
                expected_tokens=["11", "/", "12", "-", "2013"],
                expected_types=[0, 2, 0, 2, 0],
            ),
            param(
                date_string="10:30:35 PM",
                expected_tokens=["10:30:35", " ", "PM"],
                expected_types=[0, 2, 1],
            ),
            param(
                date_string="18:50",
                expected_tokens=["18:50"],
                expected_types=[0],
            ),
            param(
                date_string="December 23, 2010, 16:50 pm",
                expected_tokens=[
                    "December",
                    " ",
                    "23",
                    ", ",
                    "2010",
                    ", ",
                    "16:50",
                    " ",
                    "pm",
                ],
                expected_types=[1, 2, 0, 2, 0, 2, 0, 2, 1],
            ),
            param(
                date_string="Oct 1 2018 4:40 PM EST —",
                expected_tokens=[
                    "Oct",
                    " ",
                    "1",
                    " ",
                    "2018",
                    " ",
                    "4:40",
                    " ",
                    "PM",
                    " ",
                    "EST",
                    " —",
                ],
                expected_types=[1, 2, 0, 2, 0, 2, 0, 2, 1, 2, 1, 2],
            ),
            param(
                date_string=tokenizer.digits,
                expected_tokens=[tokenizer.digits],
                expected_types=[0],
            ),
            param(
                date_string=tokenizer.letters,
                expected_tokens=[tokenizer.letters],
                expected_types=[1],
            ),
            param(
                date_string="./\\()\"',.;<>~!@#$%^&*|+=[]{}`~?-—–     😊",  # unrecognized characters
                expected_tokens=["./\\()\"',.;<>~!@#$%^&*|+=[]{}`~?-—–     😊"],
                expected_types=[2],
            ),
        ]
    )
    def test_tokenization(self, date_string, expected_tokens, expected_types):
        self.given_tokenizer(date_string)
        self.when_tokenized()
        self.then_tokens_were(expected_tokens)
        self.then_token_types_were(expected_types)

    def given_tokenizer(self, date_string):
        self.tokenizer = tokenizer(date_string)

    def when_tokenized(self):
        self.result = list(self.tokenizer.tokenize())

    def then_tokens_were(self, expected_tokens):
        self.assertEqual([token_type[0] for token_type in self.result], expected_tokens)

    def then_token_types_were(self, expected_types):
        self.assertEqual([token_type[1] for token_type in self.result], expected_types)


class TestNoSpaceParser(BaseTestCase):
    def test_date_with_spaces_is_not_parsed(self):
        datestring = "2013 25 12"
        self.given_parser()
        self.given_settings()
        self.when_date_is_parsed(datestring)
        self.then_error_was_raised(
            ValueError, ["Unable to parse date from: %s" % datestring]
        )

    @parameterized.expand(
        [
            param(
                date_string="",
            ),
            param(
                date_string=":",
            ),
        ]
    )
    def test_empty_string_is_not_parsed(self, date_string):
        self.given_parser()
        self.given_settings()
        self.when_date_is_parsed(date_string)
        self.then_error_was_raised(ValueError, ["Empty string"])

    @parameterized.expand(
        [
            param(
                date_string="::",
            ),
            param(
                date_string=":::",
            ),
        ]
    )
    def test_colons_string_is_not_parsed(self, date_string):
        self.given_parser()
        self.given_settings()
        self.when_date_is_parsed(date_string)
        self.then_error_was_raised(
            ValueError, ["Unable to parse date from: %s" % date_string]
        )

    def test_date_with_alphabets_is_not_parsed(self):
        datestring = "12AUG2015"
        self.given_parser()
        self.given_settings()
        self.when_date_is_parsed(datestring)
        self.then_error_was_raised(
            ValueError, ["Unable to parse date from: %s" % datestring]
        )

    @parameterized.expand(
        [
            param(
                date_string="201115",
                expected_date=datetime(2015, 11, 20),
                date_order="DMY",
                expected_period="day",
            ),
            param(
                date_string="18092017",
                expected_date=datetime(2017, 9, 18),
                date_order="DMY",
                expected_period="day",
            ),
            param(
                date_string="912958:15:10",
                expected_date=datetime(9581, 12, 9, 5, 1),
                date_order="DMY",
                expected_period="day",
            ),
            param(
                date_string="20201511",
                expected_date=datetime(2015, 11, 20),
                date_order="DYM",
                expected_period="day",
            ),
            param(
                date_string="171410",
                expected_date=datetime(2014, 10, 17),
                date_order="DYM",
                expected_period="day",
            ),
            param(
                date_string="71995121046",
                expected_date=datetime(1995, 12, 7, 10, 4, 6),
                date_order="DYM",
                expected_period="day",
            ),
            param(
                date_string="112015",
                expected_date=datetime(2015, 1, 1),
                date_order="MDY",
                expected_period="day",
            ),
            param(
                date_string="12595",
                expected_date=datetime(1995, 12, 5),
                date_order="MDY",
                expected_period="day",
            ),
            param(
                date_string="459712:15:07.54",
                expected_date=datetime(4597, 12, 15, 0, 7),
                date_order="MDY",
                expected_period="day",
            ),
            param(
                date_string="11012015",
                expected_date=datetime(2015, 11, 1),
                date_order="MDY",
                expected_period="day",
            ),
            param(
                date_string="12201511",
                expected_date=datetime(2015, 12, 11),
                date_order="MYD",
                expected_period="day",
            ),
            param(
                date_string="21813",
                expected_date=datetime(2018, 2, 13),
                date_order="MYD",
                expected_period="day",
            ),
            param(
                date_string="12937886",
                expected_date=datetime(2937, 1, 8, 8, 6),
                date_order="MYD",
                expected_period="day",
            ),
            param(
                date_string="20151211",
                expected_date=datetime(2015, 12, 11),
                date_order="YMD",
                expected_period="day",
            ),
            param(
                date_string="18216",
                expected_date=datetime(2018, 2, 16),
                date_order="YMD",
                expected_period="day",
            ),
            param(
                date_string="1986411:5",
                expected_date=datetime(1986, 4, 1, 1, 5),
                date_order="YMD",
                expected_period="day",
            ),
            param(
                date_string="20153011",
                expected_date=datetime(2015, 11, 30),
                date_order="YDM",
                expected_period="day",
            ),
            param(
                date_string="14271",
                expected_date=datetime(2014, 1, 27),
                date_order="YDM",
                expected_period="day",
            ),
            param(
                date_string="2010111110:11",
                expected_date=datetime(2010, 11, 11, 10, 1, 1),
                date_order="YDM",
                expected_period="day",
            ),
            param(
                date_string="10:11:2",
                expected_date=datetime(2010, 2, 11, 0, 0),
                date_order="YDM",
                expected_period="day",
            ),
        ]
    )
    def test_date_are_parsed_in_order_supplied(
        self, date_string, expected_date, expected_period, date_order
    ):
        self.given_parser()
        self.given_settings(settings={"DATE_ORDER": date_order})
        self.when_date_is_parsed(date_string)
        self.then_date_exactly_is(expected_date)
        self.then_period_exactly_is(expected_period)

    @parameterized.expand(
        [
            param(
                date_string="10032017",
                expected_date=datetime(2017, 10, 3),
                expected_period="day",
            ),
            param(
                date_string="19991215:07:08:04.54",
                expected_date=datetime(1999, 12, 15, 7, 8, 4),
                expected_period="day",
            ),
        ]
    )
    def test_default_order_used_if_date_order_not_supplied(
        self, date_string, expected_date, expected_period
    ):
        self.given_parser()
        self.given_settings(settings={"DATE_ORDER": ""})
        self.when_date_is_parsed(date_string)
        self.then_date_exactly_is(expected_date)
        self.then_period_exactly_is(expected_period)

    @parameterized.expand(
        [
            param(
                date_string="20110101",
                expected_date=datetime(2011, 1, 1),
                expected_period="day",
            ),
            param(
                date_string="01201702",
                expected_date=datetime(1702, 1, 20),
                expected_period="day",
            ),
            param(
                date_string="01202020",
                expected_date=datetime(2020, 1, 20),
                expected_period="day",
            ),
            param(
                date_string="20202001",
                expected_date=datetime(2020, 1, 20),
                expected_period="day",
            ),
            param(
                date_string="20202020",
                expected_date=datetime(2002, 2, 2, 0, 2),
                expected_period="day",
            ),
            param(
                date_string="12345678",
                expected_date=datetime(1234, 5, 6, 7, 8),
                expected_period="day",
            ),
        ]
    )
    def test_best_order_used_if_date_order_not_supplied_to_8_digit_numbers(
        self, date_string, expected_date, expected_period
    ):
        self.given_parser()
        self.given_settings(settings={"DATE_ORDER": ""})
        self.when_date_is_parsed(date_string)
        self.then_date_exactly_is(expected_date)
        self.then_period_exactly_is(expected_period)

    @parameterized.expand(
        [
            param(date_string="12345678901234567890", date_order="YMD"),
            param(date_string="987654321234567890123456789", date_order="DMY"),
        ]
    )
    def test_error_is_raised_when_date_cannot_be_parsed(self, date_string, date_order):
        self.given_parser()
        self.given_settings(settings={"DATE_ORDER": date_order})
        self.when_date_is_parsed(date_string)
        self.then_error_was_raised(
            ValueError, ["Unable to parse date from: {}".format(date_string)]
        )

    @parameterized.expand(
        [
            param(format_string="%d", expected_period="day"),
            param(format_string="%H", expected_period="day"),
            param(format_string="%M", expected_period="day"),
            param(format_string="%S", expected_period="day"),
            param(format_string="%m", expected_period="month"),
            param(format_string="%y", expected_period="year"),
            param(format_string="", expected_period="year"),
            param(format_string="%m%d", expected_period="day"),
            param(format_string="%Y%m", expected_period="month"),
            param(format_string="%d%m%y", expected_period="day"),
            param(format_string="%Y%m%d%H%M", expected_period="day"),
            param(format_string="%Y%m%d%H%M%S.%f", expected_period="day"),
            param(format_string="%H%M", expected_period="day"),
            param(format_string="%M%S.%f", expected_period="day"),
        ]
    )
    def test_get_period_function(self, format_string, expected_period):
        self.given_parser()
        self.when_get_period_is_called(format_string)
        self.then_returned_period_is(expected_period)

    def given_parser(self):
        self.parser = _no_spaces_parser

    @apply_settings
    def given_settings(self, settings=None):
        self.settings = settings

    def when_date_is_parsed(self, date_string):
        try:
            self.result = self.parser.parse(date_string, self.settings)
        except Exception as error:
            self.error = error

    def when_get_period_is_called(self, format_string):
        self.result = self.parser._get_period(format_string)

    def then_date_exactly_is(self, expected_date):
        self.assertEqual(self.result[0], expected_date)

    def then_period_exactly_is(self, expected_period):
        self.assertEqual(self.result[1], expected_period)

    def then_date_is_not_parsed(self):
        self.assertIsNone(self.result)

    def then_returned_period_is(self, expected_period):
        self.assertEqual(self.result, expected_period)


class TestParser(BaseTestCase):
    @parameterized.expand(
        [
            param(date_string="april 2010"),
            param(date_string="11 March"),
            param(date_string="March"),
            param(date_string="31 2010"),
            param(date_string="31/2010"),
        ]
    )
    def test_error_is_raised_when_incomplete_dates_given(self, date_string):
        self.given_parser()
        self.given_settings(settings={"STRICT_PARSING": True})
        self.then_error_is_raised_when_date_is_parsed(date_string)

    @parameterized.expand(
        [
            param(date_string="april 2010"),
            param(date_string="11 March"),
            param(date_string="March"),
            param(date_string="31 2010"),
            param(date_string="31/2010"),
        ]
    )
    def test_error_is_raised_when_partially_complete_dates_given(self, date_string):
        self.given_parser()
        self.given_settings(settings={"REQUIRE_PARTS": ["day", "month", "year"]})
        self.then_error_is_raised_when_date_is_parsed(date_string)

    @parameterized.expand(
        [
            param(date_string="april 2010"),
            param(date_string="March"),
            param(date_string="2010"),
        ]
    )
    def test_error_is_raised_when_day_part_missing(self, date_string):
        self.given_parser()
        self.given_settings(settings={"REQUIRE_PARTS": ["day"]})
        self.then_error_is_raised_when_date_is_parsed(date_string)

    @parameterized.expand(
        [
            param(date_string="31 2010"),
            param(date_string="31/2010"),
        ]
    )
    def test_error_is_raised_when_month_part_missing(self, date_string):
        self.given_parser()
        self.given_settings(settings={"REQUIRE_PARTS": ["month"]})
        self.then_error_is_raised_when_date_is_parsed(date_string)

    @parameterized.expand(
        [
            param(date_string="11 March"),
            param(date_string="March"),
        ]
    )
    def test_error_is_raised_when_year_part_missing(self, date_string):
        self.given_parser()
        self.given_settings(settings={"REQUIRE_PARTS": ["year"]})
        self.then_error_is_raised_when_date_is_parsed(date_string)

    def given_parser(self):
        self.parser = _parser

    @apply_settings
    def given_settings(self, settings=None):
        self.settings = settings

    def when_date_is_parsed(self, date_string):
        try:
            self.parser.parse(date_string, self.settings)
        except Exception as error:
            self.error = error

    def then_error_is_raised_when_date_is_parsed(self, date_string):
        with self.assertRaises(ValueError):
            self.parser.parse(date_string, self.settings)

    @parameterized.expand(
        [
            param(date_string="oct 14"),
            param(date_string="14-October-2025"),
            param(date_string="2024-11-27"),
            param(date_string="tomorrow"),
            param(date_string="1484823450"),
            param(date_string="In two months"),
        ]
    )
    def test_parser_does_not_raise_ambiguious_date_deprecation_warning(
        self, date_string
    ):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.when_date_is_parsed(date_string)
            year_warnings = [
                warn
                for warn in w
                if "day of month without a year specified is ambiguious"
                in str(warn.message)
            ]
            self.assertEqual(len(year_warnings), 0)


class TestTimeParser(BaseTestCase):
    @parameterized.expand(
        [
            param(date_string="11:30:14", timeobj=time(11, 30, 14)),
            param(date_string="11:30", timeobj=time(11, 30)),
            param(date_string="11:30 PM", timeobj=time(23, 30)),
            param(date_string="13:30 PM", timeobj=time(13, 30)),
            param(
                date_string="16:14 AM", timeobj=time(16, 14)
            ),  # accepted as valid even when it's not valid
            param(date_string="23:30 AM", timeobj=time(23, 30)),
            param(date_string="1:30 AM", timeobj=time(1, 30)),
            param(date_string="1:30:15.330 AM", timeobj=time(1, 30, 15, 330000)),
            param(date_string="1:30:15.330 PM", timeobj=time(13, 30, 15, 330000)),
            param(date_string="1:30:15.3301 PM", timeobj=time(13, 30, 15, 330100)),
            param(date_string="11:20:05 AM", timeobj=time(11, 20, 5)),
            param(date_string="14:30:15.330100", timeobj=time(14, 30, 15, 330100)),
        ]
    )
    def test_time_is_parsed(self, date_string, timeobj):
        self.given_parser()
        self.when_time_is_parsed(date_string)
        self.then_time_exactly_is(timeobj)

    @parameterized.expand(
        [
            param(date_string="11"),
            param(date_string="22:12:12 PM"),
            param(date_string="22:12:10:16"),
            param(date_string="10:14.123 PM"),
            param(date_string="2:13:88"),
            param(date_string="23:01:56.34 PM"),
            param(date_string="2.45 PM"),
        ]
    )
    def test_error_is_raised_for_invalid_time_string(self, date_string):
        self.given_parser()
        self.when_time_is_parsed(date_string)
        self.then_error_was_raised(
            ValueError,
            ["{} does not seem to be a valid time string".format(date_string)],
        )

    def given_parser(self):
        self.parser = time_parser

    def when_time_is_parsed(self, datestring):
        try:
            self.result = self.parser(datestring)
        except Exception as error:
            self.error = error

    def then_time_exactly_is(self, timeobj):
        self.assertEqual(self.result, timeobj)
