# The MIT License (MIT)
#
# Copyright (c) 2016 Adam Schubert
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from __future__ import annotations

import calendar
import datetime
import re
from typing import Callable, TypedDict

from typing_extensions import Unpack

from .CasingTypeEnum import CasingTypeEnum
from .DescriptionTypeEnum import DescriptionTypeEnum
from .Exception import FormatError, WrongArgumentError
from .ExpressionParser import ExpressionParser
from .GetText import GetText
from .Options import Options
from .StringBuilder import StringBuilder


class OptionsKwargs(TypedDict, total=False):
    use_24hour_time_format: bool
    locale_code: str
    casing_type: CasingTypeEnum
    verbose: bool
    day_of_week_start_index_zero: bool
    locale_location: str | None

class ExpressionDescriptor:
    """Converts a Cron Expression into a human readable string
    """

    _special_characters = ("/", "-", ",", "*")

    _expression = ""
    _options: Options
    _expression_parts: list[str]

    def __init__(self, expression: str, options: Options | None=None, **kwargs: Unpack[OptionsKwargs]) -> None:
        """Initializes a new instance of the ExpressionDescriptor

        Args:
            expression: The cron expression string
            options: Options to control the output description
        Raises:
            WrongArgumentException: if kwarg is unknown

        """
        if options is None:
            options = Options()
        self._expression = expression
        self._options = options
        self._expression_parts = []

        # if kwargs in _options, overwrite it, if not raise exception
        for kwarg, value in kwargs.items():
            if hasattr(self._options, kwarg):
                setattr(self._options, kwarg, value)
            else:
                msg = f"Unknown {kwarg} configuration argument"
                raise WrongArgumentError(msg)

        # Initializes localization
        self.get_text = GetText(options.locale_code, options.locale_location)

        # Parse expression
        parser = ExpressionParser(self._expression, self._options)
        self._expression_parts = parser.parse()

    def translate(self, message: str) -> str:
        return self.get_text.trans.gettext(message)

    def get_description(self, description_type: DescriptionTypeEnum = DescriptionTypeEnum.FULL) -> str:
        """Generates a humanreadable string for the Cron Expression

        Args:
            description_type: Which part(s) of the expression to describe
        Returns:
            The cron expression description
        Raises:
            Exception:

        """
        choices = {
            DescriptionTypeEnum.FULL: self.get_full_description,
            DescriptionTypeEnum.TIMEOFDAY: self.get_time_of_day_description,
            DescriptionTypeEnum.HOURS: self.get_hours_description,
            DescriptionTypeEnum.MINUTES: self.get_minutes_description,
            DescriptionTypeEnum.SECONDS: self.get_seconds_description,
            DescriptionTypeEnum.DAYOFMONTH: self.get_day_of_month_description,
            DescriptionTypeEnum.MONTH: self.get_month_description,
            DescriptionTypeEnum.DAYOFWEEK: self.get_day_of_week_description,
            DescriptionTypeEnum.YEAR: self.get_year_description,
        }

        return choices.get(description_type, self.get_seconds_description)()

    def get_full_description(self) -> str:
        """Generates the FULL description

        Returns:
            The FULL description
        Raises:
            FormatException: if formatting fails

        """
        try:
            time_segment = self.get_time_of_day_description()
            day_of_month_desc = self.get_day_of_month_description()
            month_desc = self.get_month_description()
            day_of_week_desc = self.get_day_of_week_description()
            year_desc = self.get_year_description()

            description = f"{time_segment}{day_of_month_desc}{day_of_week_desc}{month_desc}{year_desc}"

            description = self.transform_verbosity(description, use_verbose_format=self._options.verbose)
            description = ExpressionDescriptor.transform_case(description, self._options.casing_type)
        except Exception as e:
            description = self.translate(
                "An error occurred when generating the expression description.  Check the cron expression syntax.",
            )
            raise FormatError(description) from e

        return description

    def get_time_of_day_description(self) -> str:
        """Generates a description for only the TIMEOFDAY portion of the expression

        Returns:
            The TIMEOFDAY description

        """
        seconds_expression = self._expression_parts[0]
        minute_expression = self._expression_parts[1]
        hour_expression = self._expression_parts[2]

        description = StringBuilder()

        # handle special cases first
        if any(exp in minute_expression for exp in self._special_characters) is False and \
            any(exp in hour_expression for exp in self._special_characters) is False and \
                any(exp in seconds_expression for exp in self._special_characters) is False:
            # specific time of day (i.e. 10 14)
            description.append(self.translate("At "))
            description.append(
                self.format_time(
                    hour_expression,
                    minute_expression,
                    seconds_expression))
        elif seconds_expression == "" and "-" in minute_expression and \
            "," not in minute_expression and \
                any(exp in hour_expression for exp in self._special_characters) is False:
            # minute range in single hour (i.e. 0-10 11)
            minute_parts = minute_expression.split("-")
            description.append(self.translate("Every minute between {0} and {1}").format(
                self.format_time(hour_expression, minute_parts[0]), self.format_time(hour_expression, minute_parts[1])))
        elif seconds_expression == "" and "," in hour_expression and "-" not in hour_expression and \
                any(exp in minute_expression for exp in self._special_characters) is False:
            # hours list with single minute (o.e. 30 6,14,16)
            hour_parts = hour_expression.split(",")
            description.append(self.translate("At"))
            for i, hour_part in enumerate(hour_parts):
                description.append(" ")
                description.append(self.format_time(hour_part, minute_expression))

                if i < (len(hour_parts) - 2):
                    description.append(",")

                if i == len(hour_parts) - 2:
                    description.append(self.translate(" and"))
        else:
            # default time description
            seconds_description = self.get_seconds_description()
            minutes_description = self.get_minutes_description()
            hours_description = self.get_hours_description()

            description.append(seconds_description)

            if description and minutes_description:
                description.append(", ")

            description.append(minutes_description)

            if description and hours_description:
                description.append(", ")

            description.append(hours_description)
        return str(description)

    def get_seconds_description(self) -> str:
        """Generates a description for only the SECONDS portion of the expression

        Returns:
            The SECONDS description

        """

        def get_description_format(s: str) -> str:
            if s == "0":
                return ""

            try:
                if int(s) < 20:
                    return self.translate("at {0} seconds past the minute")

                return self.translate("at {0} seconds past the minute [grThen20]") or self.translate("at {0} seconds past the minute")
            except ValueError:
                return self.translate("at {0} seconds past the minute")

        return self.get_segment_description(
            self._expression_parts[0],
            self.translate("every second"),
            lambda s: s,
            lambda s: self.translate("every {0} seconds").format(s),
            lambda _: self.translate("seconds {0} through {1} past the minute"),
            get_description_format,
            lambda _: self.translate(", second {0} through second {1}") or self.translate(", {0} through {1}"),
        )

    def get_minutes_description(self) -> str:
        """Generates a description for only the MINUTE portion of the expression

        Returns:
            The MINUTE description

        """
        seconds_expression = self._expression_parts[0]

        def get_description_format(s: str) -> str:
            if s == "0" and seconds_expression == "":
                return ""

            try:
                if int(s) < 20:
                    return self.translate("at {0} minutes past the hour")

                return self.translate("at {0} minutes past the hour [grThen20]") or self.translate("at {0} minutes past the hour")
            except ValueError:
                return self.translate("at {0} minutes past the hour")

        return self.get_segment_description(
            self._expression_parts[1],
            self.translate("every minute"),
            lambda s: s,
            lambda s: self.translate("every {0} minutes").format(s),
            lambda _: self.translate("minutes {0} through {1} past the hour"),
            get_description_format,
            lambda _: self.translate(", minute {0} through minute {1}") or self.translate(", {0} through {1}"),
        )

    def get_hours_description(self) -> str:
        """Generates a description for only the HOUR portion of the expression

        Returns:
            The HOUR description

        """
        expression = self._expression_parts[2]
        return self.get_segment_description(
            expression,
            self.translate("every hour"),
            lambda s: self.format_time(s, "0"),
            lambda s: self.translate("every {0} hours").format(s),
            lambda _: self.translate("between {0} and {1}"),
            lambda _: self.translate("at {0}"),
            lambda _: self.translate(", hour {0} through hour {1}") or self.translate(", {0} through {1}"),
        )

    def get_day_of_week_description(self) -> str:
        """Generates a description for only the DAYOFWEEK portion of the expression

        Returns:
            The DAYOFWEEK description

        """
        if self._expression_parts[5] == "*":
            # DOW is specified as * so we will not generate a description and defer to DOM part.
            # Otherwise, we could get a contradiction like "on day 1 of the month, every day"
            # or a dupe description like "every day, every day".
            return ""

        def get_day_name(s: str) -> str:
            exp = s
            if "#" in s:
                exp, _ = s.split("#", 2)
            elif "L" in s:
                exp = exp.replace("L", "")
            return ExpressionDescriptor.number_to_day(int(exp))

        def get_format(s: str) -> str:
            if "#" in s:
                day_of_week_of_month = s[s.find("#") + 1:]

                try:
                    day_of_week_of_month_number = int(day_of_week_of_month)
                    choices = {
                        1: self.translate("first"),
                        2: self.translate("second"),
                        3: self.translate("third"),
                        4: self.translate("fourth"),
                        5: self.translate("fifth"),
                    }
                    day_of_week_of_month_description = choices.get(day_of_week_of_month_number, "")
                except ValueError:
                    day_of_week_of_month_description = ""

                formatted = "{}{}{}".format(self.translate(", on the "), day_of_week_of_month_description, self.translate(" {0} of the month"))
            elif "L" in s:
                formatted = self.translate(", on the last {0} of the month")
            else:
                formatted = self.translate(", only on {0}")

            return formatted

        return self.get_segment_description(
            self._expression_parts[5],
            self.translate(", every day"),
            lambda s: get_day_name(s),
            lambda s: self.translate(", every {0} days of the week").format(s),
            lambda _: self.translate(", {0} through {1}"),
            lambda s: get_format(s),
            lambda _: self.translate(", {0} through {1}"),
        )

    def get_month_description(self) -> str:
        """Generates a description for only the MONTH portion of the expression

        Returns:
            The MONTH description

        """
        return self.get_segment_description(
            self._expression_parts[4],
            "",
            lambda s: datetime.date(datetime.datetime.now(tz=datetime.timezone.utc).date().year, int(s), 1).strftime("%B"),
            lambda s: self.translate(", every {0} months").format(s),
            lambda _: self.translate(", month {0} through month {1}") or self.translate(", {0} through {1}"),
            lambda _: self.translate(", only in {0}"),
            lambda _: self.translate(", month {0} through month {1}") or self.translate(", {0} through {1}"),
        )

    def get_day_of_month_description(self) -> str:
        """Generates a description for only the DAYOFMONTH portion of the expression

        Returns:
            The DAYOFMONTH description

        """
        expression = self._expression_parts[3]

        if expression == "L":
            description = self.translate(", on the last day of the month")
        elif expression in ("LW", "WL"):
            description = self.translate(", on the last weekday of the month")
        else:
            regex = re.compile(r"(\d{1,2}W)|(W\d{1,2})")
            m = regex.match(expression)
            if m:  # if matches
                day_number = int(m.group().replace("W", ""))

                day_string = self.translate("first weekday") if day_number == 1 else self.translate("weekday nearest day {0}").format(day_number)
                description = self.translate(", on the {0} of the month").format(day_string)
            elif expression == "*" and self._expression_parts[5] != "*":
                # DOW is specified, but DOM is *, so do not generate DOM description.
                # Otherwise, we could get a contradiction like "every day, on Tuesday"
                description = ""
            else:
                # Handle "last day offset"(i.e.L - 5: "5 days before the last day of the month")
                regex = re.compile(r"L-(\d{1,2})")
                m = regex.match(expression)
                if m:  # if matches
                    off_set_days = m.group(1)
                    description = self.translate(", {0} days before the last day of the month").format(off_set_days)
                else:
                    description = self.get_segment_description(
                        expression,
                        self.translate(", every day"),
                        lambda s: s,
                        lambda s: self.translate(", every day") if s == "1" else self.translate(", every {0} days"),
                        lambda _: self.translate(", between day {0} and {1} of the month"),
                        lambda _: self.translate(", on day {0} of the month"),
                        lambda _: self.translate(", {0} through {1}"),
                    )

        return description

    def get_year_description(self) -> str:
        """Generates a description for only the YEAR portion of the expression

        Returns:
            The YEAR description

        """

        def format_year(s: str) -> str:
            regex = re.compile(r"^\d+$")
            if regex.match(s):
                year_int = int(s)
                if year_int < 1900:
                    return str(year_int)
                return datetime.date(year_int, 1, 1).strftime("%Y")

            return s

        return self.get_segment_description(
            self._expression_parts[6],
            "",
            lambda s: format_year(s),
            lambda s: self.translate(", every {0} years").format(s),
            lambda _: self.translate(", year {0} through year {1}") or self.translate(", {0} through {1}"),
            lambda _: self.translate(", only in {0}"),
            lambda _: self.translate(", year {0} through year {1}") or self.translate(", {0} through {1}"),
        )

    def get_segment_description(
        self,
        expression: str,
        all_description: str,
        get_single_item_description: Callable[[str], str],
        get_interval_description_format: Callable[[str], str],
        get_between_description_format: Callable[[str], str],
        get_description_format: Callable[[str], str],
        get_range_format: Callable[[str], str],
    ) -> str:
        """Returns segment description
        Args:
            expression: Segment to descript
            all_description: *
            get_single_item_description: 1
            get_interval_description_format: 1/2
            get_between_description_format: 1-2
            get_description_format: format get_single_item_description
            get_range_format: function that formats range expressions depending on cron parts
        Returns:
            segment description

        """
        if not expression:
            return ""

        if expression == "*":
            return all_description

        if not any(ext in expression for ext in ["/", "-", ","]):
            return get_description_format(expression).format(get_single_item_description(expression))

        if "/" in expression:
            segments = expression.split("/")
            description = get_interval_description_format(segments[1]).format(segments[1])

            # interval contains 'between' piece (i.e. 2-59/3 )
            if "-" in segments[0]:
                between_segment_description = self.generate_between_segment_description(
                    segments[0],
                    get_between_description_format,
                    get_single_item_description,
                )
                if not between_segment_description.startswith(", "):
                    description += ", "

                description += between_segment_description
            elif not any(ext in segments[0] for ext in ["*", ","]):
                range_item_description = get_description_format(segments[0]).format(
                    get_single_item_description(segments[0]),
                )
                range_item_description = range_item_description.replace(", ", "")

                description += self.translate(", starting {0}").format(range_item_description)
            return description

        if "," in expression:
            segments = expression.split(",")

            description_content = ""
            for i, segment in enumerate(segments):
                if i > 0 and len(segments) > 2:
                    description_content += ","

                    if i < len(segments) - 1:
                        description_content += " "

                if i > 0 and len(segments) > 1 and (i == len(segments) - 1 or len(segments) == 2):
                    description_content += self.translate(" and ")

                if "-" in segment:
                    between_segment_description = self.generate_between_segment_description(
                        segment,
                        get_range_format,
                        get_single_item_description,
                    )

                    between_segment_description = between_segment_description.replace(", ", "")

                    description_content += between_segment_description
                else:
                    description_content += get_single_item_description(segment)

            return get_description_format(expression).format(description_content)

        if "-" in expression:
            return self.generate_between_segment_description(
                expression,
                get_between_description_format,
                get_single_item_description,
            )

        return "?"

    def generate_between_segment_description(
            self,
            between_expression: str,
            get_between_description_format: Callable[[str], str],
            get_single_item_description: Callable[[str], str],
    ) -> str:
        """Generates the between segment description
        :param between_expression:
        :param get_between_description_format:
        :param get_single_item_description:
        :return: The between segment description
        """
        description = ""
        between_segments = between_expression.split("-")
        between_segment_1_description = get_single_item_description(between_segments[0])
        between_segment_2_description = get_single_item_description(between_segments[1])
        between_segment_2_description = between_segment_2_description.replace(":00", ":59")

        between_description_format = get_between_description_format(between_expression)
        description += between_description_format.format(between_segment_1_description, between_segment_2_description)

        return description

    def format_time(
        self,
        hour_expression: str,
        minute_expression: str,
        second_expression: str | None=None,
    ) -> str:
        """Given time parts, will construct a formatted time description
        Args:
            hour_expression: Hours part
            minute_expression: Minutes part
            second_expression: Seconds part
        Returns:
            Formatted time description

        """
        hour = int(hour_expression)

        period = ""
        if self._options.use_24hour_time_format is False:
            period = self.translate("PM") if (hour >= 12) else self.translate("AM")
            if period:
                # add preceding space
                period = " " + period

            if hour > 12:
                hour -= 12

            if hour == 0:
                hour = 12

        minute = str(int(minute_expression))  # Removes leading zero if any
        second = ""
        if second_expression is not None and second_expression:
            second = "{}{}".format(":", str(int(second_expression)).zfill(2))

        return f"{str(hour).zfill(2)}:{minute.zfill(2)}{second}{period}"

    def transform_verbosity(self, description: str, *, use_verbose_format: bool = False) -> str:
        """Transforms the verbosity of the expression description by stripping verbosity from original description
        Args:
            description: The description to transform
            use_verbose_format: If True, will leave description as it, if False, will strip verbose parts
        Returns:
            The transformed description with proper verbosity

        """
        if not use_verbose_format:
            description = description.replace(self.translate(", every minute"), "")
            description = description.replace(self.translate(", every hour"), "")
            description = description.replace(self.translate(", every day"), "")
            description = re.sub(r", ?$", "", description)
        return description

    @staticmethod
    def transform_case(description: str, case_type: CasingTypeEnum) -> str:
        """Transforms the case of the expression description, based on options
        Args:
            description: The description to transform
            case_type: The casing type that controls the output casing
        Returns:
            The transformed description with proper casing
        """
        if case_type == CasingTypeEnum.Sentence:
            description = f"{description[0].upper()}{description[1:]}"
        elif case_type == CasingTypeEnum.Title:
            description = description.title()
        else:
            description = description.lower()
        return description

    @staticmethod
    def number_to_day(day_number: int) -> str:
        """Returns localized day name by its CRON number

        Args:
            day_number: Number of a day
        Returns:
            Day corresponding to day_number
        Raises:
            IndexError: When day_number is not found

        """
        try:
            return [
                calendar.day_name[6],
                calendar.day_name[0],
                calendar.day_name[1],
                calendar.day_name[2],
                calendar.day_name[3],
                calendar.day_name[4],
                calendar.day_name[5],
            ][day_number]
        except IndexError as e:
            msg = f"Day {day_number} is out of range!"
            raise IndexError(msg) from e

    def __str__(self) -> str:
        return self.get_description()

    def __repr__(self) -> str:
        return self.get_description()


def get_description(expression: str, options: Options | None=None) -> str:
    """Generates a human readable string for the Cron Expression
    Args:
        expression: The cron expression string
        options: Options to control the output description
    Returns:
        The cron expression description

    """
    descriptor = ExpressionDescriptor(expression, options)
    return descriptor.get_description(DescriptionTypeEnum.FULL)
