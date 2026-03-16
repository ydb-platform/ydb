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

import re
from typing import ClassVar

from .Exception import FormatError, MissingFieldError
from .Options import Options


class ExpressionParser:
    _expression = ""
    _options: Options

    _cron_days: ClassVar[dict[int, str]] = {
        0: "SUN",
        1: "MON",
        2: "TUE",
        3: "WED",
        4: "THU",
        5: "FRI",
        6: "SAT",
    }

    _cron_months: ClassVar[dict[int, str]] = {
        1: "JAN",
        2: "FEB",
        3: "MAR",
        4: "APR",
        5: "MAY",
        6: "JUN",
        7: "JUL",
        8: "AUG",
        9: "SEP",
        10: "OCT",
        11: "NOV",
        12: "DEC",
    }

    def __init__(self, expression: str, options: Options) -> None:
        """Initializes a new instance of the ExpressionParser class
        Args:
            expression: The cron expression string
            options: Parsing options

        """
        self._expression = expression
        self._options = options

    def parse(self) -> list[str]:
        """Parses the cron expression string
        Returns:
            A 7 part string array, one part for each component of the cron expression (seconds, minutes, etc.)

        Raises:
            MissingFieldException: if _expression is empty or None
            FormatException: if _expression has wrong format

        """
        # Initialize all elements of parsed array to empty strings
        parsed = ["", "", "", "", "", "", ""]

        if not self._expression:
            msg = "ExpressionDescriptor.expression"
            raise MissingFieldError(msg)

        expression_parts_temp = self._expression.split()
        expression_parts_temp_length = len(expression_parts_temp)
        if expression_parts_temp_length < 5:
            msg = f"Error: Expression only has {expression_parts_temp_length} parts.  At least 5 part are required."
            raise FormatError(msg)
        if expression_parts_temp_length == 5:
            # 5 part cron so shift array past seconds element
            for i, expression_part_temp in enumerate(expression_parts_temp):
                parsed[i + 1] = expression_part_temp
        elif expression_parts_temp_length == 6:
            # We will detect if this 6 part expression has a year specified and if so we will shift the parts and treat the
            # first part as a minute part rather than a second part.
            # Ways we detect:
            # 1. Last part is a literal year (i.e. 2020)
            # 2. 3rd or 5th part is specified as "?" (DOM or DOW)
            year_regex = re.compile(r"\d{4}$")
            is_year_with_no_seconds_part = bool(year_regex.search(expression_parts_temp[5])) or "?" in [expression_parts_temp[4], expression_parts_temp[2]]
            for i, expression_part_temp in enumerate(expression_parts_temp):
                if is_year_with_no_seconds_part:
                    # Shift parts over by one
                    parsed[i + 1] = expression_part_temp
                else:
                    parsed[i] = expression_part_temp

        elif expression_parts_temp_length == 7:
            parsed = expression_parts_temp
        else:
            msg = f"Error: Expression has too many parts ({expression_parts_temp_length}).  Expression must not have more than 7 parts."
            raise FormatError(msg)
        self.normalize_expression(parsed)

        return parsed

    def normalize_expression(self, expression_parts: list[str]) -> None:
        """Converts cron expression components into consistent, predictable formats.

        Args:
            expression_parts: A 7 part string array, one part for each component of the cron expression
        Returns:
            None

        """
        # convert ? to * only for DOM and DOW
        expression_parts[3] = expression_parts[3].replace("?", "*")
        expression_parts[5] = expression_parts[5].replace("?", "*")

        # convert 0/, 1/ to */
        if expression_parts[0].startswith("0/"):
            expression_parts[0] = expression_parts[0].replace("0/", "*/")  # seconds

        if expression_parts[1].startswith("0/"):
            expression_parts[1] = expression_parts[1].replace("0/", "*/")  # minutes

        if expression_parts[2].startswith("0/"):
            expression_parts[2] = expression_parts[2].replace("0/", "*/")  # hours

        if expression_parts[3].startswith("1/"):
            expression_parts[3] = expression_parts[3].replace("1/", "*/")  # DOM

        if expression_parts[4].startswith("1/"):
            expression_parts[4] = expression_parts[4].replace("1/", "*/")  # Month

        if expression_parts[5].startswith("1/"):
            expression_parts[5] = expression_parts[5].replace("1/", "*/")  # DOW

        if expression_parts[6].startswith("1/"):
            expression_parts[6] = expression_parts[6].replace("1/", "*/")  # Years

        # Adjust DOW based on dayOfWeekStartIndexZero option
        def digit_replace(match: re.Match[str]) -> str:
            match_value = match.group()
            dow_digits = re.sub(r"\D", "", match_value)
            dow_digits_adjusted = dow_digits
            if self._options.day_of_week_start_index_zero:
                if dow_digits == "7":
                    dow_digits_adjusted = "0"
            else:
                dow_digits_adjusted = str(int(dow_digits) - 1)

            return match_value.replace(dow_digits, dow_digits_adjusted)

        expression_parts[5] = re.sub(r"(^\d)|([^#/\s]\d)", digit_replace, expression_parts[5])

        # Convert DOM '?' to '*'
        if expression_parts[3] == "?":
            expression_parts[3] = "*"

        # convert SUN-SAT format to 0-6 format
        for day_number in self._cron_days:
            expression_parts[5] = expression_parts[5].upper().replace(self._cron_days[day_number], str(day_number))

        # convert JAN-DEC format to 1-12 format
        for month_number in self._cron_months:
            expression_parts[4] = expression_parts[4].upper().replace(
                self._cron_months[month_number], str(month_number))

        # convert 0 second to (empty)
        if expression_parts[0] == "0":
            expression_parts[0] = ""

        # If time interval is specified for seconds or minutes and next time part is single item, make it a "self-range" so
        # the expression can be interpreted as an interval 'between' range.
        # For example:
        # 0-20/3 9 * * * => 0-20/3 9-9 * * * (9 => 9-9)
        # */5 3 * * * => */5 3-3 * * * (3 => 3-3)
        star_and_slash = ["*", "/"]
        has_part_zero_star_and_slash = any(ext in expression_parts[0] for ext in star_and_slash)
        has_part_one_star_and_slash = any(ext in expression_parts[1] for ext in star_and_slash)
        has_part_two_special_chars = any(ext in expression_parts[2] for ext in ["*", "-", ",", "/"])
        if not has_part_two_special_chars and (has_part_zero_star_and_slash or has_part_one_star_and_slash):
            expression_parts[2] += f"-{expression_parts[2]}"

        # Loop through all parts and apply global normalization
        length = len(expression_parts)
        for i in range(length):

            # convert all '*/1' to '*'
            if expression_parts[i] == "*/1":
                expression_parts[i] = "*"

            """
            Convert Month,DOW,Year step values with a starting value (i.e. not '*') to between expressions.
            This allows us to reuse the between expression handling for step values.

            For Example:
            - month part '3/2' will be converted to '3-12/2' (every 2 months between March and December)
            - DOW part '3/2' will be converted to '3-6/2' (every 2 days between Tuesday and Saturday)
            """

            if "/" in expression_parts[i] and not any(exp in expression_parts[i] for exp in ["*", "-", ","]):
                choices = {
                    4: "12",
                    5: "6",
                    6: "9999",
                }

                step_range_through = choices.get(i)

                if step_range_through is not None:
                    parts = expression_parts[i].split("/")
                    expression_parts[i] = f"{parts[0]}-{step_range_through}/{parts[1]}"
