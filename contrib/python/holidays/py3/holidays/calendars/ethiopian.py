#  holidays
#  --------
#  A fast, efficient Python library for generating country, province and state
#  specific sets of holidays on the fly. It aims to make determining whether a
#  specific date is a holiday as fast and flexible as possible.
#
#  Authors: Vacanza Team and individual contributors (see CONTRIBUTORS file)
#           dr-prodigy <dr.prodigy.github@gmail.com> (c) 2017-2023
#           ryanss <ryanssdev@icloud.com> (c) 2014-2017
#  Website: https://github.com/vacanza/holidays
#  License: MIT (see LICENSE file)

ETHIOPIAN_CALENDAR = "ETHIOPIAN_CALENDAR"


def is_ethiopian_leap_year(year: int) -> bool:
    """Determine if the Ethiopian year that starts in the given Gregorian year is a leap year.

    Ethiopian leap years follow the Coptic/Julian rule:
        * Every 4 years without exception (no century rule).
        * The Ethiopian year starts on September 11 (or 12 in an Ethiopian leap year).

    Args:
        year:
            Gregorian year to evaluate.

    Returns:
        `True` if an Ethiopian leap year starts in the given Gregorian year, `False` otherwise.
    """

    return (year + 1) % 4 == 0
