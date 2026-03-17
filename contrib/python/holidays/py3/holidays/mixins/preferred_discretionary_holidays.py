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


class PreferredDiscretionaryHolidays:
    """A mixin for setting preferred discretionary holidays.

    See [HongKong][holidays.countries.hongkong.HongKong] for an example.
    """

    default_preferred_discretionary_holidays: tuple[str, ...] = ()
    """Preferred discretionary holidays defaults."""

    def __init__(self, preferred_discretionary_holidays):
        self.preferred_discretionary_holidays = set(
            preferred_discretionary_holidays
            if preferred_discretionary_holidays
            else self.default_preferred_discretionary_holidays
        )
