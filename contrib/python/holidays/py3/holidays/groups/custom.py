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

from datetime import date

from holidays.helpers import _normalize_tuple


class StaticHolidays:
    """Helper class for special and substituted holidays support.

    Populates special and substituted holidays related data from
    an external class.
    """

    def __init__(self, cls) -> None:
        for attribute_name in cls.__dict__.keys():
            # Special holidays.
            if attribute_name.startswith("special_") and (
                value := getattr(cls, attribute_name, None)
            ):
                setattr(self, attribute_name, value)
                self.has_special_holidays = True

            # "Substituted" labels.
            elif attribute_name.startswith("substituted_") and (
                value := getattr(cls, attribute_name, None)
            ):
                setattr(self, attribute_name, value)

        # Populate substituted holidays from adjacent years.
        self.weekend_workdays = set()
        for special_public_holidays in getattr(self, "special_public_holidays", {}).values():
            for special_public_holiday in _normalize_tuple(special_public_holidays):
                # Normally, special holiday is a 3 item tuple: (month, day, name).
                if len(special_public_holiday) < 4:  # Skip non-substituted holidays.
                    continue

                # Handle cross-year substituted holidays.
                if len(special_public_holiday) == 5:  # The fifth element is the year.
                    _, _, from_month, from_day, from_year = special_public_holiday
                    self.weekend_workdays.add(date(from_year, from_month, from_day))

                self.has_substituted_holidays = True
