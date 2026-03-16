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

from gettext import gettext as tr

from holidays.countries.united_kingdom import UnitedKingdom, UnitedKingdomStaticHolidays
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_MON


class IsleOfMan(UnitedKingdom):
    """Isle Of Man holidays."""

    country = "IM"
    # The Isle of Man (IM) is not a subdivision of the United Kingdom (GB)
    # entity, so the `IsleOfMan` class does not inherit from the `ChildEntity`
    # mixin. The `parent_entity` is specified below solely to maintain
    # consistency in holiday name localization.
    parent_entity = UnitedKingdom
    subdivisions = ()  # Override UnitedKingdom subdivisions.
    subdivisions_aliases = {}  # Override UnitedKingdom subdivisions aliases.

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, UnitedKingdomStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        ObservedHolidayBase.__init__(self, *args, **kwargs)

    def _populate_public_holidays(self) -> None:
        # Common holidays with United Kingdom.
        super()._populate_public_holidays()

        self._populate_common()

        # Isle of Man exclusive holidays.

        # TT Bank Holiday.
        self._add_holiday_1st_fri_of_jun(tr("TT Bank Holiday"))

        # Tynwald Day.
        jul_5 = self._add_holiday_jul_5(tr("Tynwald Day"))
        if self._year >= 1992:
            # Move to the next Monday if falls on a weekend.
            self._move_holiday(jul_5, show_observed_label=False)


class IM(IsleOfMan):
    pass


class IMN(IsleOfMan):
    pass
