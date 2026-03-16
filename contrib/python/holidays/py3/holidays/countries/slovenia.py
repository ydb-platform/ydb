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

from holidays.calendars.gregorian import AUG
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Slovenia(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Slovenia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Slovenia>
        * <https://sl.wikipedia.org/wiki/Državni_prazniki_v_Sloveniji>
        * <https://web.archive.org/web/20250429081020/https://www.uradni-list.si/glasilo-uradni-list-rs/vsebina/1991-01-1091/zakon-o-praznikih-in-dela-prostih-dnevih-v-republiki-sloveniji>
        * <https://web.archive.org/web/20250414154423/https://www.gov.si/teme/drzavni-prazniki-in-dela-prosti-dnevi>
    """

    country = "SI"
    default_language = "sl"
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("en_US", "sl", "uk")
    # Act on Holidays and Non-Working Days in the Republic of Slovenia, 1991-11-21.
    start_year = 1992

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, SloveniaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Work-free national holidays and other work-free days.

        # New Year's Day.
        name = tr("novo leto")
        self._add_new_years_day(name)
        if self._year <= 2012 or self._year >= 2017:
            self._add_new_years_day_two(name)

        # Prešeren's Day, the Slovenian Cultural Holiday.
        self._add_holiday_feb_8(tr("Prešernov dan, slovenski kulturni praznik"))

        # Easter Sunday.
        self._add_easter_sunday(tr("velikonočna nedelja"))

        # Easter Monday.
        self._add_easter_monday(tr("velikonočni ponedeljek"))

        # Day of Uprising Against Occupation.
        self._add_holiday_apr_27(tr("dan upora proti okupatorju"))

        # Labor Day.
        name = tr("praznik dela")
        self._add_labor_day(name)
        self._add_labor_day_two(name)

        # Whit Sunday.
        self._add_whit_sunday(tr("binkoštna nedelja"))

        # Statehood Day.
        self._add_holiday_jun_25(tr("dan državnosti"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Marijino vnebovzetje"))

        # Reformation Day.
        self._add_holiday_oct_31(tr("dan reformacije"))

        # Day of Remembrance for the Dead.
        self._add_all_saints_day(tr("dan spomina na mrtve"))

        # Christmas Day.
        self._add_christmas_day(tr("božič"))

        self._add_holiday_dec_26(
            # Independence and Unity Day.
            tr("dan samostojnosti in enotnosti")
            if self._year >= 2005
            # Independence Day.
            else tr("dan samostojnosti")
        )

    def _populate_workday_holidays(self):
        # Not work-free national holidays.

        if self._year >= 2011:
            # Primož Trubar Day.
            self._add_holiday_jun_8(tr("dan Primoža Trubarja"))

        if self._year >= 2006:
            # Unification of Prekmurje Slovenes with the Mother Nation.
            self._add_holiday_aug_17(tr("združitev prekmurskih Slovencev z matičnim narodom"))

            self._add_holiday_sep_15(
                # Integration of Primorska into the Homeland.
                tr("priključitev Primorske k matični domovini")
                if self._year >= 2025
                # Return of Primorska into the Homeland.
                else tr("vrnitev Primorske k matični domovini")
            )

        if self._year >= 2020:
            # Slovenian Sport's Day.
            self._add_holiday_sep_23(tr("dan slovenskega športa"))

        if self._year >= 2015:
            # Sovereignty Day.
            self._add_holiday_oct_25(tr("dan suverenosti"))

        if self._year >= 2005:
            # Rudolf Maister Day.
            self._add_holiday_nov_23(tr("dan Rudolfa Maistra"))


class SI(Slovenia):
    pass


class SVN(Slovenia):
    pass


class SloveniaStaticHolidays:
    special_public_holidays = {
        # Solidarity Day.
        2023: (AUG, 14, tr("dan solidarnosti")),
    }
