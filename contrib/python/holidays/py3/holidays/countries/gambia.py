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

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_MON


class Gambia(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Gambia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_the_Gambia>
        * <https://web.archive.org/web/20230610113952/https://www.visitthegambia.gm/public-holidays/>
        * <https://web.archive.org/web/20250803084358/https://www.op.gov.gm/media-advisory-public-holidays>
    """

    country = "GM"
    default_language = "en_GM"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    # %s (observed).
    observed_label = tr("%s (observed)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (observed, estimated)")
    start_year = 1966
    supported_languages = ("en_GM", "en_US")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=GambiaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        kwargs.setdefault("observed_since", 2021)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # Independence Day.
        self._add_observed(self._add_holiday_feb_18(tr("Independence Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # Labor Day.
        self._add_observed(self._add_labor_day(tr("Labour Day")))

        # Africa Liberation Day.
        self._add_observed(self._add_africa_day(tr("Africa Liberation Day")))

        # July 22 Revolution Day.
        self._add_observed(self._add_holiday_jul_22(tr("July 22 Revolution Day")))

        # Assumption Day.
        self._add_observed(self._add_assumption_of_mary_day(tr("Feast of the Assumption")))

        # Christmas Day.
        self._add_christmas_day(tr("Christmas Day"))

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two(tr("Boxing Day")))

        # Ashura.
        for dt in self._add_ashura_day(tr("Yawmul Ashura")):
            self._add_observed(dt)

        # Prophet's Birthday.
        for dt in self._add_mawlid_day(tr("Mawlid Nabi")):
            self._add_observed(dt)

        # Laylat al-Qadr.
        for dt in self._add_laylat_al_qadr_day(tr("Lialat-Ul-Qadr")):
            self._add_observed(dt)

        # Eid al-Fitr.
        name = tr("Koriteh")
        self._add_eid_al_fitr_day(name)
        if self._year >= 2021:
            for dt in self._add_eid_al_fitr_day_two(name):
                self._add_observed(dt)

        # Eid al-Adha.
        name = tr("Tobaski")
        self._add_eid_al_adha_day(name)
        if self._year >= 2021:
            for dt in self._add_eid_al_adha_day_two(name):
                self._add_observed(dt)


class GM(Gambia):
    pass


class GMB(Gambia):
    pass


class GambiaIslamicHolidays(_CustomIslamicHolidays):
    # https://web.archive.org/web/20240716225449/https://www.timeanddate.com/holidays/gambia/ashura
    ASHURA_DATES_CONFIRMED_YEARS = (2015, 2025)
    ASHURA_DATES = {
        2015: (OCT, 24),
        2016: (OCT, 12),
        2017: (OCT, 1),
        2019: (SEP, 10),
        2021: (AUG, 19),
    }

    # https://web.archive.org/web/20241209170416/https://www.timeanddate.com/holidays/gambia/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_ADHA_DATES = {
        2015: (SEP, 24),
        2016: (SEP, 13),
        2017: (SEP, 2),
        2018: (AUG, 22),
    }

    # https://web.archive.org/web/20241204114244/https://www.timeanddate.com/holidays/gambia/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2015, 2025)
    EID_AL_FITR_DATES = {
        2015: (JUL, 18),
        2016: (JUL, 7),
        2017: (JUN, 26),
        2020: (MAY, 23),
    }

    # https://web.archive.org/web/20241209070551/https://www.timeanddate.com/holidays/gambia/laylat-al-qadr
    LAYLAT_AL_QADR_DATES_CONFIRMED_YEARS = (2015, 2025)
    LAYLAT_AL_QADR_DATES = {
        2016: (JUL, 3),
        2022: (APR, 29),
    }

    # https://web.archive.org/web/20240909111449/https://www.timeanddate.com/holidays/gambia/prophet-birthday
    MAWLID_DATES_CONFIRMED_YEARS = (2016, 2024)
    MAWLID_DATES = {
        2016: (DEC, 12),
        2017: (DEC, 1),
        2018: (NOV, 21),
        2021: (OCT, 19),
        2023: (SEP, 28),
        2024: (SEP, 16),
    }
