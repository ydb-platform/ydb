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

from holidays.calendars import _CustomHinduHolidays, _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, SEP, OCT, NOV, DEC
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_TO_NONE, SUN_TO_NONE


class NationalStockExchangeOfIndia(
    ObservedHolidayBase, HinduCalendarHolidays, ChristianHolidays, IslamicHolidays, StaticHolidays
):
    """National Stock Exchange of India (NSE) holidays.

    References:
        * <https://web.archive.org/web/20250821175252/https://www.nseindia.com/resources/exchange-communication-circulars>

    Historical data:
        * [2001](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr2189.wri)
        * [2002](https://archive.org/details/cmtr3058)
        * [2003](https://web.archive.org/web/20250904042405/https://nsearchives.nseindia.com/content/circulars/cmtr3809.htm)
        * [2004](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr4645.htm)
        * [2005](https://web.archive.org/web/20250904043234/https://nsearchives.nseindia.com/content/circulars/cmtr5633.htm)
        * [2006](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr6946.htm)
        * [2007](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr8182.pdf)
        * [2008](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr9908.htm)
        * [2009](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr11733.htm)
        * [2010](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr13713.pdf)
        * [2011](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr16348.pdf)
        * [2012](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr19539.pdf)
        * [2013](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr22317.pdf)
        * [2014](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr25326.pdf)
        * [2015](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr28337.pdf)
        * [2016](https://web.archive.org/web/20250903152854/https://nsearchives.nseindia.com/content/circulars/CMTR31297.pdf)
        * [2017](https://web.archive.org/web/20250903152829/https://nsearchives.nseindia.com/content/circulars/CMTR33746.pdf)
        * [2018](https://web.archive.org/web/20250903152307/https://nsearchives.nseindia.com/content/circulars/CMTR36475.pdf)
        * [2019](https://archive.org/details/nsearchives.nseindia.comcontentcircularscmtr39612.pdf)
        * [2020](https://web.archive.org/web/20250903152140/https://nsearchives.nseindia.com/content/circulars/CMTR42877.pdf)
        * [2021](https://web.archive.org/web/20250903152041/https://nsearchives.nseindia.com/content/circulars/CMTR46623.pdf)
        * [2022](https://web.archive.org/web/20250821071611/https://nsearchives.nseindia.com/content/circulars/CMTR50560.pdf)
        * [2023](https://web.archive.org/web/20250821071635/https://nsearchives.nseindia.com/content/circulars/CMTR54757.pdf)
        * [2024](https://web.archive.org/web/20250821071650/https://nsearchives.nseindia.com/content/circulars/CMTR59722.pdf)
        * [2025](https://web.archive.org/web/20250624132016/https://nsearchives.nseindia.com/content/circulars/CMTR65587.pdf)
    """

    market = "XNSE"
    default_language = "en_IN"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    supported_languages = ("en_IN", "en_US", "gu", "hi")
    start_year = 2001

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In India, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=NationalStockExchangeOfIndiaHinduHolidays)
        IslamicHolidays.__init__(
            self,
            cls=NationalStockExchangeOfIndiaIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, cls=NationalStockExchangeOfIndiaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_TO_NONE + SUN_TO_NONE)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Republic Day.
        self._move_holiday(self._add_holiday_jan_26(tr("Republic Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Dr. B. R. Ambedkar Jayanti.
        self._move_holiday(self._add_holiday_apr_14(tr("Dr. Baba Saheb Ambedkar Jayanti")))

        self._move_holiday(
            self._add_holiday_may_1(
                # May Day.
                tr("May Day")
                if 2010 <= self._year <= 2014
                # Maharashtra Day.
                else tr("Maharashtra Day")
            )
        )

        # Independence Day.
        self._move_holiday(self._add_holiday_aug_15(tr("Independence Day")))

        # Gandhi Jayanti.
        self._move_holiday(self._add_holiday_oct_2(tr("Mahatma Gandhi Jayanti")))

        # Christmas Day.
        self._move_holiday(self._add_christmas_day(tr("Christmas Day")))

        # Hindu Calendar Holidays.

        if self._year >= 2007:
            # Maha Shivaratri.
            self._move_holiday(self._add_maha_shivaratri(tr("Maha Shivaratri")))

        # Holi.
        self._move_holiday(self._add_holi(tr("Holi")))

        if self._year >= 2006:
            # Ram Navami.
            self._move_holiday(self._add_ram_navami(tr("Ram Navami")))

            # Mahavir Jayanti.
            self._move_holiday(self._add_mahavir_jayanti(tr("Mahavir Jayanti")))

        # Ganesh Chaturthi.
        self._move_holiday(self._add_ganesh_chaturthi(tr("Ganesh Chaturthi")))

        # Dussehra.
        self._move_holiday(self._add_dussehra(tr("Dussehra")))

        # Diwali Lakshmi Puja.
        self._move_holiday(self._add_diwali_india(tr("Diwali Laxmi Pujan")))

        if self._year <= 2002 or self._year >= 2011:
            # Diwali Balipratipada.
            self._move_holiday(self._add_govardhan_puja(tr("Diwali Balipratipada")))

        # Guru Nanak Jayanti.
        self._move_holiday(self._add_guru_nanak_jayanti(tr("Guru Nanak Jayanti")))

        if 2003 <= self._year <= 2010:
            # Bhai Dooj.
            self._move_holiday(self._add_bhai_dooj(tr("Bhau Bhij")))

        if 2006 <= self._year <= 2009:
            # Buddha Purnima.
            self._move_holiday(self._add_buddha_purnima(tr("Buddha Purnima")))

        # Islamic Calendar Holidays.

        # Ashura.
        for dt in self._add_ashura_day(tr("Muharram")):
            self._move_holiday(dt)

        if 2006 <= self._year <= 2009:
            # Prophet's Birthday.
            for dt in self._add_mawlid_day(tr("Id-E-Milad-Un-Nabi")):
                self._move_holiday(dt)

        # Eid al-Fitr.
        for dt in self._add_eid_al_fitr_day(tr("Id-Ul-Fitr (Ramadan Eid)")):
            self._move_holiday(dt)

        # Eid al-Adha.
        for dt in self._add_eid_al_adha_day(tr("Bakri Id")):
            self._move_holiday(dt)


class XNSE(NationalStockExchangeOfIndia):
    pass


class NSE(NationalStockExchangeOfIndia):
    pass


class NationalStockExchangeOfIndiaHinduHolidays(_CustomHinduHolidays):
    BUDDHA_PURNIMA_DATES = {
        2008: (MAY, 19),
    }

    DIWALI_INDIA_DATES = {
        2024: (NOV, 1),
        2025: (OCT, 21),
    }

    DUSSEHRA_DATES = {
        2003: (OCT, 4),
        2018: (OCT, 18),
    }

    GOVARDHAN_PUJA_DATES = {
        2001: (NOV, 16),
        2002: (NOV, 6),
        2003: (OCT, 25),
        2006: (OCT, 23),
        2020: (NOV, 16),
        2022: (OCT, 26),
        2023: (NOV, 14),
    }

    HOLI_DATES = {
        2023: (MAR, 7),
    }

    MAHAVIR_JAYANTI_DATES = {
        2010: (MAR, 28),
        2016: (APR, 19),
    }

    RAM_NAVAMI_DATES = {
        2007: (MAR, 27),
    }


class NationalStockExchangeOfIndiaIslamicHolidays(_CustomIslamicHolidays):
    ASHURA_DATES_CONFIRMED_YEARS = (2001, 2025)
    ASHURA_DATES = {
        2003: (MAR, 15),
        2006: (FEB, 9),
        2008: (JAN, 19),
        2015: (OCT, 23),
        2018: (SEP, 20),
    }

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_ADHA_DATES = {
        2003: (FEB, 13),
        2005: (JAN, 21),
        2006: (JAN, 11),
        2007: ((JAN, 1), (DEC, 21)),
        2009: (NOV, 27),
        2014: (OCT, 6),
        2015: (SEP, 25),
        2016: (SEP, 13),
        2023: (JUN, 28),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_FITR_DATES = {
        2002: (DEC, 7),
        2005: (NOV, 5),
        2006: (OCT, 25),
        2010: (SEP, 10),
        2016: (JUL, 6),
        2021: (MAY, 13),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2006, 2009)
    MAWLID_DATES = {
        2008: (MAR, 20),
    }


class NationalStockExchangeOfIndiaStaticHolidays:
    """National Stock Exchange of India (NSE) special holidays."""

    special_public_holidays = {
        # New Year's Day.
        2010: (JAN, 1, tr("New Year")),
    }
