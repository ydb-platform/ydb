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
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
from holidays.constants import HALF_DAY, PUBLIC
from holidays.groups import InternationalHolidays, IslamicHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Turkey(HolidayBase, InternationalHolidays, IslamicHolidays, StaticHolidays):
    """Turkey holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Turkey>
        * [Law 2739 of May 27, 1935](https://web.archive.org/web/20250102114014/https://www5.tbmm.gov.tr/tutanaklar/KANUNLAR_KARARLAR/kanuntbmmc015/kanuntbmmc015/kanuntbmmc01502739.pdf)
        * [Law 2429 of March 19, 1981](https://web.archive.org/web/20250121111504/http://www.mevzuat.gov.tr/MevzuatMetin/1.5.2429.pdf)
        * [Hijri calendar holidays](https://web.archive.org/web/20250415045516/https://vakithesaplama.diyanet.gov.tr/hicriden_miladiye.php)
    """

    country = "TR"
    default_language = "tr"
    # %s (estimated).
    estimated_label = tr("%s (tahmini)")
    supported_categories = (HALF_DAY, PUBLIC)
    supported_languages = ("en_US", "tr", "uk")
    # Law 2739 of May 27, 1935.
    start_year = 1936

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=TurkeyIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, TurkeyStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Yılbaşı"))

        self._add_holiday_apr_23(
            # National Sovereignty and Children's Day.
            tr("Ulusal Egemenlik ve Çocuk Bayramı")
            if self._year >= 1981
            # National Sovereignty Day.
            else tr("Ulusal Egemenlik Bayramı")
        )

        if self._year <= 1980:
            # Spring Day.
            self._add_labor_day(tr("Bahar Bayramı"))
        elif self._year >= 2009:
            # Labour and Solidarity Day.
            self._add_labor_day(tr("Emek ve Dayanışma Günü"))

        self._add_holiday_may_19(
            # Commemoration of Atatürk, Youth and Sports Day.
            tr("Atatürk'ü Anma, Gençlik ve Spor Bayramı")
            if self._year >= 1981
            # Youth and Sports Day.
            else tr("Gençlik ve Spor Bayramı")
        )

        if 1963 <= self._year <= 1980:
            # Freedom and Constitution Day.
            self._add_holiday_may_27(tr("Hürriyet ve Anayasa Bayramı"))

        if self._year >= 2017:
            # Democracy and National Unity Day.
            self._add_holiday_jul_15(tr("Demokrasi ve Millî Birlik Günü"))

        # Victory Day.
        self._add_holiday_aug_30(tr("Zafer Bayramı"))

        # Republic Day.
        name = tr("Cumhuriyet Bayramı")
        self._add_holiday_oct_29(name)
        if self._year <= 1980:
            self._add_holiday_oct_30(name)

        # Eid al-Fitr.
        name = tr("Ramazan Bayramı")
        self._add_eid_al_fitr_day(name)
        self._add_eid_al_fitr_day_two(name)
        self._add_eid_al_fitr_day_three(name)

        # Eid al-Adha.
        name = tr("Kurban Bayramı")
        self._add_eid_al_adha_day(name)
        self._add_eid_al_adha_day_two(name)
        self._add_eid_al_adha_day_three(name)
        self._add_eid_al_adha_day_four(name)

    def _populate_half_day_holidays(self):
        # %s (from 1pm).
        begin_time_label = self.tr("%s (saat 13.00'ten)")

        # Republic Day.
        self._add_holiday_oct_28(begin_time_label % self.tr("Cumhuriyet Bayramı"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_eve(begin_time_label % self.tr("Ramazan Bayramı"))

        # Eid al-Adha.
        self._add_arafah_day(begin_time_label % self.tr("Kurban Bayramı"))


class TR(Turkey):
    pass


class TUR(Turkey):
    pass


class TurkeyIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (1936, 2032)
    EID_AL_ADHA_DATES = {
        1937: (FEB, 22),
        1938: (FEB, 11),
        1939: (JAN, 31),
        1941: ((JAN, 8), (DEC, 29)),
        1943: (DEC, 8),
        1944: (NOV, 26),
        1949: (OCT, 3),
        1958: (JUN, 28),
        1960: (JUN, 5),
        1963: (MAY, 4),
        1964: (APR, 23),
        1965: (APR, 12),
        1968: (MAR, 10),
        1970: (FEB, 17),
        1972: (JAN, 27),
        1973: (JAN, 15),
        1974: ((JAN, 4), (DEC, 24)),
        1976: (DEC, 2),
        1977: (NOV, 22),
        1978: (NOV, 11),
        1984: (SEP, 6),
        1986: (AUG, 16),
        1987: (AUG, 5),
        1988: (JUL, 24),
        1990: (JUL, 3),
        1991: (JUN, 23),
        1993: (JUN, 1),
        1994: (MAY, 21),
        1995: (MAY, 10),
        1996: (APR, 28),
        1997: (APR, 18),
        1999: (MAR, 28),
        2005: (JAN, 20),
        2012: (OCT, 25),
        2015: (SEP, 24),
        2016: (SEP, 12),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (1936, 2032)
    EID_AL_FITR_DATES = {
        1939: (NOV, 13),
        1941: (OCT, 22),
        1942: (OCT, 12),
        1943: (OCT, 1),
        1944: (SEP, 19),
        1945: (SEP, 8),
        1946: (AUG, 29),
        1949: (JUL, 27),
        1952: (JUN, 24),
        1956: (MAY, 12),
        1959: (APR, 9),
        1960: (MAR, 29),
        1962: (MAR, 8),
        1963: (FEB, 25),
        1964: (FEB, 15),
        1965: (FEB, 3),
        1966: (JAN, 23),
        1969: (DEC, 11),
        1970: (DEC, 1),
        1971: (NOV, 20),
        1972: (NOV, 8),
        1973: (OCT, 28),
        1974: (OCT, 17),
        1976: (SEP, 25),
        1977: (SEP, 15),
        1978: (SEP, 4),
        1979: (AUG, 24),
        1982: (JUL, 22),
        1983: (JUL, 12),
        1985: (JUN, 20),
        1986: (JUN, 9),
        1987: (MAY, 29),
        1988: (MAY, 17),
        1991: (APR, 16),
        1995: (MAR, 3),
        1996: (FEB, 20),
        1997: (FEB, 9),
        1999: (JAN, 19),
        2007: (OCT, 12),
        2008: (SEP, 30),
        2010: (SEP, 9),
        2016: (JUL, 5),
    }


class TurkeyStaticHolidays:
    special_public_holidays = {
        # Public holiday.
        1999: (DEC, 31, tr("Genel tati̇l"))
    }
