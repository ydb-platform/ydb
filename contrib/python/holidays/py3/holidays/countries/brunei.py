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
from gettext import gettext as tr

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import (
    JAN,
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    MON,
    TUE,
    WED,
    THU,
    FRI,
    SAT,
    SUN,
)
from holidays.groups import (
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    ObservedRule,
    FRI_SUN_TO_NEXT_WORKDAY,
)

BN_WORKDAY_TO_NEXT_WORKDAY = ObservedRule({MON: +7, TUE: +7, WED: +7, THU: +7, SAT: +7})


class Brunei(
    ObservedHolidayBase,
    ChineseCalendarHolidays,
    ChristianHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Brunei holidays.

    References:
        * <https://web.archive.org/web/20250421085743/https://www.labour.gov.bn/lists/upcomming%20events/allitems.aspx>
        * <https://web.archive.org/web/20250421234214/https://www.labour.gov.bn/Download/GUIDE%20TO%20BRUNEI%20EMPLOYMENT%20LAWS%20-%20english%20version-3.pdf>
        * <https://web.archive.org/web/20250429080129/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk12-1997.pdf>
        * <https://web.archive.org/web/20250429080148/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk14-1998.pdf>
        * <https://web.archive.org/web/20250429080326/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk15-1998.pdf>
        * <https://web.archive.org/web/20250429080154/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk14-1999.pdf>
        * <https://web.archive.org/web/20250428210642/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk05-2000.pdf>
        * <https://web.archive.org/web/20250429075903/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk10-2000.pdf>
        * <https://web.archive.org/web/20250429080137/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk13-2000.pdf>
        * <https://web.archive.org/web/20250428210647/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk07-2001.pdf>
        * <https://web.archive.org/web/20250429080046/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk11-2002.pdf>
        * <https://web.archive.org/web/20250428210125/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk02-2003.pdf>
        * <https://web.archive.org/web/20250429075953/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk09-2004.pdf>
        * <https://web.archive.org/web/20250429080052/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk11-2005.pdf>
        * <https://web.archive.org/web/20250429080357/https://www.pmo.gov.bn/Circulars%20PDF%20Library/jpmsk12-2005.pdf>
        * <https://web.archive.org/web/20250428210640/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk03-2006.pdf>
        * <https://web.archive.org/web/20250429080354/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk17-2006.pdf>
        * <https://web.archive.org/web/20250429080005/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk10-2007.pdf>
        * <https://web.archive.org/web/20250428211158/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk09-2008.pdf>
        * <https://web.archive.org/web/20241105162103/https://chittychat.wordpress.com/wp-content/uploads/2008/11/school_terms_20091.pdf>
        * <https://web.archive.org/web/20250428210649/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk08-2009.pdf>
        * <https://web.archive.org/web/20250428211159/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk09-2010.pdf>
        * <https://web.archive.org/web/20250429080022/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk10-2011.pdf>
        * <https://web.archive.org/web/20250428211153/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk08-2012.pdf>
        * <https://web.archive.org/web/20250429084043/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk10-2012.pdf>
        * <https://web.archive.org/web/20250428210648/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk07-2013.pdf>
        * <https://web.archive.org/web/20250428211155/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk09-14.pdf>
        * <https://web.archive.org/web/20180713195340/http://www.jpm.gov.bn:80/Circulars%20PDF%20Library/jpmsk11-2015.pdf>
        * <https://web.archive.org/web/20180713195319/http://www.jpm.gov.bn:80/Circulars%20PDF%20Library/jpmsk09-2016.pdf>
        * <https://web.archive.org/web/20191124043940/http://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk07-2017.pdf>
        * <https://web.archive.org/web/20191124043933/http://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk08-2017.pdf>
        * <https://web.archive.org/web/20191124043741/http://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk11-2018.pdf>
        * <https://web.archive.org/web/20220125022931/http://jpm.gov.bn/Circulars%20PDF%20Library/jpmsk06-2019.pdf>
        * <https://web.archive.org/web/20240724020501/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk10-2020.pdf>
        * <https://web.archive.org/web/20240724020246/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk10-2021.pdf>
        * <https://web.archive.org/web/20240901140406/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk04-2022.pdf>
        * <https://web.archive.org/web/20250428091155/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk04-2023.pdf>
        * <https://web.archive.org/web/20241105052917/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk06-2024.pdf>
        * <https://web.archive.org/web/20260119103509/https://www.jpm.gov.bn/Circulars%20PDF%20Library/jpmsk04-2025.pdf>

    Checked with:
        * <https://web.archive.org/web/20250414071145/https://asean.org/wp-content/uploads/2021/12/ASEAN-National-Holidays-2022.pdf>
        * <https://web.archive.org/web/20250414071156/https://asean.org/wp-content/uploads/2022/12/ASEAN-Public-Holidays-2023.pdf>
        * <https://web.archive.org/web/20250130135348/https://www.timeanddate.com/holidays/brunei/>
        * [Jubli Emas Sultan Hassanal Bolkiah](https://web.archive.org/web/20241127203518/https://www.brudirect.com/news.php?id=28316)
    """

    country = "BN"
    default_language = "ms"
    # %s (estimated).
    estimated_label = tr("%s (anggaran)")
    # %s (observed).
    observed_label = tr("%s (diperhatikan)")
    # %s (observed, estimated).
    observed_estimated_label = tr("%s (diperhatikan, anggaran)")
    supported_languages = ("en_US", "ms", "th")
    weekend = {FRI, SUN}
    # Available post-Independence from 1984 afterwards.
    start_year = 1984

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChineseCalendarHolidays.__init__(self)
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=BruneiIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=BruneiStaticHolidays)
        kwargs.setdefault("observed_rule", FRI_SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _populate_observed(self, dts: set[date], *, multiple: bool = False) -> None:
        super()._populate_observed(dts, multiple=multiple)
        for dt in sorted(dts):
            if len(self.get_list(dt)) > 1:
                self._add_observed(dt, rule=BN_WORKDAY_TO_NEXT_WORKDAY)

    def _populate_public_holidays(self):
        dts_observed = set()

        # New Year's Day.
        dts_observed.add(self._add_new_years_day(tr("Awal Tahun Masihi")))

        # Lunar New Year.
        dts_observed.add(self._add_chinese_new_years_day(tr("Tahun Baru Cina")))

        # Starts in 1984.

        # National Day.
        dts_observed.add(self._add_holiday_feb_23(tr("Hari Kebangsaan")))

        # Starts in 1984.
        # Commemorates the formation of Royal Brunei Malay Regiment in 1961.

        dts_observed.add(
            # Armed Forces Day.
            self._add_holiday_may_31(tr("Hari Angkatan Bersenjata Diraja Brunei"))
        )

        # Started in 1968.

        dts_observed.add(
            # Sultan Hassanal Bolkiah's Birthday.
            self._add_holiday_jul_15(tr("Hari Keputeraan KDYMM Sultan Brunei"))
        )

        # Christmas Day.
        dts_observed.add(self._add_christmas_day(tr("Hari Natal")))

        # Islamic New Year.
        dts_observed.update(self._add_islamic_new_year_day(tr("Awal Tahun Hijrah")))

        # Prophet's Birthday.
        dts_observed.update(self._add_mawlid_day(tr("Maulidur Rasul")))

        # Isra' and Mi'raj.
        dts_observed.update(self._add_isra_and_miraj_day(tr("Israk dan Mikraj")))

        # First Day of Ramadan.
        dts_observed.update(self._add_ramadan_beginning_day(tr("Hari Pertama Berpuasa")))

        # Anniversary of the revelation of the Quran.
        dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

        # This is celebrate for three days in Brunei.
        # Observed as 'Hari Raya Puasa' and only for 2 days prior to 2012.
        # This was only observed for a single day in 2000.

        # Eid al-Fitr.
        name = tr("Hari Raya Aidil Fitri")
        dts_observed.update(self._add_eid_al_fitr_day(name))
        if self._year != 2000:
            dts_observed.update(self._add_eid_al_fitr_day_two(name))
        if self._year >= 2012:
            dts_observed.update(self._add_eid_al_fitr_day_three(name))

        # Eid al-Adha.
        dts_observed.update(self._add_eid_al_adha_day(tr("Hari Raya Aidil Adha")))

        if self.observed:
            self._populate_observed(dts_observed, multiple=True)


class BN(Brunei):
    pass


class BRN(Brunei):
    pass


class BruneiIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (1998, 2026)
    EID_AL_ADHA_DATES = {
        1999: (MAR, 28),
        2000: (MAR, 17),
        2001: (MAR, 6),
        2002: (FEB, 23),
        2003: (FEB, 12),
        2004: (FEB, 2),
        2006: (JAN, 11),
        2007: ((JAN, 1), (DEC, 20)),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 12),
        2018: (AUG, 22),
        2023: (JUN, 29),
        2024: (JUN, 17),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (1998, 2026)
    EID_AL_FITR_DATES = {
        1998: (JAN, 30),
        1999: (JAN, 19),
        2002: (DEC, 6),
        2005: (NOV, 4),
        2006: (OCT, 24),
        2019: (JUN, 5),
        2023: (APR, 22),
        2025: (MAR, 31),
        2026: (MAR, 21),
    }

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (1998, 2026)
    HIJRI_NEW_YEAR_DATES = {
        1998: (APR, 28),
        2004: (FEB, 22),
        2011: (NOV, 27),
        2013: (NOV, 5),
        2017: (SEP, 22),
        2019: (SEP, 1),
        2021: (AUG, 10),
        2025: (JUN, 27),
        2026: (JUN, 17),
    }

    ISRA_AND_MIRAJ_DATES_CONFIRMED_YEARS = (1998, 2026)
    ISRA_AND_MIRAJ_DATES = {
        1998: (NOV, 17),
        1999: (NOV, 6),
        2000: (OCT, 25),
        2001: (OCT, 15),
        2007: (AUG, 11),
        2010: (JUL, 10),
        2014: (MAY, 27),
        2016: (MAY, 5),
        2018: (APR, 14),
        2026: (JAN, 17),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (1998, 2026)
    MAWLID_DATES = {
        2000: (JUN, 15),
        2002: (MAY, 25),
        2003: (MAY, 14),
        2004: (MAY, 2),
        2006: (APR, 11),
        2012: (FEB, 5),
        2014: (JAN, 14),
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2021: (OCT, 19),
        2023: (SEP, 28),
        2024: (SEP, 16),
        2025: (SEP, 5),
    }

    NUZUL_AL_QURAN_DATES_CONFIRMED_YEARS = (1998, 2026)
    NUZUL_AL_QURAN_DATES = {
        1998: (JAN, 16),
        1999: ((JAN, 5), (DEC, 25)),
        2001: (DEC, 3),
        2003: (NOV, 12),
        2004: (NOV, 1),
        2005: (OCT, 21),
        2008: (SEP, 18),
        2012: (AUG, 6),
        2013: (JUL, 26),
        2014: (JUL, 15),
        2018: (JUN, 2),
        2022: (APR, 19),
        2024: (MAR, 28),
        2025: (MAR, 18),
        2026: (MAR, 7),
    }

    RAMADAN_BEGINNING_DATES_CONFIRMED_YEARS = (1998, 2026)
    RAMADAN_BEGINNING_DATES = {
        1998: (DEC, 20),
        2001: (NOV, 17),
        2003: (OCT, 27),
        2004: (OCT, 16),
        2005: (OCT, 5),
        2012: (JUL, 21),
        2013: (JUL, 10),
        2014: (JUN, 29),
        2018: (MAY, 17),
        2022: (APR, 3),
        2024: (MAR, 12),
        2025: (MAR, 2),
        2026: (FEB, 19),
    }


class BruneiStaticHolidays:
    special_public_holidays = {
        1998: (
            AUG,
            10,
            # Proclamation Ceremony of Crown Prince Al-Muhtadee Billah of Brunei.
            tr("Istiadat Pengisytiharan Duli Pengiran Muda Mahkota Al-Muhtadee Billah"),
        ),
        # Royal Wedding of Crown Prince Al-Muhtadee Billah and Crown Princess Sarah of Brunei.
        2004: (SEP, 9, tr("Istiadat Perkahwinan Diraja Brunei 2004")),
        # Sultan Hassanal Bolkiah's Golden Jubilee celebration.
        2017: (OCT, 5, tr("Jubli Emas Sultan Hassanal Bolkiah")),
    }
