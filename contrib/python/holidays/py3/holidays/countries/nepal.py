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

from holidays.calendars import _CustomIslamicHolidays, _CustomHinduHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, AUG, SEP, OCT, NOV
from holidays.constants import PUBLIC, WORKDAY
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.holiday_base import HolidayBase


class Nepal(
    HolidayBase,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Nepal holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Nepal>
        * <https://web.archive.org/web/20250422191700/https://narayanilawfirm.org.np/list-of-public-holidays-in-nepal-2079/>
        * <https://web.archive.org/web/20250131082346/https://www.timeanddate.com/holidays/nepal/>
        * <https://web.archive.org/web/20240911172745/https://kathmandupost.com/national/2018/03/29/government-trims-public-holidays-by-22-days>
        * <https://web.archive.org/web/20250419211438/https://kathmandupost.com/national/2021/02/13/two-years-after-shortening-public-holiday-list-government-starts-adding-to-it-again>
        * <https://web.archive.org/web/20250316094224/https://english.hamropatro.com/nepali-public-holidays>
        * <https://web.archive.org/web/20250527164012/https://english.hamropatro.com/calendar>
        * <https://web.archive.org/web/20250322000610/https://www.ashesh.com.np/nepali-calendar/>
        * <https://web.archive.org/web/20250117021312/https://www.bolpatra.gov.np/egp/openDateConverter>
        * <https://web.archive.org/web/20241013164133/https://www.moha.gov.np/en/page/holidays>
    """

    country = "NP"
    default_language = "ne"
    # %s (estimated).
    estimated_label = tr("%s (अनुमानित)")
    start_year = 2010
    supported_categories = (PUBLIC, WORKDAY)
    supported_languages = ("en_US", "ne")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Nepal, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=NepalHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=NepalIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        StaticHolidays.__init__(self, cls=NepalStaticHolidays)
        super().__init__(*args, **kwargs)

    def _add_non_continuous_holidays(self, *, is_workday: bool = False):
        """Holidays removed by MoHA between 2019-2020."""
        if (2019 <= self._year <= 2020) == is_workday:
            martyrs_day_dates = {
                2010: (JAN, 30),
                2011: (JAN, 30),
                2012: (JAN, 30),
                2013: (JAN, 29),
                2014: (JAN, 30),
                2015: (JAN, 30),
                2016: (JAN, 30),
                2017: (JAN, 29),
                2018: (JAN, 30),
                2019: (JAN, 30),
                2020: (JAN, 30),
                2021: (JAN, 29),
                2022: (JAN, 30),
                2023: (JAN, 30),
                2024: (JAN, 30),
                2025: (JAN, 29),
                2026: (JAN, 29),
                2027: (JAN, 29),
                2028: (JAN, 29),
                2029: (JAN, 29),
                2030: (JAN, 29),
                2031: (JAN, 30),
                2032: (JAN, 30),
                2033: (JAN, 29),
            }
            if dt := martyrs_day_dates.get(self._year):
                # Martyr's Day.
                self._add_holiday(tr("शहीद दिवस"), dt)

            democracy_day_dates = {
                2010: (FEB, 19),
                2011: (FEB, 19),
                2012: (FEB, 19),
                2013: (FEB, 18),
                2014: (FEB, 19),
                2015: (FEB, 19),
                2016: (FEB, 19),
                2017: (FEB, 18),
                2018: (FEB, 19),
                2019: (FEB, 19),
                2020: (FEB, 19),
                2021: (FEB, 19),
                2022: (FEB, 19),
                2023: (FEB, 19),
                2024: (FEB, 19),
                2025: (FEB, 19),
                2026: (FEB, 19),
                2027: (FEB, 19),
                2028: (FEB, 19),
                2029: (FEB, 19),
                2030: (FEB, 19),
                2031: (FEB, 20),
                2032: (FEB, 20),
                2033: (FEB, 19),
            }
            if dt := democracy_day_dates.get(self._year):
                # National Democracy Day.
                self._add_holiday(tr("राष्ट्रिय प्रजातन्त्र दिवस"), dt)

            republic_day_dates = {
                2010: (MAY, 29),
                2011: (MAY, 29),
                2012: (MAY, 28),
                2013: (MAY, 29),
                2014: (MAY, 29),
                2015: (MAY, 29),
                2016: (MAY, 28),
                2017: (MAY, 29),
                2018: (MAY, 29),
                2019: (MAY, 29),
                2020: (MAY, 28),
                2021: (MAY, 29),
                2022: (MAY, 29),
                2023: (MAY, 29),
                2024: (MAY, 28),
                2025: (MAY, 28),
                2026: (MAY, 29),
                2027: (MAY, 29),
                2028: (MAY, 28),
                2029: (MAY, 28),
                2030: (MAY, 29),
                2031: (MAY, 29),
                2032: (MAY, 28),
            }
            if dt := republic_day_dates.get(self._year):
                # Established in 2009.

                # Republic Day.
                self._add_holiday(tr("गणतन्त्र दिवस"), dt)

            # Hindu Holidays.

            # Ram Navami.
            self._add_ram_navami(tr("राम नवमी"))

            # Janai Poornima.
            self._add_raksha_bandhan(tr("जनै पूर्णिमा"))

            # Shree Krishna Janmashtami.
            self._add_janmashtami(tr("श्रीकृष्ण जन्माष्टमी"))

            # Ghatasthapana.
            self._add_sharad_navratri(tr("घटस्थापना"))

            # Duwadashi (Dashain).
            self._add_papankusha_duwadashi(tr("दुवादशी (दशैं)"))

    def _populate_public_holidays(self):
        if self._year >= 2023:
            # Prithvi Jayanti.
            self._add_holiday_jan_11(tr("पृथ्वी जयन्ती"))

        # International Women's Day.
        self._add_womens_day(tr("अन्तर्राष्ट्रिय महिला दिवस"))

        # Nepali New Year.
        self._add_vaisakhi(tr("नेपाली नयाँ वर्ष"))

        # International Labor Day.
        self._add_labor_day(tr("अन्तर्राष्ट्रिय श्रम दिवस"))

        constitution_day_dates = {
            2016: (SEP, 19),
            2017: (SEP, 19),
            2018: (SEP, 19),
            2019: (SEP, 20),
            2020: (SEP, 19),
            2021: (SEP, 19),
            2022: (SEP, 19),
            2023: (SEP, 20),
            2024: (SEP, 19),
            2025: (SEP, 19),
            2026: (SEP, 19),
            2027: (SEP, 19),
            2028: (SEP, 18),
            2029: (SEP, 19),
            2030: (SEP, 19),
            2031: (SEP, 19),
            2032: (SEP, 19),
        }
        if dt := constitution_day_dates.get(self._year):
            # Constitution Day.
            self._add_holiday(tr("संविधान दिवस"), dt)

        # Christmas Day.
        self._add_christmas_day(tr("क्रिसमसको दिन"))

        # Hindu holidays.

        # Maghe Sankranti.
        self._add_makar_sankranti(tr("माघे संक्रान्ति"))

        # Sonam Lhochhar.
        self._add_sonam_losar(tr("सोनम ल्होछार"))

        # Maha Shivaratri.
        self._add_maha_shivaratri(tr("महाशिवरात्रि"))

        # Gyalpo Lhosar.
        self._add_gyalpo_losar(tr("ग्याल्पो ल्होसार"))

        # Fagu Poornima.
        self._add_nepal_holi(tr("फागुपुर्णिमा"))

        # Fagu Poornima (Terai).
        self._add_holi(tr("फागुपूर्णिमा (तराई)"))

        # Buddha Jayanti.
        self._add_buddha_purnima(tr("बुद्ध जयन्ती"))

        # Fulpati.
        self._add_maha_saptami(tr("फुलपाती"))

        # Maha Ashtami.
        self._add_maha_ashtami(tr("महा अष्टमी"))

        # Maha Navami.
        self._add_maha_navami(tr("महा नवमी"))

        # Bijaya Dashami.
        self._add_dussehra(tr("विजया दशमी"))

        # Ekadashi (Dashain).
        self._add_papankusha_ekadashi(tr("एकादशी (दशैं)"))

        # Laxmi Pooja.
        self._add_diwali_india(tr("लक्ष्मी पुजा"))

        # Gai Tihar.
        self._add_gau_krida(tr("गाई तिहार"))

        # Gobardhan Pooja.
        self._add_govardhan_puja(tr("गोवर्धन पूजा"))

        # Mha Pooja.
        self._add_govardhan_puja(tr("म्ह पूजा"))

        # Bhai Tika.
        self._add_bhai_dooj(tr("भाइटीका"))

        # Chhath Parva.
        self._add_chhath_puja(tr("छठ पर्व"))

        # Tamu Lhochhar.
        self._add_tamu_losar(tr("तमु ल्हाेसार"))

        # Islamic holidays.

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("इद उल फित्र"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("बकरीद"))

        # Removed by MoHA between 2019-2020.
        self._add_non_continuous_holidays()

    def _populate_workday_holidays(self):
        self._add_non_continuous_holidays(is_workday=True)


class NP(Nepal):
    pass


class NPL(Nepal):
    pass


class NepalHinduHolidays(_CustomHinduHolidays):
    # Maghe Sankranti.
    MAKAR_SANKRANTI_DATES = {
        2022: (JAN, 15),
        2023: (JAN, 15),
        2024: (JAN, 15),
        2026: (JAN, 15),
    }

    # Fagu Poornima (Terai).
    HOLI_DATES = {
        2023: (MAR, 7),
        2026: (MAR, 3),
    }

    # Ram Navami.
    RAM_NAVAMI_DATES = {
        2026: (MAR, 27),
    }

    # Nepal New Year.
    VAISAKHI_DATES = {
        2025: (APR, 14),
    }

    # Janai Punima.
    RAKSHA_BANDHAN_DATES = {
        2022: (AUG, 12),
        2023: (AUG, 31),
    }

    # Shree Krishna Janmashtami.
    JANMASHTAMI_DATES = {
        2023: (SEP, 6),
    }

    # Maha Ashtami.
    MAHA_ASHTAMI_DATES = {
        2020: (OCT, 24),
    }

    # Maha Navami.
    MAHA_NAVAMI_DATES = {
        2020: (OCT, 25),
    }

    # Bijaya Dashami.
    DUSSEHRA_DATES = {
        2020: (OCT, 26),
    }

    # Gobardhan Pooja.
    GOVARDHAN_PUJA_DATES = {
        2020: (NOV, 16),
        2022: (OCT, 26),
        2023: (NOV, 14),
    }

    # Chhath Parva.
    CHHATH_PUJA_DATES = {
        2025: (OCT, 27),
    }


class NepalIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_ADHA_DATES = {
        2014: (OCT, 6),
        2015: (SEP, 25),
        2016: (SEP, 13),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2010, 2025)
    EID_AL_FITR_DATES = {
        2010: (SEP, 10),
        2013: (AUG, 8),
    }


class NepalStaticHolidays:
    """Nepal special holidays.

    References:
        * [Death of Krishna Prasad Bhattarai](https://web.archive.org/web/20250527164153/https://myrepublica.nagariknetwork.com/news/27618/)
        * [Death of Sushil Koirala](https://web.archive.org/web/20250527164210/https://kathmandupost.com/miscellaneous/2016/02/09/cabinet-decision-koirala-to-be-honoured-with-state-funeral)
        * [Crash of Yeti Airlines Flight 691](https://web.archive.org/web/20250123204057/https://edition.cnn.com/2023/01/15/asia/nepal-yeti-airlines-crash-intl-hnk/index.html)
        * [People's War Day Instituted](https://web.archive.org/web/20241211215602/https://kathmandupost.com/national/2023/02/12/government-announces-public-holiday-on-monday-to-mark-people-s-war-day)
        * [People's War Day Annulled](https://web.archive.org/web/20240111230228/https://kathmandupost.com/national/2023/12/29/supreme-court-annuls-public-holiday-on-people-s-war-day)
        * [Death of Subas Chandra Nembang](https://web.archive.org/web/20241214193305/https://en.nepalkhabar.com/news/detail/6023/)
    """

    # Day of National Mourning.
    name_day_of_national_mourning = tr("राष्ट्रिय शोक दिवस")

    # Tihar Holiday.
    name_tihar_holiday = tr("तिहार बिदा")

    special_public_holidays = {
        2011: (MAR, 6, name_day_of_national_mourning),
        2016: (FEB, 10, name_day_of_national_mourning),
        2021: (NOV, 7, name_tihar_holiday),
        2022: (OCT, 28, name_tihar_holiday),
        2023: (
            (JAN, 16, name_day_of_national_mourning),
            # People War's Day.
            (FEB, 13, tr("जनयुद्ध दिवस")),
            (SEP, 14, name_day_of_national_mourning),
            (NOV, 16, name_tihar_holiday),
        ),
        2024: (NOV, 4, name_tihar_holiday),
        2025: (OCT, 24, name_tihar_holiday),
    }
