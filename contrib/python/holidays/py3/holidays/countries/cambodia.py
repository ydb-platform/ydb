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

from holidays.calendars.gregorian import APR, MAY, AUG, SEP, _timedelta
from holidays.calendars.thai import KHMER_CALENDAR
from holidays.groups import InternationalHolidays, StaticHolidays, ThaiCalendarHolidays
from holidays.holiday_base import HolidayBase


class Cambodia(HolidayBase, InternationalHolidays, StaticHolidays, ThaiCalendarHolidays):
    """Cambodia holidays.

    References:
        * <https://web.archive.org/web/20241117220849/https://www.nbc.gov.kh/english/news_and_events/official_holiday.php>
        * <https://web.archive.org/web/20250401111433/https://www.nbc.gov.kh/news_and_events/official_holiday.php>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Cambodia>
        * <https://web.archive.org/web/20250427180236/https://admin.taftac-cambodia.org/public/pdf_file/regulation_1704245695_Z7IALJjE.pdf>
        * <http://archive.today/2026.01.22-052218/https://www.khmertimeskh.com/501424903/24-public-holidays-for-2024-peace-day-now-included/>
        * <http://archive.today/2026.01.22-051938/https://www.khmertimeskh.com/501551204/govt-announces-22-public-holidays-for-next-year/>

    Checked with:
        * <https://web.archive.org/web/20250414071145/https://asean.org/wp-content/uploads/2021/12/ASEAN-National-Holidays-2022.pdf>
        * <https://web.archive.org/web/20250414071156/https://asean.org/wp-content/uploads/2022/12/ASEAN-Public-Holidays-2023.pdf>
        * <https://web.archive.org/web/20250416140353/https://www.timeanddate.com/holidays/cambodia/>

    Limitations:
        * Cambodian holidays only works from 1993 onwards.
        * Exact Public Holidays as per Cambodia's Official Gazette are only available
            from 2015 onwards.
        * Cambodian Lunar Calendar Holidays only work from 1941 (B.E. 2485) onwards until 2157
            (B.E. 2701) as we only have Thai year-type data for cross-checking until then.
    """

    country = "KH"
    default_language = "km"
    supported_languages = ("en_US", "km", "th")
    # Available post-Independence from 1993 afterwards
    start_year = 1993

    def __init__(self, *args, **kwargs):
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=CambodiaStaticHolidays)
        ThaiCalendarHolidays.__init__(self, KHMER_CALENDAR)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Fixed Holidays

        #  ទិវាចូលឆ្នាំសាកល
        # Status: In-Use.

        # International New Year Day.
        self._add_new_years_day(tr("ទិវាចូលឆ្នាំសាកល"))

        #  ទិវាជ័យជម្នះលើរបបប្រល័យពូជសាសន៍
        # Status: In-Use.
        # Commemorates the end of the Khmer Rouge regime in 1979

        # Day of Victory over the Genocidal Regime.
        self._add_holiday_jan_7(tr("ទិវាជ័យជម្នះលើរបបប្រល័យពូជសាសន៍"))

        # ទិវាអន្តរជាតិនារី
        # Status: In-Use.

        # International Women's Rights Day.
        self._add_womens_day(tr("ទិវាអន្តរជាតិនារី"))

        #  ពិធីបុណ្យចូលឆ្នាំថ្មីប្រពៃណីជាតិ
        # Status: In-Use.
        # Usually falls on April 13th except for 2017-2018, 2021-2023, 2025-2027, 2029-2031
        # for years 2001-2050.

        if self._year != 2020:
            # Khmer New Year's Day.
            sangkranta = tr("ពិធីបុណ្យចូលឆ្នាំថ្មីប្រពៃណីជាតិ")
            sangkranta_years_apr_14 = {
                2017,
                2018,
                2021,
                2022,
                2023,
                2025,
                2026,
                2027,
                2029,
                2030,
                2031,
            }
            dt = (
                self._add_holiday_apr_14(sangkranta)
                if self._year in sangkranta_years_apr_14
                else self._add_holiday_apr_13(sangkranta)
            )
            self._add_holiday(sangkranta, _timedelta(dt, +1))
            self._add_holiday(sangkranta, _timedelta(dt, +2))

        #  ទិវាពលកម្មអន្តរជាតិ
        # Status: In-Use.

        # International Labor Day.
        self._add_labor_day(tr("ទិវាពលកម្មអន្តរជាតិ"))

        # ព្រះរាជពិធីបុណ្យចម្រើនព្រះជន្ម ព្រះករុណា ព្រះបាទសម្តេចព្រះបរមនាថ នរោត្តម សីហមុនី
        # Status: In-Use.
        # Assumed to start in 2005. Was celebrated for 3 days until 2020.

        if self._year >= 2005:
            king_sihamoni_bday = tr(
                # Birthday of His Majesty Preah Bat Samdech Preah Boromneath
                # NORODOM SIHAMONI, King of Cambodia.
                "ព្រះរាជពិធីបុណ្យចម្រើនព្រះជន្ម ព្រះករុណា ព្រះបាទសម្តេចព្រះបរមនាថ នរោត្តម សីហមុនី"
            )
            self._add_holiday_may_14(king_sihamoni_bday)
            if self._year <= 2019:
                self._add_holiday_may_13(king_sihamoni_bday)
                self._add_holiday_may_15(king_sihamoni_bday)

        # ទិវាជាតិនៃការចងចាំ
        # Status: Defunct.
        # Active between 2018-2019 as Public Holiday.
        # Was ទិវាចងកំហឹង (National Day of Anger) between 1983-2000.
        # Its celebration was put onhold by UN administration with
        # its name changed to present one in 2001.

        if 2018 <= self._year <= 2019:
            # National Day of Remembrance.
            self._add_holiday_may_20(tr("ទិវាជាតិនៃការចងចាំ"))

        # ទិវាកុមារអន្តរជាតិ
        # Status: Defunct.
        # Assumed to start in 1993, defunct from 2020 onwards.

        if self._year <= 2019:
            # International Children's Day.
            self._add_childrens_day(tr("ទិវាកុមារអន្តរជាតិ"))

        # ព្រះរាជពិធីបុណ្យចម្រើនព្រះជន្ម សម្តេចព្រះមហាក្សត្រី ព្រះវររាជមាតា នរោត្តម មុនិនាថ សីហនុ
        # Status: In-Use.
        # Assumed to start in 1994. A public holiday since 2015 at least.

        if self._year >= 1994:
            self._add_holiday_jun_18(
                # Birthday of Her Majesty the Queen-Mother NORODOM MONINEATH SIHANOUK of Cambodia.
                tr("ព្រះរាជពិធីបុណ្យចម្រើនព្រះជន្ម សម្តេចព្រះមហាក្សត្រី ព្រះវររាជមាតា នរោត្តម មុនិនាថ សីហនុ")
            )

        # ទិវាប្រកាសរដ្ឋធម្មនុញ្ញ
        # Status: In-Use.
        # Starts in 1993

        # Constitution Day.
        self._add_holiday_sep_24(tr("ទិវាប្រកាសរដ្ឋធម្មនុញ្ញ"))

        # ទិវាប្រារព្ឋពិធីគោរពព្រះវិញ្ញាណក្ខន្ឋ ព្រះករុណា ព្រះបាទសម្តេចព្រះ នរោត្តម សីហនុ
        # ព្រះមហាវីរក្សត្រ ព្រះវររាជបិតាឯករាជ្យ បូរណភាពទឹកដី និងឯកភាពជាតិខ្មែរ ព្រះបរមរតនកោដ្ឋ
        # Status: In-Use.
        # Starts in 2012.

        if self._year >= 2012:
            self._add_holiday_oct_15(
                # Mourning Day of the Late King-Father NORODOM SIHANOUK of Cambodia.
                tr(
                    "ទិវាប្រារព្ឋពិធីគោរពព្រះវិញ្ញាណក្ខន្ឋ ព្រះករុណា ព្រះបាទសម្តេចព្រះ នរោត្តម "
                    "សីហនុ ព្រះមហាវីរក្សត្រ ព្រះវររាជបិតាឯករាជ្យ បូរណភាពទឹកដី និងឯកភាពជាតិខ្មែរ "
                    "ព្រះបរមរតនកោដ្ឋ"
                )
            )

        # ទិវារំលឹកសន្ធិសញ្ញាសន្តិភាពទីក្រុងប៉ារីស
        # Status: Defunct.
        # Assumed to start in 1993, defunct from 2020 onwards.

        if self._year <= 2019:
            # Paris Peace Agreement's Day.
            self._add_holiday_oct_23(tr("ទិវារំលឹកសន្ធិសញ្ញាសន្តិភាពទីក្រុងប៉ារីស"))

        # ព្រះរាជពិធីគ្រងព្រះបរមរាជសម្បត្តិ របស់ ព្រះករុណា
        # ព្រះបាទសម្តេចព្រះបរមនាថ នរោត្តម សីហមុនី
        # ព្រះមហាក្សត្រនៃព្រះរាជាណាចក្រកម្ពុជា
        # Status: In-Use.
        # Starts in 2004.

        if self._year >= 2004:
            self._add_holiday_oct_29(
                # Coronation Day of His Majesty Preah Bat Samdech Preah
                # Boromneath NORODOM SIHAMONI, King of Cambodia.
                tr(
                    "ព្រះរាជពិធីគ្រងព្រះបរមរាជសម្បត្តិ របស់ ព្រះករុណា "
                    "ព្រះបាទសម្តេចព្រះបរមនាថ នរោត្តម សីហមុនី "
                    "ព្រះមហាក្សត្រនៃព្រះរាជាណាចក្រកម្ពុជា"
                )
            )

        # ពិធីបុណ្យឯករាជ្យជាតិ
        # Status: In-Use.
        # Starts in 1953

        # National Independence Day.
        self._add_holiday_nov_9(tr("ពិធីបុណ្យឯករាជ្យជាតិ"))

        # ទិវាសិទ្ធិមនុស្សអន្តរជាតិ
        # Status: Defunct.
        # Assumed to start in 1993, defunct from 2020 onwards.

        if self._year <= 2019:
            # International Human Rights Day.
            self._add_holiday_dec_10(tr("ទិវាសិទ្ធិមនុស្សអន្តរជាតិ"))

        # Cambodian Lunar Calendar Holidays
        # See `_ThaiLunisolar` in holidays/calendars/thai.py for more details.
        # Cambodian Lunar Calendar Holidays only work from 1941 to 2157.

        # ពិធីបុណ្យមាឃបូជា
        # Status: Defunct.
        # 15th Waxing Day of Month 3.
        # Defunct from 2020 onwards.

        if self._year <= 2019:
            # Meak Bochea Day.
            self._add_makha_bucha(tr("ពិធីបុណ្យមាឃបូជា"))

        # ពិធីបុណ្យវិសាខបូជា
        # Status: In-Use.
        # 15th Waxing Day of Month 6.
        # This utilizes Thai calendar as a base, though are calculated to always happen
        # in the Traditional Visakhamas month (May).

        # Visaka Bochea Day.
        self._add_visakha_bucha(tr("ពិធីបុណ្យវិសាខបូជា"))

        # ព្រះរាជពិធីច្រត់ព្រះនង្គ័ល
        # Status: In-Use.
        # 4th Waning Day of Month 6.
        # Unlike Thai ones, Cambodian Royal Ploughing Ceremony is always fixed.

        # Royal Ploughing Ceremony.
        self._add_preah_neangkoal(tr("ព្រះរាជពិធីច្រត់ព្រះនង្គ័ល"))

        # ពិធីបុណ្យភ្ផុំបិណ្ឌ
        # Status: In-Use.
        # 14th Waning Day of Month 10 - 1st Waxing Day of Month 11.
        # The 3rd day is added as a public holiday from 2017 onwards.

        # Pchum Ben Day.
        pchum_ben = tr("ពិធីបុណ្យភ្ផុំបិណ្ឌ")
        pchum_ben_date = self._add_pchum_ben(pchum_ben)
        self._add_holiday(pchum_ben, _timedelta(pchum_ben_date, -1))
        if self._year >= 2017:
            self._add_holiday(pchum_ben, _timedelta(pchum_ben_date, +1))

        # ព្រះរាជពិធីបុណ្យអុំទូក បណ្តែតប្រទីប និងសំពះព្រះខែអកអំបុក
        # Status: In-Use.
        # 14th Waxing Day of Month 12 - 1st Waning Day of Month 12.

        # Water Festival.
        bon_om_touk = tr("ព្រះរាជពិធីបុណ្យអុំទូក បណ្តែតប្រទីប និងសំពះព្រះខែអកអំបុក")
        bon_om_touk_date = self._add_loy_krathong(bon_om_touk)
        self._add_holiday(bon_om_touk, _timedelta(bon_om_touk_date, -1))
        self._add_holiday(bon_om_touk, _timedelta(bon_om_touk_date, +1))

        # ទិវាសន្តិភាពនៅកម្ពុជា
        # Status: In-Use.
        # Dec 29, added from 2024 onwards.

        if self._year >= 2024:
            # Peace Day in Cambodia.
            self._add_holiday_dec_29(tr("ទិវាសន្តិភាពនៅកម្ពុជា"))


class KH(Cambodia):
    pass


class KHM(Cambodia):
    pass


class CambodiaStaticHolidays:
    sangkranta_in_lieu_covid = tr(
        # Khmer New Year's Replacement Holiday.
        "ថ្ងៃឈប់សម្រាកសងជំនួសឲ្យពិធីបុណ្យចូលឆ្នាំថ្មីប្រពៃណីជាតិ"
    )
    # Special Public Holiday.
    special_in_lieu_holidays = tr("ថ្ងៃឈប់សម្រាកសងជំនួស")

    # Khmer New Year's Day.
    sangkranta = tr("ពិធីបុណ្យចូលឆ្នាំថ្មីប្រពៃណីជាតិ")

    special_public_holidays = {
        2016: (
            (MAY, 2, special_in_lieu_holidays),
            (MAY, 16, special_in_lieu_holidays),
        ),
        2018: (MAY, 21, special_in_lieu_holidays),
        2019: (SEP, 30, special_in_lieu_holidays),
        2020: (
            (MAY, 11, special_in_lieu_holidays),
            (AUG, 17, sangkranta_in_lieu_covid),
            (AUG, 18, sangkranta_in_lieu_covid),
            (AUG, 19, sangkranta_in_lieu_covid),
            (AUG, 20, sangkranta_in_lieu_covid),
            (AUG, 21, sangkranta_in_lieu_covid),
        ),
        2024: (APR, 16, sangkranta),
    }
