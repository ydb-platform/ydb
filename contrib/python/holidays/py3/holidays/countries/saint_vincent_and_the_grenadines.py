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

from holidays.calendars.gregorian import JAN, JUL, AUG, SEP, _timedelta
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_NEXT_MON, SUN_TO_NEXT_TUE


class SaintVincentAndTheGrenadines(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Saint Vincent and the Grenadines holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Saint_Vincent_and_the_Grenadines>
        * [2013](https://web.archive.org/web/20240225145904/https://www.bosvg.com/wp-content/uploads/2021/10/bosvg_calendar2013-9x12_ver2-2.pdf)
        * [2014](https://web.archive.org/web/20250610053913/https://www.bosvg.com/wp-content/uploads/2021/10/BOSVG-Calendar-2014-5th-draft.pdf)
        * [2015](https://web.archive.org/web/20240225145904/https://www.bosvg.com/wp-content/uploads/2021/10/BOSVG-Bklet-Calendar-2015_21_11-3.pdf)
        * [2016](https://web.archive.org/web/20240225145903/https://www.bosvg.com/wp-content/uploads/2021/10/BOSVG-2016_pgs_sm_fin.pdf)
        * [2022](https://web.archive.org/web/20230426000952/https://www.bosvg.com/wp-content/uploads/2022/05/Tent-Calendar.pdf)
        * [2023](https://web.archive.org/web/20240225145904/https://www.bosvg.com/wp-content/uploads/2023/03/BOSVG-CALENDAR-TENT_F_REPRINT_TENT-EXCLUDED-FINAL.pdf)
        * [2019-2025](https://web.archive.org/web/20250214232128/https://pmoffice.gov.vc/pmoffice/index.php/public-holidays)
        * [2020 Carnival Monday](https://web.archive.org/web/20250607111242/https://www.stvincenttimes.com/august-3rd-and-4th-2020-declared-public-holidays-in-svg/)
        * [2025 National Spiritual Baptist Day](https://web.archive.org/web/20250513011200/https://www.gov.vc/images/pdf_documents/VINCENTIANS-PREPARE-FOR-MAY-21--SPIRITUAL-BAPTIST-LIBERATION-DAY-NATIONAL-HOLIDAY.pdf)
    """

    country = "VC"
    default_language = "en_VC"
    # %s (observed).
    observed_label = tr("%s (observed)")
    supported_languages = ("en_US", "en_VC")
    start_year = 1979

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=SaintVincentAndTheGrenadinesStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("New Year's Day")))

        # National Heroes' Day.
        self._add_observed(self._add_holiday_mar_14(tr("National Heroes' Day")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Easter Monday.
        self._add_easter_monday(tr("Easter Monday"))

        # National Workers' Day.
        self._add_observed(self._add_labor_day(tr("National Workers' Day")))

        if self._year >= 2025:
            # National Spiritual Baptist Day.
            self._add_holiday_may_21(tr("National Spiritual Baptist Day"))

        # Whit Monday.
        self._add_whit_monday(tr("Whit Monday"))

        # Carnival Monday.
        name = tr("Carnival Monday")
        carnival_monday_dates = {
            2013: (JUL, 8),
            2018: (JUL, 9),
            2019: (JUL, 8),
            2020: (AUG, 3),
            2021: (SEP, 6),
            2023: (JUL, 10),
            2024: (JUL, 8),
        }
        dt = (
            self._add_holiday(name, dt)
            if (dt := carnival_monday_dates.get(self._year))
            else self._add_holiday_1st_mon_of_jul(name)
        )

        # Carnival Tuesday.
        self._add_holiday(tr("Carnival Tuesday"), _timedelta(dt, +1))

        # Emancipation Day.
        self._add_observed(self._add_holiday_aug_1(tr("Emancipation Day")))

        # Independence Day.
        self._add_observed(self._add_holiday_oct_27(tr("Independence Day")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Christmas Day")), rule=SUN_TO_NEXT_TUE)

        # Boxing Day.
        self._add_observed(self._add_christmas_day_two(tr("Boxing Day")))


class VC(SaintVincentAndTheGrenadines):
    pass


class VCT(SaintVincentAndTheGrenadines):
    pass


class SaintVincentAndTheGrenadinesStaticHolidays:
    """Saint Vincent and the Grenadines special holidays.

    References:
        * [Statutory Rules and Orders 2021 No.1](https://web.archive.org/web/20250613051716/https://pmoffice.gov.vc/pmoffice/images/PDF/Gazettes/No_1_Proclamation_Delcaring_Friday_the_22nd_and_Monday_25th_day_of_January_2021_to_be_Public_Holidays_in_Saint_Vincent_and_the_Grenadines_19th_January_2021.pdf)
    """

    # Public Health Holiday.
    name = tr("Public Health Holiday")
    special_public_holidays = {
        2021: (
            (JAN, 22, name),
            (JAN, 25, name),
        ),
    }
