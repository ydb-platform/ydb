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

from holidays.calendars.gregorian import MAY, AUG
from holidays.calendars.julian import JULIAN_CALENDAR
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.holiday_base import HolidayBase


class Georgia(HolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Georgia holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Georgia_(country)>
        * [Labour Code of Georgia](https://web.archive.org/web/20250121212149/https://matsne.gov.ge/en/document/view/1155567?publication=24)
        * [Organic Law 4455-XVIმს-Xმპ](https://web.archive.org/web/20250421162538/https://matsne.gov.ge/ka/document/view/6283937?publication=0)
    """

    country = "GE"
    default_language = "ka"
    supported_languages = ("en_US", "ka", "uk")
    start_year = 1991

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self, JULIAN_CALENDAR)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, GeorgiaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("ახალი წელი")
        self._add_new_years_day(name)
        self._add_new_years_day_two(name)

        # Christmas Day.
        self._add_christmas_day(tr("ქრისტეშობა"))

        # Epiphany.
        self._add_epiphany_day(tr("ნათლისღება"))

        # Mother's Day.
        self._add_holiday_mar_3(tr("დედის დღე"))

        # International Women's Day.
        self._add_womens_day(tr("ქალთა საერთაშორისო დღე"))

        # National Unity Day.
        self._add_holiday_apr_9(tr("ეროვნული ერთიანობის დღე"))

        # Good Friday.
        self._add_good_friday(tr("წითელი პარასკევი"))

        # Holy Saturday.
        self._add_holy_saturday(tr("დიდი შაბათი"))

        # Easter Sunday.
        self._add_easter_sunday(tr("აღდგომა"))

        # Easter Monday.
        self._add_easter_monday(tr("შავი ორშაბათი"))

        # Day of Victory over Fascism.
        self._add_world_war_two_victory_day(tr("ფაშიზმზე გამარჯვების დღე"), is_western=False)

        # Saint Andrew's Day.
        self._add_holiday_may_12(tr("წმინდა ანდრია პირველწოდებულის დღე"))

        # Established by Organic Law 4455-XVIმს-Xმპ.
        if self._year >= 2025:
            # Day of Family Sanctity and Respect for Parents.
            self._add_holiday_may_17(tr("ოჯახის სიწმინდისა და მშობლების პატივისცემის დღე"))

        # Independence Day.
        self._add_holiday_may_26(tr("დამოუკიდებლობის დღე"))

        # Dormition of the Mother of God.
        self._add_assumption_of_mary_day(tr("მარიამობა"))

        # Holiday of Svetitskhovloba, Robe of Jesus.
        self._add_holiday_oct_14(tr("მცხეთობის"))

        # Saint George's Day.
        self._add_holiday_nov_23(tr("გიორგობა"))


class GE(Georgia):
    pass


class GEO(Georgia):
    pass


class GeorgiaStaticHolidays:
    """Georgia special holidays.

    References:
        * [Decree 167 of 15/05/2024](https://web.archive.org/web/20240620045954/https://www.matsne.gov.ge/ka/document/view/6173967?publication=0)
        * [Decree 381 of 25/08/2025](https://archive.org/details/matsne-6612161-0)
    """

    # Public Holiday.
    public_holiday = tr("უქმე დღე")

    special_public_holidays = {
        2024: (MAY, 17, public_holiday),
        2025: (AUG, 29, public_holiday),
    }
