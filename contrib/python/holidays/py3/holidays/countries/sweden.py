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

from holidays.calendars.gregorian import (
    MAR,
    SUN,
    _get_all_sundays,
    _get_nth_weekday_from,
    _timedelta,
)
from holidays.constants import BANK, DE_FACTO, OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Sweden(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Sweden holidays.

    References:
        * <https://sv.wikipedia.org/wiki/Helgdagar_i_Sverige>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Sweden>
        * [Act 1952:48](https://web.archive.org/web/20250518071409/https://blogs.loc.gov/law/2023/06/midsommar-becomes-weekend-holiday-in-sweden-in-1953-pic-of-the-week/)
        * [Act 1989:253](https://web.archive.org/web/20250414065223/https://www.riksdagen.se/sv/dokument-lagar/dokument/svensk-forfattningssamling/lag-1989253-om-allmanna-helgdagar_sfs-1989-253)
        * <https://sv.wikipedia.org/wiki/Första_maj>
        * <https://sv.wikipedia.org/wiki/Sveriges_nationaldag>
        * <https://sv.wikipedia.org/wiki/Midsommarafton>
        * [Bank Holidays 2025](https://web.archive.org/web/20250811112642/https://www.riksbank.se/sv/press-och-publicerat/kalender/helgdagar-2025/)
        * [Swedish Annual Leave Law (SFS 1977:480)](https://web.archive.org/web/20260106114757/https://www.riksdagen.se/sv/dokument-och-lagar/dokument/svensk-forfattningssamling/semesterlag-1977480_sfs-1977-480/)

    In Sweden, ALL sundays are considered a holiday.
    Initialize this class with `include_sundays=False` to not include sundays as a holiday.

    Supported holiday categories:

    - PUBLIC: Official public holidays with general time off
    - DE_FACTO: Holidays treated equivalently to public holidays by law
    - BANK: Banking institution holidays
    - OPTIONAL: Optional or cultural observances

    The DE_FACTO category includes:
        - Midsommarafton (Midsummer Eve)
        - Julafton (Christmas Eve)
        - Nyårsafton (New Year's Eve)

    According to Swedish Annual Leave Law (SFS 1977:480, Section 7): "Med söndag
    jämställs allmän helgdag samt midsommarafton, julafton och nyårsafton"
    (Sundays are equivalent to public holidays as well as Midsummer's Eve,
    Christmas Eve, and New Year's Eve).

    These holidays are not official public holidays but must be treated as
    non-working days. For accurate `is_working_day()` calculations, use:
        `Sweden(categories=(PUBLIC, DE_FACTO))`
    """

    # %s (from 2pm).
    begin_time_label = tr("%s (från kl. 14.00)")
    country = "SE"
    default_language = "sv"
    # Act 1952:48.
    start_year = 1953
    supported_categories = (BANK, DE_FACTO, OPTIONAL, PUBLIC)
    supported_languages = ("en_US", "sv", "th", "uk")

    def __init__(self, *args, include_sundays: bool = True, **kwargs):
        """
        Args:
            include_sundays:
                Whether to consider sundays as a holiday (which they are in Sweden)
        """
        self.include_sundays = include_sundays
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nyårsdagen"))

        # Epiphany.
        self._add_epiphany_day(tr("Trettondedag jul"))

        if self._year <= 1989:
            # According to Act 1952:48, Feast of the Annunciation is
            # "on the Sunday between March 22 and 28 or, if this Sunday is Palm Sunday
            # or Easter Sunday, on the Sunday before Palm Sunday".
            dt = _get_nth_weekday_from(1, SUN, date(self._year, MAR, 22))
            self._add_holiday(
                # Feast of the Annunciation.
                tr("Marie bebådelsedag"),
                _timedelta(self._easter_sunday, -14)
                if dt == _timedelta(self._easter_sunday, -7) or dt == self._easter_sunday
                else dt,
            )

        # Good Friday.
        self._add_good_friday(tr("Långfredagen"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Påskdagen"))

        # Easter Monday.
        self._add_easter_monday(tr("Annandag påsk"))

        # May Day.
        self._add_labor_day(tr("Första maj"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Kristi himmelsfärdsdag"))

        if self._year >= 2005:
            # National Day.
            self._add_holiday_jun_6(tr("Nationaldagen"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Pingstdagen"))

        if self._year <= 2004:
            # Whit Monday.
            self._add_whit_monday(tr("Annandag pingst"))

        # Midsummer Day.
        self._add_holiday_1st_sat_from_jun_20(tr("Midsommardagen"))

        # All Saints' Day.
        self._add_holiday_1st_sat_from_oct_31(tr("Alla helgons dag"))

        # Christmas Day.
        self._add_christmas_day(tr("Juldagen"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Annandag jul"))

        # Optionally add all Sundays of the year.
        if self.include_sundays:
            for dt in _get_all_sundays(self._year):
                # Sunday.
                self._add_holiday(tr("Söndag"), dt)

    def _populate_de_facto_holidays(self):
        # Midsummer Eve.
        self._add_holiday_1st_fri_from_jun_19(tr("Midsommarafton"))

        # Christmas Eve.
        self._add_christmas_eve(tr("Julafton"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Nyårsafton"))

    def _populate_common(self):
        """Populate holidays that are both optional and bank holidays."""

        # Twelfth Night.
        self._add_holiday_jan_5(self.tr(self.begin_time_label) % self.tr("Trettondagsafton"))

        # Maundy Thursday.
        self._add_holy_thursday(self.tr(self.begin_time_label) % self.tr("Skärtorsdagen"))

        # Walpurgis Night.
        self._add_holiday_apr_30(self.tr(self.begin_time_label) % self.tr("Valborgsmässoafton"))

        self._add_holiday_1st_fri_from_oct_30(
            # All Saints' Eve.
            self.tr(self.begin_time_label) % self.tr("Allahelgonsafton")
        )

    def _populate_bank_holidays(self):
        self._populate_common()
        self._populate_de_facto_holidays()

        self._add_holiday_38_days_past_easter(
            # Day before Ascension Day.
            self.tr(self.begin_time_label) % self.tr("Dag före Kristi himmelsfärdsdag")
        )

        self._add_holiday_1st_thu_from_jun_18(
            # Day before Midsummer Eve.
            self.tr(self.begin_time_label) % self.tr("Dag före Midsommarafton")
        )

        self._add_holiday_47_days_past_easter(
            # Day before Whitsun Eve.
            self.tr(self.begin_time_label) % self.tr("Dag före Pingstafton")
        )

        # Day before Christmas Eve.
        self._add_holiday_dec_23(self.tr(self.begin_time_label) % self.tr("Dag före Julafton"))

        # Day before New Year's Eve.
        self._add_holiday_dec_30(self.tr(self.begin_time_label) % self.tr("Dag före Nyårsafton"))

    def _populate_optional_holidays(self):
        self._populate_common()

        # Holy Saturday.
        self._add_holy_saturday(tr("Påskafton"))

        # Days between a holiday and a weekend are in Swedish called "klämdagar" (squeeze days).
        # There is one permanent "klämdag" every year - Friday after Ascension Day.

        # Squeeze day.
        self._add_holiday_40_days_past_easter(tr("Klämdag"))

        # Whitsun Eve.
        self._add_holiday_48_days_past_easter(tr("Pingstafton"))


class SE(Sweden):
    pass


class SWE(Sweden):
    pass
