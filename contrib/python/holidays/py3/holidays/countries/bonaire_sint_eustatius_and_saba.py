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

from holidays.calendars.gregorian import MAY
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SUN_TO_PREV_SAT


class BonaireSintEustatiusAndSaba(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Bonaire, Sint Eustatius and Saba holidays.

    References:
        * [Public Holidays 2025 (English)](https://web.archive.org/web/20250620050728/https://english.rijksdienstcn.com/social-affairs-work/employment/official-holidays)
        * [Public Holidays 2025 (Dutch)](https://web.archive.org/web/20250323122427/https://www.rijksdienstcn.com/sociale-zaken-werk/arbeid/officiele-feestdagen)
        * [Public Holidays 2025 (Papiamento)](https://web.archive.org/web/20250323125431/https://papiamentu.rijksdienstcn.com/asuntunan-sosial-i-labor/labor/dia-di-fiesta-ofisial)
        * [Arbeidsregeling 2000](https://web.archive.org/web/20250625203706/https://lokaleregelgeving.overheid.nl/CVDR10375/1)
        * [Dia di Rincon 2025](https://web.archive.org/web/20250515183744/https://sunwisebonaire.com/blog/dia-di-rincon-2025-on-bonaire/)
    """

    country = "BQ"
    default_language = "nl"
    # Became special municipalities of the Netherlands on October 10th, 2010.
    start_year = 2011
    subdivisions = (
        "BON",  # Bonaire.
        "SAB",  # Saba.
        "STA",  # Sint Eustatius.
    )
    subdivisions_aliases = {
        "Bonaire": "BON",
        "Saba": "SAB",
        "Sint Eustatius": "STA",
    }
    supported_languages = ("en_BQ", "en_US", "nl", "pap_BQ")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=BonaireSintEustatiusAndSabaStaticHolidays)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Nieuwjaarsdag"))

        # Good Friday.
        self._add_good_friday(tr("Goede Vrijdag"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Eerste Paasdag"))

        # Easter Monday.
        self._add_easter_monday(tr("Tweede Paasdag"))

        self._move_holiday(
            # King's Day.
            self._add_holiday_apr_27(tr("Koningsdag"))
            if self._year >= 2014
            # Queen's Day.
            else self._add_holiday_apr_30(tr("Koninginnedag")),
            rule=SUN_TO_PREV_SAT,
        )

        # Labor Day.
        self._add_labor_day(tr("Dag van de Arbeid"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Hemelvaartsdag"))

        # Whit Sunday.
        self._add_whit_sunday(tr("Eerste Pinksterdag"))

        # Christmas Day.
        self._add_christmas_day(tr("Eerste Kerstdag"))

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Tweede Kerstdag"))

    def _populate_subdiv_bon_public_holidays(self):
        if self._year >= 2020:
            # Rincon Day.
            self._add_holiday_apr_30(tr("Rincondag"))

        # Bonaire Day.
        self._add_holiday_sep_6(tr("Bonairedag"))

    def _populate_subdiv_sab_public_holidays(self):
        # Carnival Monday.
        self._add_holiday_last_mon_of_jul(tr("Dag na de carnavalsoptocht"))

        # Saba Day.
        self._add_holiday_1st_fri_of_dec(tr("Sabadag"))

    def _populate_subdiv_sta_public_holidays(self):
        if self._year >= 2022:
            # Emancipation Day.
            self._add_holiday_jul_1(tr("Emancipatiedag"))

        # Statia Day.
        self._add_holiday_nov_16(tr("Statiadag"))


class BQ(BonaireSintEustatiusAndSaba):
    pass


class BES(BonaireSintEustatiusAndSaba):
    pass


class BonaireSintEustatiusAndSabaStaticHolidays(StaticHolidays):
    """Bonaire, Sint Eustatius and Saba special holidays.

    References:
        * [Bonaire May 2nd, 2025](https://web.archive.org/web/20250620052258/https://bonairegov.com/nieuwsoverzicht/artikel/vrijdag-2-mei-2025-aangewezen-als-vrije-dag-voor-ambtenaren)
    """

    special_bon_public_holidays = {
        # Bridge Holiday.
        2025: (MAY, 2, tr("Brugdag")),
    }
