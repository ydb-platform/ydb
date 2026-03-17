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

from holidays.calendars.gregorian import MAR, APR, MON, THU, _timedelta, _get_nth_weekday_of_month
from holidays.constants import HALF_DAY, OPTIONAL, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_ONLY,
    TUE_TO_NONE,
    SAT_TO_NONE,
    SUN_TO_NONE,
    ALL_TO_NEXT_MON,
)


class Switzerland(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Switzerland holidays.

    References:
        * <https://web.archive.org/web/20250201054902/https://www.bj.admin.ch/dam/bj/de/data/publiservice/service/zivilprozessrecht/kant-feiertage.pdf>
        * <https://web.archive.org/web/20251002085718/https://www.zuerich.com/en/inform-plan/useful-information-and-services/opening-hours-and-public-holidays/feiertage>
        * <https://web.archive.org/web/20251008032850/https://www.stadt-zuerich.ch/de/politik-und-verwaltung/arbeiten-bei-der-stadt/gut-zu-wissen/ferien-urlaub/feiertage-betriebsferientage-bft.html>
        * <https://web.archive.org/web/20230423124030/https://zuercher-bankenverband.ch/zbv/assets/uploads/2021/09/2022-Feiertagsuebersicht.pdf>
        * <https://web.archive.org/web/20251008045946/https://zuercher-bankenverband.ch/zbv/assets/uploads/2022/09/2023-Feiertagsuebersicht.pdf>
        * <https://web.archive.org/web/20251003094601/https://www.timeanddate.com/calendar/seasons.html?year=1900&n=268>
        * <https://de.wikipedia.org/wiki/Feiertage_in_der_Schweiz>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Switzerland>
    """

    country = "CH"
    default_language = "de"
    start_year = 1801
    subdivisions = (
        "AG",  # Aargau.
        "AI",  # Appenzell Innerrhoden.
        "AR",  # Appenzell Ausserrhoden.
        "BE",  # Bern (Berne).
        "BL",  # Basel-Landschaft.
        "BS",  # Basel-Stadt.
        "FR",  # Freiburg (Fribourg).
        "GE",  # Genève.
        "GL",  # Glarus.
        "GR",  # Graubünden (Grigioni, Grischun).
        "JU",  # Jura.
        "LU",  # Luzern.
        "NE",  # Neuchâtel.
        "NW",  # Nidwalden.
        "OW",  # Obwalden.
        "SG",  # Sankt Gallen.
        "SH",  # Schaffhausen.
        "SO",  # Solothurn.
        "SZ",  # Schwyz.
        "TG",  # Thurgau.
        "TI",  # Ticino.
        "UR",  # Uri.
        "VD",  # Vaud.
        "VS",  # Valais (Wallis).
        "ZG",  # Zug.
        "ZH",  # Zürich.
    )
    subdivisions_aliases = {
        "Aargau": "AG",
        "Appenzell Innerrhoden": "AI",
        "Appenzell Ausserrhoden": "AR",
        "Bern": "BE",
        "Berne": "BE",
        "Basel-Landschaft": "BL",
        "Basel-Stadt": "BS",
        "Freiburg": "FR",
        "Fribourg": "FR",
        "Genève": "GE",
        "Glarus": "GL",
        "Graubünden": "GR",
        "Grigioni": "GR",
        "Grischun": "GR",
        "Jura": "JU",
        "Luzern": "LU",
        "Neuchâtel": "NE",
        "Nidwalden": "NW",
        "Obwalden": "OW",
        "Sankt Gallen": "SG",
        "Schaffhausen": "SH",
        "Solothurn": "SO",
        "Schwyz": "SZ",
        "Thurgau": "TG",
        "Ticino": "TI",
        "Uri": "UR",
        "Vaud": "VD",
        "Valais": "VS",
        "Wallis": "VS",
        "Zug": "ZG",
        "Zürich": "ZH",
    }
    supported_categories = (HALF_DAY, OPTIONAL, PUBLIC)
    supported_languages = ("de", "en_US", "fr", "it", "th", "uk")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Neujahrstag"))

        # Ascension Day.
        self._add_ascension_thursday(tr("Auffahrt"))

        # National Day.
        self._add_holiday_aug_1(tr("Nationalfeiertag"))

        # Christmas Day.
        self._add_christmas_day(tr("Weihnachten"))

    def _populate_subdiv_ag_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_ar_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        self._add_observed(
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("Stephanstag")),
            rule=TUE_TO_NONE + SAT_TO_NONE,
        )

    def _populate_subdiv_ai_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        self._add_observed(
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("Stephanstag")),
            rule=TUE_TO_NONE + SAT_TO_NONE,
        )

    def _populate_subdiv_bl_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_bs_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_be_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_fr_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

    def _populate_subdiv_fr_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_ge_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Genevan Fast.
        self._add_holiday_4_days_past_1st_sun_of_sep(tr("Genfer Bettag"))

        # Restoration Day.
        self._add_holiday_dec_31(tr("Wiederherstellung der Republik"))

    def _populate_subdiv_gl_public_holidays(self):
        # Näfelser Fahrt (first Thursday in April but not in Holy Week)
        if self._year >= 1835:
            dt = _get_nth_weekday_of_month(1, THU, APR, self._year)
            self._add_holiday(
                # Battle of Naefels Victory Day.
                tr("Näfelser Fahrt"),
                _timedelta(dt, +7) if dt == _timedelta(self._easter_sunday, -3) else dt,
            )

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_gl_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

    def _populate_subdiv_gr_public_holidays(self):
        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_gr_optional_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

    def _populate_subdiv_ju_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Independence Day.
        self._add_holiday_jun_23(tr("Fest der Unabhängigkeit"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

    def _populate_subdiv_lu_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_ne_public_holidays(self):
        self._add_observed(
            # Saint Berchtold's Day.
            self._add_new_years_day_two(tr("Berchtoldstag")),
            rule=MON_ONLY,  # Jan 2 is public holiday only when it falls on Monday.
        )

        # Republic Day.
        self._add_holiday_mar_1(tr("Jahrestag der Ausrufung der Republik"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Saint Stephen's Day.
        self._add_observed(self._add_christmas_day_two(tr("Stephanstag")), rule=MON_ONLY)

    def _populate_subdiv_nw_public_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Josefstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

    def _populate_subdiv_nw_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_ow_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # Saint Nicholas of Flüe.
        self._add_holiday_sep_25(tr("Bruder Klaus"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

    def _populate_subdiv_ow_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_sg_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_sg_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

    def _populate_subdiv_sh_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_sh_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

    def _populate_subdiv_sz_public_holidays(self):
        # Epiphany.
        self._add_epiphany_day(tr("Heilige Drei Könige"))

        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Josefstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_so_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

    def _populate_subdiv_so_half_day_holidays(self):
        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

    def _populate_subdiv_so_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

    def _populate_subdiv_tg_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_ti_public_holidays(self):
        # Epiphany.
        self._add_epiphany_day(tr("Heilige Drei Könige"))

        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Josefstag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Saints Peter and Paul.
        self._add_saints_peter_and_paul_day(tr("Peter und Paul"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_ur_public_holidays(self):
        # Epiphany.
        self._add_epiphany_day(tr("Heilige Drei Könige"))

        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Josefstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

        self._add_observed(
            # Saint Stephen's Day.
            self._add_christmas_day_two(tr("Stephanstag")),
            rule=TUE_TO_NONE + SAT_TO_NONE,
        )

    def _populate_subdiv_vd_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Prayer Monday.
        self._add_holiday_1_day_past_3rd_sun_of_sep(tr("Bettagsmontag"))

    def _populate_subdiv_vs_public_holidays(self):
        # Saint Joseph's Day.
        self._add_saint_josephs_day(tr("Josefstag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

    def _populate_subdiv_vs_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_zg_public_holidays(self):
        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Fronleichnam"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Mariä Himmelfahrt"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Allerheiligen"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Mariä Empfängnis"))

    def _populate_subdiv_zg_optional_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_zh_public_holidays(self):
        # Saint Berchtold's Day.
        self._add_new_years_day_two(tr("Berchtoldstag"))

        # Good Friday.
        self._add_good_friday(tr("Karfreitag"))

        # Easter Monday.
        self._add_easter_monday(tr("Ostermontag"))

        # Labor Day.
        self._add_labor_day(tr("Tag der Arbeit"))

        # Whit Monday.
        self._add_whit_monday(tr("Pfingstmontag"))

        if self._year >= 1899:
            # Knabenschiessen.
            name = tr("Knabenschiessen")
            self._add_holiday_2nd_sun_of_sep(name)
            self._add_holiday_1_day_prior_2nd_sun_of_sep(name)

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Stephanstag"))

    def _populate_subdiv_zh_common(self):
        """Populate list of holidays observed by both `HALF_DAY` and `OPTIONAL` categories."""
        # Day before Good Friday.
        self._add_holy_thursday(tr("Vortag vor Karfreitag"))

        if self._year >= 1902:
            # Sechseläuten.
            name = tr("Sechseläuten")
            # Third Monday in April but not in Holy Week.
            if self._year >= 1952:
                dt = _get_nth_weekday_of_month(3, MON, APR, self._year)
                self._add_holiday(
                    name, _timedelta(dt, +7) if dt == _timedelta(self._easter_sunday, +1) else dt
                )
            # From 1902-1951 this was the First Monday following the Vernal Equinox.
            else:
                self._move_holiday_forced(
                    self._add_holiday(name, self._vernal_equinox_date), rule=ALL_TO_NEXT_MON
                )

        # Christmas Eve.
        self._add_christmas_eve(tr("Heiligabend"))

        # New Year's Eve.
        self._add_new_years_eve(tr("Vortag vor Neujahr"))

    def _populate_subdiv_zh_half_day_holidays(self):
        self._populate_subdiv_zh_common()

        if self._year >= 1899:
            # Knabenschiessen.
            self._add_holiday_1_day_past_2nd_sun_of_sep(tr("Knabenschiessen"))

        # Day before Ascension Day.
        self._add_holiday_38_days_past_easter(tr("Vortag vor Auffahrt"))

    def _populate_subdiv_zh_optional_holidays(self):
        self._populate_subdiv_zh_common()

        dec_25 = self._christmas_day

        # This is only granted if end-year bridge holidays <= 2.
        if self._is_tuesday(dec_25) or self._is_wednesday(dec_25) or self._is_thursday(dec_25):
            # Bridge Holiday for Ascension Day.
            self._add_holiday_40_days_past_easter(tr("Brückentag nach Auffahrt"))

        # This is only granted if end-year bridge holidays <= 3.
        if self._year >= 1899 and not self._is_weekend(dec_25):
            # Knabenschiessen.
            self._add_holiday_1_day_past_2nd_sun_of_sep(tr("Knabenschiessen"))

        # Bridge Holiday.
        name = tr("Brückentag")
        self._add_observed(self._add_holiday_dec_27(name), rule=SAT_TO_NONE + SUN_TO_NONE)
        self._add_observed(self._add_holiday_dec_28(name), rule=SAT_TO_NONE + SUN_TO_NONE)
        self._add_observed(self._add_holiday_dec_29(name), rule=SAT_TO_NONE + SUN_TO_NONE)
        self._add_observed(self._add_holiday_dec_30(name), rule=SAT_TO_NONE + SUN_TO_NONE)

    @property
    def _vernal_equinox_date(self) -> tuple[int, int]:
        """Return the Vernal Equinox date for Zurich (1902-1951)."""
        return MAR, 20 if (
            (self._year >= 1916 and self._year % 4 == 0) or self._year == 1949
        ) else 21


class CH(Switzerland):
    pass


class CHE(Switzerland):
    pass
