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

from holidays.calendars.gregorian import JAN
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_SUN_TO_NEXT_MON


class EquatorialGuinea(
    ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays
):
    """Equatorial Guinea holidays.

    References:
        * [Decree 9/2007](https://web.archive.org/web/20250518185011/https://boe.gob.gq/files/Decreto%20de%20Fijación%20de%20los%20Días%20Feriados%20en%20Guinea%20Ecuatorial.pdf)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Equatorial_Guinea>
        * <https://web.archive.org/web/20250503042253/https://www.timeanddate.com/holidays/guineaecuatorial/>
        * <https://web.archive.org/web/20250503042610/https://gq.usembassy.gov/holiday-calendar/>
        * [2015](https://web.archive.org/web/20250421105147/https://www.guineainfomarket.com/gobierno/2015/01/01/calendario-laboral-de-guinea-ecuatorial-2015/)
        * [2016](https://web.archive.org/web/20250212174334/https://www.guineainfomarket.com/libros/2015/12/06/calendario-laboral-de-guinea-ecuatorial-2016/)
        * [2018](https://web.archive.org/web/20241005004303/https://www.guineainfomarket.com/economia/2017/12/29/calendario-laboral-guinea-ecuatorial-2018/)
        * [2019](https://web.archive.org/web/20220526022213/https://www.guineainfomarket.com/economia/2019/01/01/calendario-laboral-guinea-ecuatorial-2019/)
        * [2020](https://web.archive.org/web/20250503044753/https://www.guineainfomarket.com/cultura/2020/01/01/calendario-laboral-de-guinea-ecuatorial-2020/)
        * [2021](https://web.archive.org/web/20250503044727/https://www.guineainfomarket.com/cultura/2021/01/29/calendario-laboral-de-guinea-ecuatorial-2021/)
        * [2022](https://web.archive.org/web/20250503044746/https://www.guineainfomarket.com/cultura/2021/01/29/calendario-laboral-de-guinea-ecuatorial-2021-2/)
        * [2024](https://web.archive.org/web/20250504175804/https://www.guineainfomarket.com/business/2024/03/29/calendario-laboral-de-guinea-ecuatorial-2024/)
    """

    country = "GQ"
    default_language = "es"
    supported_languages = ("en_US", "es")
    # %s observed.
    observed_label = tr("%s (observado)")
    # Decree 9/2007.
    start_year = 2007
    subdivisions = (
        "AN",  # Annobón (Annobon).
        "BN",  # Bioko Norte (North Bioko).
        "BS",  # Bioko Sur (South Bioko).
        "CS",  # Centro Sur (South Center).
        "DJ",  # Djibloho.
        "KN",  # Kié-Ntem (Kie-Ntem).
        "LI",  # Litoral (Coast).
        "WN",  # Wele-Nzas.
    )
    subdivisions_aliases = {
        "Annobón": "AN",
        "Annobon": "AN",
        "Bioko Norte": "BN",
        "North Bioko": "BN",
        "Bioko Sur": "BS",
        "South Bioko": "BS",
        "Centro Sur": "CS",
        "South Center": "CS",
        "Djibloho": "DJ",
        "Kié-Ntem": "KN",
        "Kie-Ntem": "KN",
        "Litoral": "LI",
        "Coast": "LI",
        "Wele-Nzas": "WN",
    }

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=EquatorialGuineaStaticHolidays)
        kwargs.setdefault("observed_rule", SAT_SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_observed(self._add_new_years_day(tr("Año Nuevo")))

        # International Women's Day.
        self._add_womens_day(tr("Día Internacional de la Mujer"))

        # Maundy Thursday.
        self._add_holy_thursday(tr("Jueves Santo"))

        # Good Friday.
        self._add_good_friday(tr("Viernes Santo"))

        # International Labor Day.
        self._add_observed(self._add_labor_day(tr("Día Internacional del Trabajo")))

        # African Liberation Day.
        self._add_africa_day(tr("Día de la liberación Africana"))

        self._add_observed(
            # President's Day.
            self._add_holiday_jun_5(tr("Natalicio de Su Excelencia el Presidente de la República"))
        )

        # Corpus Christi.
        self._add_corpus_christi_day(tr("Corpus Christi"))

        # Armed Forces Day.
        self._add_observed(self._add_holiday_aug_3(tr("Día de las Fuerzas Armadas")))

        # Constitution Day.
        self._add_observed(self._add_holiday_aug_15(tr("Día de la Constitución")))

        # Independence Day.
        self._add_observed(self._add_holiday_oct_12(tr("Día de la Independencia Nacional")))

        self._add_observed(
            self._add_immaculate_conception_day(
                # Immaculate Conception.
                tr("Festividad de la Inmaculada Concepción de María")
            )
        )

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Día de Navidad")))

    def _populate_subdiv_an_public_holidays(self):
        # Patron Saint Festival of Annobón.
        self._add_holiday_jun_13(tr("Fiesta Patronal de Annobón"))


class GQ(EquatorialGuinea):
    pass


class GNQ(EquatorialGuinea):
    pass


class EquatorialGuineaStaticHolidays:
    """Equatorial Guinea special holidays.

    References:
        * [AFCON Victory Holiday](https://web.archive.org/web/20240124021813/https://www.monitor.co.ug/uganda/sports/soccer/equatorial-guinea-president-awards-team-1-million-for-afcon-victory-4500644)
    """

    special_public_holidays = {
        # AFCON Victory Against Ivory Coast.
        2024: (JAN, 23, tr("Victoria de la AFCON contra Costa de Marfil")),
    }
