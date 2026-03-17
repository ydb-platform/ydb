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

from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import ObservedHolidayBase, SAT_TO_PREV_FRI, SUN_TO_NEXT_MON


class SaoTomeAndPrincipe(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Sao Tome and Principe holidays.

    References:
        * <https://web.archive.org/web/20190319201800/http://www.mnec.gov.st/index.php/o-pais?showall=1>
        * <https://web.archive.org/web/20241102204908/https://visitsaotomeprincipe.st/pt/informacoes-uteis/feriados>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_São_Tomé_and_Príncipe>
        * <https://web.archive.org/web/20250320223941/https://www.timeanddate.com/holidays/sao-tome-and-principe/>
    """

    country = "ST"
    default_language = "pt_ST"
    # %s (observed).
    observed_label = tr("%s (observado)")
    start_year = 2014
    subdivisions = (
        "01",  # Água Grande.
        "02",  # Cantagalo.
        "03",  # Caué.
        "04",  # Lembá.
        "05",  # Lobata.
        "06",  # Mé-Zóchi.
        "P",  # Príncipe.
    )
    subdivisions_aliases = {
        # Districts.
        "Água Grande": "01",
        "Cantagalo": "02",
        "Caué": "03",
        "Lembá": "04",
        "Lobata": "05",
        "Mé-Zóchi": "06",
        # Autonomous Region.
        "Príncipe": "P",
    }
    supported_languages = ("en_US", "pt_ST")

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON + SAT_TO_PREV_FRI)
        # Holidays are observed on the next Monday if on Sunday, or previous Friday if on Saturday.
        # Based on common government practices since at least 2020.
        kwargs.setdefault("observed_since", 2020)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Ano Novo")
        self._add_observed(self._add_new_years_day(name))
        self._add_observed(self._next_year_new_years_day, name=name, rule=SAT_TO_PREV_FRI)

        # Day of King Amador.
        self._add_observed(self._add_holiday_jan_4(tr("Dia do Rei Amador")))

        # Martyrs' Day.
        self._add_observed(self._add_holiday_feb_3(tr("Dia dos Mártires")))

        # Worker's Day.
        self._add_observed(self._add_labor_day(tr("Dia do Trabalhador")))

        # Independence Day.
        self._add_observed(self._add_holiday_jul_12(tr("Dia da Independência")))

        # Armed Forces Day.
        self._add_observed(self._add_holiday_sep_6(tr("Dia das Forças Armadas")))

        # Agricultural Reform Day.
        self._add_observed(self._add_holiday_sep_30(tr("Dia da Reforma Agrária")))

        if self._year >= 2019:
            # São Tomé Day.
            self._add_observed(self._add_holiday_dec_21(tr("Dia de São Tomé")))

        # Christmas Day.
        self._add_observed(self._add_christmas_day(tr("Natal")))

    def _populate_subdiv_p_public_holidays(self):
        # Discovery of Príncipe Island.
        self._add_observed(self._add_holiday_jan_17(tr("Descobrimento da Ilha do Príncipe")))

        # Autonomy Day.
        self._add_observed(self._add_holiday_apr_29(tr("Dia da Autonomia do Príncipe")))

        # São Lourenço Day.
        self._add_observed(self._add_holiday_aug_15(tr("Dia de São Lourenço")))


class ST(SaoTomeAndPrincipe):
    pass


class STP(SaoTomeAndPrincipe):
    pass
