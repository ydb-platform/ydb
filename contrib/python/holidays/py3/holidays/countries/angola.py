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

from holidays.calendars.gregorian import AUG, SEP
from holidays.groups import ChristianHolidays, InternationalHolidays, StaticHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    TUE_TO_PREV_MON,
    THU_TO_NEXT_FRI,
    SUN_TO_NEXT_MON,
)


class Angola(ObservedHolidayBase, ChristianHolidays, InternationalHolidays, StaticHolidays):
    """Angola holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Angola>
        * <https://web.archive.org/web/20240617085329/https://www.siac.gv.ao/downloads/181029-Lei-Feriados.pdf>
        * [Decree #5/75](https://web.archive.org/web/20230719141732/https://www.lexlink.eu/FileGet.aspx?FileId=3023486)
        * [Decree #92/80](https://web.archive.org/web/20250427180154/https://www.lexlink.eu/FileGet.aspx?FileId=3023473)
        * [Decree #7/92](https://web.archive.org/web/20230719141946/https://www.lexlink.eu/FileGet.aspx?FileId=3023485)
        * [Law #16/96](https://web.archive.org/web/20230719141332/https://www.lexlink.eu/FileGet.aspx?FileId=3037036)
        * [Law #1/01](https://web.archive.org/web/20230719142311/https://www.lexlink.eu/FileGet.aspx?FileId=3029035)
        * [Law #7/03](https://web.archive.org/web/20230719142347/https://www.lexlink.eu/FileGet.aspx?FileId=3002131)
        * [Law #10/11](https://web.archive.org/web/20250427180133/https://equadros.gov.ao/documents/40468/0/lei_10_11-1+(1).pdf)
        * [Law #11/18](https://web.archive.org/web/20250427175641/https://equadros.gov.ao/documents/40468/0/Lei_no_11-18+(1).pdf)
        * <https://web.archive.org/web/20250211025045/https://www.officeholidays.com/countries/angola>
        * <https://web.archive.org/web/20250323005924/https://www.timeanddate.com/holidays/angola/>
    """

    country = "AO"
    default_language = "pt_AO"
    supported_languages = ("en_US", "pt_AO", "uk")
    # %s (observed).
    observed_label = tr("%s (ponte)")
    # Decree #5/75.
    start_year = 1975

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        StaticHolidays.__init__(self, cls=AngolaStaticHolidays)
        kwargs.setdefault("observed_rule", TUE_TO_PREV_MON + THU_TO_NEXT_FRI)
        super().__init__(*args, **kwargs)

    def _is_observed(self, dt: date) -> bool:
        # As per Law # 16/96, from 1996/9/27, when public holiday falls on Sunday,
        # it rolls over to the following Monday.
        return dt >= date(1996, SEP, 27)

    def _add_observed(self, dt: date, **kwargs) -> tuple[bool, date | None]:
        # As per Law # #11/18, from 2018/9/10, when public holiday falls on Tuesday or Thursday,
        # the Monday or Friday is also a holiday.
        kwargs.setdefault(
            "rule", SUN_TO_NEXT_MON if dt < date(2018, SEP, 10) else self._observed_rule
        )
        return super()._add_observed(dt, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        name = tr("Dia do Ano Novo")
        dt = self._add_new_years_day(name)
        if self._year <= 2011 or self._year >= 2018:
            self._add_observed(dt)
            self._add_observed(self._next_year_new_years_day, name=name)

        # Law #16/96.
        if 1997 <= self._year <= 2011:
            self._add_observed(
                # Martyrs of Colonial Repression Day.
                self._add_holiday_jan_4(tr("Dia dos Mártires da Repressão Colonial"))
            )

        self._add_observed(
            self._add_holiday_feb_4(
                # Beginning of the Armed Struggle for National Liberation Day.
                tr("Dia do Início da Luta Armada de Libertação Nacional")
                if self._year >= 2012
                # Beginning of the Armed Struggle Day.
                else tr("Dia do Início da Luta Armada")
            )
        )

        # Law #16/96.
        if self._year >= 1997:
            # Carnival Day.
            self._add_observed(self._add_carnival_tuesday(tr("Dia do Carnaval")))

            # International Women's Day.
            self._add_observed(self._add_womens_day(tr("Dia Internacional da Mulher")))

        # Law #11/18.
        if self._year >= 2019:
            # Southern Africa Liberation Day.
            self._add_observed(self._add_holiday_mar_23(tr("Dia da Libertação da África Austral")))

        # Law #7/03.
        if self._year >= 2003:
            # Peace and National Reconciliation Day.
            self._add_observed(self._add_holiday_apr_4(tr("Dia da Paz e Reconciliação Nacional")))

        # Law #16/96.
        if self._year >= 1997:
            # Good Friday.
            self._add_good_friday(tr("Sexta-Feira Santa"))

        # International Worker's Day.
        self._add_observed(self._add_labor_day(tr("Dia Internacional do Trabalhador")))

        # Law #1/01.
        if 2001 <= self._year <= 2010:
            # Africa Day.
            self._add_observed(self._add_africa_day(tr("Dia da África")))

        # Law #16/96.
        if 1997 <= self._year <= 2010:
            # International Children's Day.
            self._add_observed(self._add_childrens_day(tr("Dia Internacional da Criança")))

        # Decree #92/80.
        if self._year >= 1980:
            self._add_observed(
                # National Heroes' Day.
                self._add_holiday_sep_17(tr("Dia do Fundador da Nação e do Herói Nacional"))
            )

        # All Souls' Day.
        dt = self._add_all_souls_day(tr("Dia dos Finados"))
        if self._year <= 2010 or self._year >= 2018:
            self._add_observed(dt)

        self._add_observed(
            self._add_holiday_nov_11(
                # National Independence Day.
                tr("Dia da Independência Nacional")
                if self._year >= 1996
                # Independence Day.
                else tr("Dia da Independência")
            )
        )

        # Decree # 7/92.
        if self._year <= 1991:
            # Date of Founding of MPLA - Labor Party.
            self._add_holiday_dec_10(tr("Data da Fundacao do MPLA - Partido do Trabalho"))

        dt = self._add_christmas_day(
            # Christmas and Family Day.
            tr("Dia de Natal e da Família")
            if self._year >= 2011
            else (
                tr("Dia do Natal")  # Christmas Day.
                if self._year >= 1996
                else tr("Dia da Família")  # Family Day.
            )
        )
        if self._year <= 2010 or self._year >= 2018:
            self._add_observed(dt)


class AO(Angola):
    pass


class AGO(Angola):
    pass


class AngolaStaticHolidays:
    special_public_holidays = {
        # General Election Day.
        2017: (AUG, 23, tr("Dia de eleições gerais")),
    }
