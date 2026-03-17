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

from holidays.constants import HALF_DAY, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SUN_TO_PREV_SAT,
    SUN_TO_NEXT_MON,
)


class Curacao(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Curaçao holidays.

    References:
        * [Arbeidsregeling 2000](https://web.archive.org/web/20250625071823/https://lokaleregelgeving.overheid.nl/CVDR10375)
        * [Landbesluit no. 22/2060](https://web.archive.org/web/20240629135453/https://gobiernu.cw/wp-content/uploads/2022/12/123.-GT-Lb.Arebeidsregeling-2000-4.pdf)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Curaçao>
        * <https://web.archive.org/web/20240723012531/https://loketdigital.gobiernu.cw/contact/officiele-vrije-dagen-op-curacao-nationale-feestdagen>
        * <https://web.archive.org/web/20250422122824/https://www.meetcuracao.com/2-juli-dia-di-bandera-y-himno-op-curacao/>
    """

    country = "CW"
    default_language = "pap_CW"
    supported_categories = (HALF_DAY, PUBLIC)
    supported_languages = ("en_US", "nl", "pap_CW", "uk")
    # The Netherlands Antilles was established on December 15th, 1954.
    start_year = 1955

    def __init__(self, *args, **kwargs):
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_MON)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Aña Nobo.
        # Status: In-Use.

        # New Year's Day.
        self._add_new_years_day(tr("Aña Nobo"))

        # Dialuna despues di Carnaval Grandi.
        # Status: In-Use.
        # Started in 1947.

        # Carnival Monday.
        self._add_ash_monday(tr("Dialuna despues di Carnaval Grandi"))

        # Bièrnèsantu.
        # Status: In-Use.

        # Good Friday.
        self._add_good_friday(tr("Bièrnèsantu"))

        # Pasku di Resurekshon.
        # Status: In-Use

        # Easter Sunday.
        self._add_easter_sunday(tr("Pasku di Resurekshon"))

        # Di dos dia di Pasku di Resurekshon.
        # Status: In-Use.

        # Easter Monday.
        self._add_easter_monday(tr("Di dos dia di Pasku di Resurekshon"))

        # Dia di Obrero.
        # Status: In-Use.
        # If fall on Sunday, then this will be move to next working day.
        # This is placed here before King's/Queen's Day for _move_holiday logic.

        self._move_holiday(
            # Labor Day.
            self._add_labor_day(tr("Dia di Obrero")),
            rule=SUN_TO_NEXT_MON if self._year >= 1980 else MON_TO_NEXT_TUE + SUN_TO_NEXT_MON,
        )

        # Dia di la Reina/Dia di Rey.
        # Status: In-Use.
        # Started under Queen Wilhelmina in 1891.
        # Queen Beatrix kept Queen Juliana's Birthday after her coronation.
        # Switched to Dia di Rey in 2014 for King Willem-Alexander.

        self._move_holiday(
            # King's Day.
            self._add_holiday_apr_27(tr("Dia di Rey"))
            if self._year >= 2014
            # Queen's Day.
            else self._add_holiday_apr_30(tr("Dia di la Reina")),
            rule=SUN_TO_PREV_SAT if self._year >= 1980 else SUN_TO_NEXT_MON,
        )

        # Dia di Asenshon.
        # Status: In-Use.

        # Ascension Day.
        self._add_ascension_thursday(tr("Dia di Asenshon"))

        # Domingo di Pentekòstès.
        # Status: Removed
        # Exists in Labor Regulation 2000.
        # Presumed to be removed from 2010 onwards.

        if self._year <= 2009:
            # Whit Sunday.
            self._add_whit_sunday(tr("Domingo di Pentekòstès"))

        # Dia di Himno i Bandera.
        # Status: In-Use.
        # Starts in 1984.

        if self._year >= 1984:
            # National Anthem and Flag Day.
            self._add_holiday_jul_2(tr("Dia di Himno i Bandera"))

        # Dia di Pais Kòrsou / Dia di autonomia.
        # Status: In-Use.
        # Starts in 2010.

        if self._year >= 2010:
            # Curaçao Day.
            self._add_holiday_oct_10(tr("Dia di Pais Kòrsou"))

        # Kingdom Day.
        # Status: Removed.
        # Added on June 3rd, 2008 via P.B. 2008 no. 50.
        # Presumed to have been removed in 2010 after Curacao gain its Autonomy.

        if 2008 <= self._year <= 2009:
            # Kingdom Day.
            self._add_holiday_dec_15(tr("Dia di Reino"))

        # Pasku di Nasementu.
        # Status: In-Use.

        # Christmas Day.
        self._add_christmas_day(tr("Pasku di Nasementu"))

        # Di dos dia di Pasku di Nasementu.
        # Status: In-Use.

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Di dos dia di Pasku di Nasementu"))

    def _populate_half_day_holidays(self):
        # New Year's Eve.
        # Status: In-Use.
        # Presumed to have been added after 2010.

        if self._year >= 2010:
            # New Year's Eve.
            self._add_new_years_eve(tr("Vispu di Aña Nobo"))


class CW(Curacao):
    pass


class CUW(Curacao):
    pass
