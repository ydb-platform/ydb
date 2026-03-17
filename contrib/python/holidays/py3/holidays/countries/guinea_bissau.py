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

from holidays.calendars import _CustomIslamicHolidays
from holidays.groups import ChristianHolidays, InternationalHolidays, IslamicHolidays
from holidays.holiday_base import HolidayBase


class GuineaBissau(HolidayBase, ChristianHolidays, InternationalHolidays, IslamicHolidays):
    """Guinea-Bissau holidays.

    References:
        * [Ley núm. 7/2022, de 18 de julio, que aprueba el Código del Trabajo](https://archive.org/details/742984686-co-digo-de-trabalho-lei-no-7-2022)
    """

    country = "GW"
    default_language = "pt_GW"
    # %s (estimated).
    estimated_label = tr("%s (prevista)")
    supported_languages = ("en_US", "pt_GW")
    start_year = 2023

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=GuineaBissauIslamicHolidays, show_estimated=islamic_show_estimated
        )
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # New Year's Day.
        self._add_new_years_day(tr("Ano Novo"))

        # National Heroes' Day.
        self._add_holiday_jan_20(tr("Dia dos Heróis Nacionais"))

        # Day of the Beginning of the Armed Struggle.
        self._add_holiday_jan_23(tr("Dia do Início da Luta Armada"))

        # International Women's Day.
        self._add_womens_day(tr("Dia Internacional da Mulher"))

        # Easter Sunday.
        self._add_easter_sunday(tr("Páscoa"))

        # Worker's Day.
        self._add_labor_day(tr("Dia do Trabalhador"))

        # Pidjiguiti Day.
        self._add_holiday_aug_3(tr("Dia de Pidjiguiti"))

        # Independence Day.
        self._add_holiday_sep_24(tr("Dia da Independência"))

        # Christmas Day.
        self._add_christmas_day(tr("Dia de Natal"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Korité"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Tabaski"))


class GW(GuineaBissau):
    pass


class GNB(GuineaBissau):
    pass


class GuineaBissauIslamicHolidays(_CustomIslamicHolidays):
    # http://web.archive.org/web/20250811201914/https://www.timeanddate.com/holidays/guinea-bissau/eid-al-adha
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2023, 2025)

    # http://web.archive.org/web/20250811202225/https://www.timeanddate.com/holidays/guinea-bissau/eid-al-fitr
    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2023, 2025)
