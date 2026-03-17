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

from holidays.constants import GOVERNMENT, PUBLIC
from holidays.groups import ChristianHolidays, InternationalHolidays
from holidays.holiday_base import HolidayBase


class Andorra(HolidayBase, ChristianHolidays, InternationalHolidays):
    """Andorra holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Andorra>
        * [Andorra's Constitution](https://web.archive.org/web/20250506145838/https://www.bopa.ad/Legislacio/Detall?doc=7586)
        * [Government holidays source](https://web.archive.org/web/20241005040546/https://seu.consellgeneral.ad/calendariPublic/show)
        * [2025](https://web.archive.org/web/20250506142149/https://www.bopa.ad/Documents/Detall?doc=GD_2024_10_24_14_36_16)

    Subdivisions holidays:
        * Canillo:
            * [2020](https://web.archive.org/web/20250506143618/https://www.bopa.ad/Documents/Detall?doc=QCH20191209_09_01_14)
            * [2021](https://web.archive.org/web/20250506143707/https://www.bopa.ad/Documents/Detall?doc=QCH20201130_11_52_35)
            * [2022](https://web.archive.org/web/20250506143404/https://www.bopa.ad/Documents/Detall?doc=QCD20211126_12_16_57)
            * [2023](https://web.archive.org/web/20250506143725/https://www.bopa.ad/Documents/Detall?doc=QCH20221010_11_26_39)
            * [2024](https://web.archive.org/web/20250506143740/https://www.bopa.ad/Documents/Detall?doc=QCH_2023_12_06_16_16_52)
            * [2025](https://web.archive.org/web/20250506144017/https://www.bopa.ad/Documents/Detall?doc=QCH_2024_10_10_11_42_15)
        * Encamp:
            * [2024](https://web.archive.org/web/20241201165207/https://www.bopa.ad/Documents/Detall?doc=QEH_2023_12_15_13_03_32)
            * [2025](https://web.archive.org/web/20241212121414/https://www.bopa.ad/Documents/Detall?doc=QEH_2024_11_27_10_29_49)
        * La Massana:
            * [2020](https://web.archive.org/web/20250506144157/https://www.bopa.ad/Documents/Detall?doc=QMH20191209_09_08_10)
            * [2021](https://web.archive.org/web/20250506144241/https://www.bopa.ad/Documents/Detall?doc=QMH20201218_11_52_59)
            * [2022](https://web.archive.org/web/20250506144526/https://www.bopa.ad/Documents/Detall?doc=QMH20211116_12_23_19)
            * [2023](https://web.archive.org/web/20250506144455/https://www.bopa.ad/Documents/Detall?doc=QMH20221216_12_41_13)
            * [2024](https://web.archive.org/web/20250506144500/https://www.bopa.ad/Documents/Detall?doc=QMH_2023_12_11_10_54_27)
            * [2025](https://web.archive.org/web/20250506144645/https://www.bopa.ad/Documents/Detall?doc=QMH_2024_11_19_09_10_48)
        * Ordino:
            * [2020](https://web.archive.org/web/20250506144555/https://www.bopa.ad/Documents/Detall?doc=QOH20191127_11_43_45)
            * [2021](https://web.archive.org/web/20250506144713/https://www.bopa.ad/Documents/Detall?doc=QOH20201229_12_24_42)
            * [2022](https://web.archive.org/web/20250506144715/https://www.bopa.ad/Documents/Detall?doc=QOH20211125_15_57_33)
            * [2023](https://web.archive.org/web/20250506144738/https://www.bopa.ad/Documents/Detall?doc=QOH20221228_10_02_52)
            * [2024](https://web.archive.org/web/20250506144729/https://www.bopa.ad/Documents/Detall?doc=QOH_2023_12_28_09_52_56)
            * [2025](https://web.archive.org/web/20250506144815/https://www.bopa.ad/Documents/Detall?doc=QOH_2024_11_28_13_42_34)
        * Sant Julià de Lòria:
            * [2020](https://web.archive.org/web/20250506144715/https://www.bopa.ad/Documents/Detall?doc=QSH20191122_11_45_30)
            * [2021](https://web.archive.org/web/20250506144827/https://www.bopa.ad/Documents/Detall?doc=QSH20201223_14_47_40)
            * [2022](https://web.archive.org/web/20250506144843/https://www.bopa.ad/Documents/Detall?doc=QSH20211217_08_48_18)
            * [2023](https://web.archive.org/web/20250506144859/https://www.bopa.ad/Documents/Detall?doc=QSH20221216_10_26_20)
            * [2024](https://web.archive.org/web/20250506145411/https://www.bopa.ad/Documents/Detall?doc=QSH_2023_12_07_11_20_16)
            * [2025](https://web.archive.org/web/20250506145311/https://www.bopa.ad/Documents/Detall?doc=QSH_2024_12_17_17_06_23)
        * Andorra la Vella:
            * [2020](https://web.archive.org/web/20250506142322/https://www.bopa.ad/Documents/Detall?doc=QAH20191128_08_58_08)
            * [2021](https://web.archive.org/web/20250506142457/https://www.bopa.ad/Documents/Detall?doc=QAH20201210_11_13_19)
            * [2022](https://web.archive.org/web/20250506142545/https://www.bopa.ad/Documents/Detall?doc=QAH20211111_16_06_47)
            * [2023](https://web.archive.org/web/20250506142748/https://www.bopa.ad/Documents/Detall?doc=QAH20221117_10_31_45)
            * [2024](https://web.archive.org/web/20250506142903/https://www.bopa.ad/Documents/Detall?doc=QAH_2023_11_17_12_40_42)
            * [2025](https://web.archive.org/web/20250506143036/https://www.bopa.ad/Documents/Detall?doc=QAH_2024_11_21_09_44_47)
        * Escaldes-Engordany:
            * [Parish foundation](https://web.archive.org/web/20130409081302/http://www.andorra.ad/ca-ES/Andorra/Pagines/comu_escaldes.aspx)
            * [2020](https://web.archive.org/web/20250506145337/https://www.bopa.ad/Documents/Detall?doc=QXH20200113_11_32_08)
            * [2021](https://web.archive.org/web/20250506145439/https://www.bopa.ad/Documents/Detall?doc=QXH20210127_13_33_13)
            * [2022](https://web.archive.org/web/20250506145442/https://www.bopa.ad/Documents/Detall?doc=QXH20220103_09_55_00)
            * [2023](https://web.archive.org/web/20250506145436/https://www.bopa.ad/Documents/Detall?doc=QXH20230102_11_21_25)
            * [2024](https://web.archive.org/web/20250506145940/https://www.bopa.ad/Documents/Detall?doc=QXH_2024_01_05_14_43_32)
            * [2025](https://web.archive.org/web/20250506145955/https://www.bopa.ad/Documents/Detall?doc=QXH_2024_12_23_11_47_04)
    """

    country = "AD"
    default_language = "ca"
    # The 1933 Revolution in Andorra
    start_year = 1934
    subdivisions = (
        "02",  # Canillo.
        "03",  # Encamp.
        "04",  # La Massana.
        "05",  # Ordino.
        "06",  # Sant Julià de Lòria.
        "07",  # Andorra la Vella.
        "08",  # Escaldes-Engordany.
    )
    subdivisions_aliases = {
        "Canillo": "02",
        "Encamp": "03",
        "La Massana": "04",
        "Ordino": "05",
        "Sant Julià de Lòria": "06",
        "Andorra la Vella": "07",
        "Escaldes-Engordany": "08",
    }
    supported_categories = (GOVERNMENT, PUBLIC)
    supported_languages = ("ca", "en_US", "uk")

    def __init__(self, *args, **kwargs) -> None:
        ChristianHolidays.__init__(self)
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self) -> None:
        # New Year's Day.
        self._add_new_years_day(tr("Cap d'Any"))

        # Epiphany.
        self._add_epiphany_day(tr("Reis"))

        # Carnival.
        self._add_carnival_monday(tr("Carnaval"))

        if self._year >= 1994:
            # Constitution Day.
            self._add_holiday_mar_14(tr("Dia de la Constitució"))

        # Good Friday.
        self._add_good_friday(tr("Divendres Sant"))

        # Easter Monday.
        self._add_easter_monday(tr("Dilluns de Pasqua"))

        # Labor Day.
        self._add_labor_day(tr("Festa del treball"))

        # Whit Monday.
        self._add_whit_monday(tr("Dilluns de Pentecosta"))

        # Assumption Day.
        self._add_assumption_of_mary_day(tr("Assumpció"))

        # Our Lady of Meritxell.
        self._add_holiday_sep_8(tr("Nostra Senyora de Meritxell"))

        # All Saints' Day.
        self._add_all_saints_day(tr("Tots Sants"))

        # Immaculate Conception.
        self._add_immaculate_conception_day(tr("Immaculada Concepció"))

        # Christmas Day.
        self._add_christmas_day(tr("Nadal"))

        # Saint Stephen's Day.
        self._add_christmas_day_two(tr("Sant Esteve"))

    def _populate_subdiv_02_public_holidays(self):
        # Saint Roch's Day.
        self._add_holiday_aug_16(tr("Sant Roc"))

    def _populate_subdiv_03_public_holidays(self):
        # There are no holidays common to the whole parish.
        pass

    def _populate_subdiv_04_public_holidays(self):
        # Saint Anthony's Day.
        self._add_holiday_jan_17(tr("Sant Antoni"))

    def _populate_subdiv_05_public_holidays(self):
        # Saint Peter's Day.
        self._add_saints_peter_and_paul_day(tr("Sant Pere"))

    def _populate_subdiv_06_public_holidays(self):
        # Saint Julian's Day.
        self._add_holiday_jan_7(tr("Sant Julià"))

        # Virgin Mary of Canòlich.
        self._add_holiday_last_sat_of_may(tr("Diada de Canòlich"))

        # Sant Julià de Lòria Festival.
        name = tr("Festa Major de Sant Julià de Lòria")
        self._add_holiday_2_days_past_last_sat_of_jul(name)
        self._add_holiday_3_days_past_last_sat_of_jul(name)

    def _populate_subdiv_07_public_holidays(self):
        # Andorra la Vella Festival.
        name = tr("Festa Major d'Andorra la Vella")
        self._add_holiday_1st_sat_of_aug(name)
        self._add_holiday_1_day_past_1st_sat_of_aug(name)
        self._add_holiday_2_days_past_1st_sat_of_aug(name)

    def _populate_subdiv_08_public_holidays(self):
        # The parish of Escaldes-Engordany created on June 14, 1978.
        if self._year <= 1978:
            return None

        # Saint Michael of Engolasters' Day.
        self._add_holiday_may_7(tr("Sant Miquel d'Engolasters"))

        # Parish foundation day.
        name = tr("Diada de la creació de la parròquia")
        if self._year >= 1997:
            self._add_holiday_1st_sun_from_jun_14(name)
        else:
            self._add_holiday_jun_14(name)

        # Escaldes-Engordany Festival.
        name = tr("Festa Major d'Escaldes-Engordany")
        self._add_holiday_jul_25(name)
        self._add_holiday_jul_26(name)

    def _populate_government_holidays(self):
        # %s (from 1pm).
        begin_time_label = self.tr("%s (a partir de les 13h)")

        # Epiphany Eve.
        self._add_holiday_jan_5(begin_time_label % self.tr("Vigília de Reis"))

        # Maundy Thursday.
        self._add_holy_thursday(begin_time_label % self.tr("Dijous Sant"))

        # Christmas Eve.
        self._add_christmas_eve(begin_time_label % self.tr("Vigília de Nadal"))

        # New Year's Eve.
        self._add_new_years_eve(begin_time_label % self.tr("Vigília de Cap d'Any"))


class AD(Andorra):
    pass


class AND(Andorra):
    pass
