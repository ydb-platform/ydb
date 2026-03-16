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
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    MON_TO_NEXT_TUE,
    SUN_TO_PREV_SAT,
    SUN_TO_NEXT_MON,
)


class Aruba(ObservedHolidayBase, ChristianHolidays, InternationalHolidays):
    """Aruba holidays.

    References:
        * [AB 2013 no. 14](https://web.archive.org/web/20250212184919/https://cuatro.sim-cdn.nl/arubaoverheid2858bd/uploads/ab2013no.14_0.pdf?cb=WDbZKYCl)
        * [Holidays List (English)]( https://web.archive.org/web/20230808172049/https://www.government.aw/information-public-services/hiring-people_47940/item/holidays_43823.html)
        * [Holidays List (Dutch)](https://web.archive.org/web/20231208145916/https://www.overheid.aw/informatie-dienstverlening/ondernemen-en-werken-subthemas_46970/item/feestdagen_37375.html)
        * [Holidays List (Papiamento)](https://web.archive.org/web/20231202011228/https://www.gobierno.aw/informacion-tocante-servicio/haci-negoshi-y-traha-sub-topics_47789/item/dia-di-fiesta_41242.html)
        * [National Holidays & Celebrations](https://web.archive.org/web/20250210000132/https://www.visitaruba.com/about-aruba/national-holidays-and-celebrations/)
        * <https://web.archive.org/web/20240619235841/https://www.arubatoday.com/we-celebrate-our-national-hero-betico-croes/>
        * <https://web.archive.org/web/20240721173750/https://caribbeannewsglobal.com/carnival-monday-remains-a-festive-day-in-aruba/>
        * <https://web.archive.org/web/20250426210119/https://www.aruba.com/us/calendar/national-anthem-and-flag-day>
    """

    country = "AW"
    default_language = "pap_AW"
    supported_languages = ("en_US", "nl", "pap_AW", "uk")
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

        # Dia Di Betico.
        # Status: In-Use.
        # Started in 1989.

        if self._year >= 1989:
            # Betico Day.
            self._add_holiday_jan_25(tr("Dia di Betico"))

        # Dialuna prome cu diaranson di shinish.
        # Status: In-Use.
        # Starts as a public holiday from 1956 onwards.
        # Event cancelled but remain a holiday in 2021.
        # Have its name changed from 2023 onwards.

        if self._year >= 1956:
            self._add_ash_monday(
                # Carnival Monday.
                tr("Dialuna despues di Carnaval Grandi")
                if self._year <= 2022
                # Monday before Ash Wednesday.
                else tr("Dialuna prome cu diaranson di shinish")
            )

        # Dia di Himno y Bandera.
        # Status: In-Use.
        # Started in 1976.

        if self._year >= 1976:
            # National Anthem and Flag Day.
            self._add_holiday_mar_18(tr("Dia di Himno y Bandera"))

        # Bierna Santo.
        # Status: In-Use.

        # Good Friday.
        self._add_good_friday(tr("Bierna Santo"))

        # Di dos dia di Pasco di Resureccion.
        # Status: In-Use.

        # Easter Monday.
        self._add_easter_monday(tr("Di dos dia di Pasco di Resureccion"))

        # Dia di Labor/Dia di Obrero.
        # Status: In-Use.
        # If fall on Sunday, then this will be move to next working day.
        # This is placed here before King's/Queen's Day for _move_holiday logic.

        self._move_holiday(
            # Labor Day.
            self._add_labor_day(tr("Dia di Obrero")),
            rule=SUN_TO_NEXT_MON if self._year >= 1980 else MON_TO_NEXT_TUE + SUN_TO_NEXT_MON,
        )

        # Aña di La Reina/Aña di Rey/Dia di Rey.
        # Status: In-Use.
        # Started under Queen Wilhelmina in 1891.
        # Queen Beatrix kept Queen Juliana's Birthday after her coronation.
        # Switched to Aña di Rey in 2014 for King Willem-Alexander.
        # Have its name changed again to Dia di Rey from 2021 onwards.

        name = (
            # King's Day.
            tr("Dia di Rey")
            if self._year >= 2021
            else (
                tr("Aña di Rey")  # King's Day.
                if self._year >= 2014
                else tr("Aña di La Reina")  # Queen's Day.
            )
        )
        self._move_holiday(
            self._add_holiday_apr_27(name)
            if self._year >= 2014
            else self._add_holiday_apr_30(name),
            rule=SUN_TO_PREV_SAT if self._year >= 1980 else SUN_TO_NEXT_MON,
        )

        # Dia di Asuncion.
        # Status: In-Use.

        # Ascension Day.
        self._add_ascension_thursday(tr("Dia di Asuncion"))

        # Pasco di Nacemento.
        # Status: In-Use.

        # Christmas Day.
        self._add_christmas_day(tr("Pasco di Nacemento"))

        # Di dos dia di Pasco di Nacemento.
        # Status: In-Use.

        # Second Day of Christmas.
        self._add_christmas_day_two(tr("Di dos dia di Pasco di Nacemento"))


class AW(Aruba):
    pass


class ABW(Aruba):
    pass
