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

from holidays.calendars.thai import THAI_CALENDAR, _ThaiLunisolar


class ThaiCalendarHolidays:
    """
    Thai lunisolar calendar holidays.

    For more info, see class `_ThaiLunisolar`.
    Calendar-type checking are done by `_ThaiLunisolar`.
    """

    def __init__(self, calendar=THAI_CALENDAR) -> None:
        self.__calendar = calendar
        self._thai_calendar = _ThaiLunisolar(calendar)

    def _add_asarnha_bucha(self, name) -> date | None:
        """
        Add Asarnha Bucha.

        Asalha Pūjā (also written as Asarnha Bucha Day) is a Buddhist festival
        celebrated on the 15th Waxing Day (Full Moon) of Month 8.

        https://en.wikipedia.org/wiki/Asalha_Puja
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.asarnha_bucha_date(self._year)
        )

    def _add_boun_haw_khao_padapdin(self, name) -> date | None:
        """
        Add Boun Haw Khao Padapdin.

        Boun Haw Khao Padapdin (also known as Rice Growing Festival)
        is a Buddhist festival celebrated on the 14th Waning Day of Month 9.

        https://web.archive.org/web/20250415072547/https://www.timsthailand.com/boon-khao-pradap-din/
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.boun_haw_khao_padapdin_date(self._year)
        )

    def _add_boun_haw_khao_salark(self, name) -> date | None:
        """
        Add Boun Haw Khao Salark.

        Boun Haw Khao Salark (also known as Ancestor Festival)
        is a Buddhist festival celebrated on the 15th Waxing Day of Month 10.

        https://web.archive.org/web/20250414145714/https://www.timsthailand.com/boon-khao-sak/
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.boun_haw_khao_salark_date(self._year)
        )

    def _add_boun_suang_heua(self, name) -> date | None:
        """
        Add Boun Suang Huea.

        Boun Suang Huea Nakhone Luang Prabang (also known as Vientiane Boat Racing Festival)
        is a Buddhist festival celebrated on the 1st Waning Day of Month 11.

        https://web.archive.org/web/20250413191357/https://sonasia-holiday.com/sonabee/laos-boat-racing-festival
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.boun_suang_heua_date(self._year)
        )

    def _add_khao_phansa(self, name) -> date | None:
        """
        Add Khao Phansa.

        Start of Buddhist Lent (also written as Khao Phansa Day) is a Buddhist
        festival celebrated on the 1st Waning Day of Month 8.

        https://en.wikipedia.org/wiki/Vassa
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.khao_phansa_date(self._year)
        )

    def _add_loy_krathong(self, name) -> date | None:
        """
        Add Loy Krathong.

        Also known as "Boun That Louang" and "Bon Om Touk".
        This concides with the 15th Waxing Day (Full Moon) of Month 12
        in Thai Lunar Calendar.

        https://en.wikipedia.org/wiki/Loy_Krathong
        https://en.wikipedia.org/wiki/Bon_Om_Touk
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.loy_krathong_date(self._year)
        )

    def _add_makha_bucha(self, name, calendar=None) -> date | None:
        """
        Add Makha Bucha.

        Māgha Pūjā (also written as Makha Bousa and Meak Bochea Day) is a Buddhist
        festival celebrated on the 15th Waxing Day (Full Moon) of Month 3.

        Khmer variant: always fall on Month 3.
        Thai variant:  will use Month 4 instead for Athikamat years.

        https://en.wikipedia.org/wiki/Māgha_Pūjā
        """
        calendar = calendar or self.__calendar

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.makha_bucha_date(self._year, calendar)
        )

    def _add_ok_phansa(self, name) -> date | None:
        """
        Add Ok Phansa.

        End of Buddhist Lent (also written as Ok Phansa Day) is a Buddhist
        festival celebrated on the 15th Waxing Day of Month 11.

        https://en.wikipedia.org/wiki/Pavarana
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.ok_phansa_date(self._year)
        )

    def _add_pchum_ben(self, name) -> date | None:
        """
        Add Pchum Ben.

        Also known as "Prachum Bandar".
        This concides with the 15th Waning Day (New Moon) of Month 10 in
        Thai Lunar Calendar.

        https://en.wikipedia.org/wiki/Pchum_Ben
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.pchum_ben_date(self._year)
        )

    def _add_preah_neangkoal(self, name) -> date | None:
        """
        Add Preah Reach Pithi Chrat Preah Neangkoal.

        Also known as "Cambodian Royal Ploughing Ceremony". This always
        concides with the 4th Waning Day of Month 6 in Khmer Lunar Calendar.

        https://en.wikipedia.org/wiki/Royal_Ploughing_Ceremony
        """

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.preah_neangkoal_date(self._year)
        )

    def _add_thai_calendar_holiday(self, name, dt) -> date | None:
        """
        Add Thai calendar holiday.

        If the result from `_ThaiLunisolar` is none then no holidays added.
        """
        if dt is None:
            return None

        return self._add_holiday(name, dt)

    def _add_visakha_bucha(self, name, calendar=None) -> date | None:
        """
        Add Visakha Bucha.

        Vesak (also written as Visakha Bousa Day and Visaka Bochea Day) is a
        Buddhist festival celebrated on the 15th Waxing Day (Full Moon)
        of Month 6.

        Khmer variant: always fall on Month 6.
        Thai variant:  will use Month 7 instead for Athikamat years.

        https://en.wikipedia.org/wiki/Vesak
        """
        calendar = calendar or self.__calendar

        return self._add_thai_calendar_holiday(
            name, self._thai_calendar.visakha_bucha_date(self._year, calendar)
        )
