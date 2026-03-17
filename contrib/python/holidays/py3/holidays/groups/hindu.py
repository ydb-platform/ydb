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

from collections.abc import Iterable
from datetime import date

from holidays.calendars.hindu import _HinduLunisolar
from holidays.groups.eastern import EasternCalendarHolidays


class HinduCalendarHolidays(EasternCalendarHolidays):
    """
    Hindu lunisolar calendar holidays.
    """

    def __init__(self, cls=None, *, show_estimated=False) -> None:
        self._hindu_calendar = cls() if cls else _HinduLunisolar()
        self._hindu_calendar_show_estimated = show_estimated

    def _add_hindu_calendar_holiday(
        self, name: str, dt_estimated: tuple[date | None, bool], days_delta: int = 0
    ) -> date | None:
        """
        Add Hindu calendar holiday.

        Adds customizable estimation label to holiday name if holiday date
        is an estimation.
        """

        return self._add_eastern_calendar_holiday(
            name,
            dt_estimated,
            show_estimated=self._hindu_calendar_show_estimated,
            days_delta=days_delta,
        )

    def _add_hindu_calendar_holiday_set(
        self, name: str, dts_estimated: Iterable[tuple[date, bool]], days_delta: int = 0
    ) -> set[date]:
        """
        Add Hindu calendar holidays.

        Adds customizable estimation label to holiday name if holiday date
        is an estimation.
        """
        return self._add_eastern_calendar_holiday_set(
            name,
            dts_estimated,
            show_estimated=self._hindu_calendar_show_estimated,
            days_delta=days_delta,
        )

    def _add_bathukamma(self, name) -> date | None:
        """
        Add Bathukamma Festival.

        Bathukamma is a floral festival celebrated predominantly in Telangana
        and some parts of Andhra Pradesh. It starts on Mahalaya Amavasya.
        https://en.wikipedia.org/wiki/Bathukamma
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.bathukamma_date(self._year)
        )

    def _add_bhai_dooj(self, name) -> date | None:
        """
        Add Bhai Dooj.

        Bhai Dooj, also known as Bhai Tika or Bhaiya Dooj, is a Hindu festival celebrating the bond
        between brothers and sisters. It is observed two days after Diwali on the second lunar day
        of the Shukla Paksha in the Hindu month of Kartika.
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.govardhan_puja_date(self._year), days_delta=+1
        )

    def _add_buddha_purnima(self, name) -> date | None:
        """
        Add Buddha Purnima.

        Buddha Purnima, also known as Vesak, commemorates the birth, enlightenment,
        and passing of Gautama Buddha. It falls on the full moon day of the
        Hindu month of Vaisakha (April-May).
        https://en.wikipedia.org/wiki/Vesak
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.buddha_purnima_date(self._year)
        )

    def _add_chhath_puja(self, name) -> date | None:
        """
        Add Chhath Puja.

        Chhath Puja is a Hindu festival dedicated to the Sun God (Surya).
        It is observed six days after Diwali in the month of Kartika (October-November).
        https://en.wikipedia.org/wiki/Chhath
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.chhath_puja_date(self._year)
        )

    def _add_diwali(self, name) -> date | None:
        """
        Add Diwali Festival.

        Diwali (Deepavali, Festival of Lights) is one of the most important
        festivals in Indian religions. It is celebrated during the Hindu
        lunisolar months of Ashvin and Kartika (between mid-October and
        mid-November).
        https://en.wikipedia.org/wiki/Diwali
        """
        return self._add_hindu_calendar_holiday(name, self._hindu_calendar.diwali_date(self._year))

    def _add_diwali_india(self, name) -> date | None:
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.diwali_india_date(self._year)
        )

    def _add_dussehra(self, name) -> date | None:
        """
        Add Dussehra Festival.

        Dussehra (Vijayadashami) is a major Hindu festival that marks the end
        of Navratri. It is celebrated on the 10th day of the Hindu lunisolar
        month of Ashvin (September-October).
        https://en.wikipedia.org/wiki/Vijayadashami
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.dussehra_date(self._year)
        )

    def _add_ganesh_chaturthi(self, name) -> date | None:
        """
        Add Ganesh Chaturthi.

        Ganesh Chaturthi is a Hindu festival celebrating the birth of Lord Ganesha.
        It falls on the fourth day of the Hindu month of Bhadrapada (August/September).
        https://en.wikipedia.org/wiki/Ganesh_Chaturthi
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.ganesh_chaturthi_date(self._year)
        )

    def _add_gau_krida(self, name) -> date | None:
        """
        Add Gau Krida.

        Gau Krida, is celebrated the day after Diwali to honor cows.
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.govardhan_puja_date(self._year), days_delta=-1
        )

    def _add_govardhan_puja(self, name) -> date | None:
        """
        Add Govardhan Puja.

        Govardhan Puja, also known as Annakut, is celebrated after Diwali
        to honor Lord Krishna. It falls on the first lunar day of the Hindu month of Kartika.
        https://en.wikipedia.org/wiki/Govardhan_Puja
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.govardhan_puja_date(self._year)
        )

    def _add_gudi_padwa(self, name) -> date | None:
        """
        Add Gudi Padwa / Ugadi.

        Gudi Padwa is the traditional New Year festival celebrated in
        Maharashtra. On the same day, the festival is also observed as
        Ugadi in Karnataka, Telangana, and Andhra Pradesh.

        It falls on the first day of Chaitra (March–April) according to
        the Hindu lunisolar calendar.

        References:
            * https://en.wikipedia.org/wiki/Gudi_Padwa
            * https://en.wikipedia.org/wiki/Ugadi
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.gudi_padwa_date(self._year)
        )

    def _add_guru_gobind_singh_jayanti(self, name) -> set[date]:
        """
        Add Guru Gobind Singh Jayanti.

        Guru Gobind Singh Jayanti commemorates the birth anniversary of
        Guru Gobind Singh, the tenth Sikh Guru. It follows the Nanakshahi calendar.
        https://en.wikipedia.org/wiki/Guru_Gobind_Singh
        """
        return self._add_hindu_calendar_holiday_set(
            name, self._hindu_calendar.guru_gobind_singh_jayanti_date(self._year)
        )

    def _add_guru_nanak_jayanti(self, name) -> date | None:
        """
        Add Guru Nanak Jayanti.

        Guru Nanak Jayanti celebrates the birth anniversary of Guru Nanak,
        the founder of Sikhism. It is observed on the full moon day of
        Kartik (October-November).
        https://en.wikipedia.org/wiki/Guru_Nanak_Gurpurab
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.guru_nanak_jayanti_date(self._year)
        )

    def _add_gyalpo_losar(self, name) -> date | None:
        """
        Add Gyalpo Losar.

        Gyalpo Losar marks the Tibetan New Year and is widely celebrated by the
        Tibetan and Sherpa communities in Nepal. It falls on the first day of the
        Tibetan lunar calendar, typically in February or March.
        https://en.wikipedia.org/wiki/Gyalpo_Losar
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.gyalpo_losar_date(self._year)
        )

    def _add_nepal_holi(self, name) -> date | None:
        """
        Add Holi Festival for Nepal (Mountain & Hilly).

        Holi, known as the Festival of Colors, is a Hindu festival that marks
        the arrival of spring. It is celebrated on the full moon day of the
        Hindu month of Phalguna (February/March).
        https://en.wikipedia.org/wiki/Holi
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.holi_date(self._year), days_delta=-1
        )

    def _add_holi(self, name) -> date | None:
        """
        Add Holi Festival.

        Holi, known as the Festival of Colors, is a Hindu festival that marks
        the arrival of spring. It is celebrated on the full moon day of the
        Hindu month of Phalguna (February/March).
        https://en.wikipedia.org/wiki/Holi
        """
        return self._add_hindu_calendar_holiday(name, self._hindu_calendar.holi_date(self._year))

    def _add_janmashtami(self, name) -> date | None:
        """
        Add Janmashtami.

        Janmashtami is a Hindu festival that celebrates the birth of Lord Krishna.
        It falls on the eighth day of the Hindu month of Bhadrapada (August/September).
        https://en.wikipedia.org/wiki/Krishna_Janmashtami
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.janmashtami_date(self._year)
        )

    def _add_maha_saptami(self, name) -> date | None:
        """
        Add Maha Saptami.

        Maha Saptami is the seventh day of Navratri, dedicated to Goddess Durga.
        It is observed in Ashvin (September-October).
        https://en.wikipedia.org/wiki/Navaratri
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.maha_ashtami_date(self._year), days_delta=-1
        )

    def _add_maha_ashtami(self, name) -> date | None:
        """
        Add Maha Ashtami.

        Maha Ashtami is the eighth day of Navratri, dedicated to Goddess Durga.
        It is observed in Ashvin (September-October).
        https://en.wikipedia.org/wiki/Navaratri
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.maha_ashtami_date(self._year)
        )

    def _add_maha_navami(self, name) -> date | None:
        """
        Add Maha Navami.

        Maha Navami is the ninth day of Navratri, dedicated to Goddess Durga.
        It is observed in Ashvin (September-October).
        https://en.wikipedia.org/wiki/Navaratri
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.maha_navami_date(self._year)
        )

    def _add_maha_shivaratri(self, name) -> date | None:
        """
        Add Maha Shivaratri.

        Maha Shivaratri is a Hindu festival dedicated to Lord Shiva. It is celebrated
        on the 14th night of the Hindu month of Phalguna (February/March).
        https://en.wikipedia.org/wiki/Maha_Shivaratri
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.maha_shivaratri_date(self._year)
        )

    def _add_maharana_pratap_jayanti(self, name) -> date | None:
        """
        Add Maharana Pratap Jayanti.

        Maharana Pratap Jayanti celebrates the birth of the Rajput king Maharana Pratap.
        It falls on the third day of the Shukla Paksha of the month of Jyeshtha.
        https://en.wikipedia.org/wiki/Maharana_Pratap
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.maharana_pratap_jayanti_date(self._year)
        )

    def _add_mahavir_jayanti(self, name) -> date | None:
        """
        Add Mahavir Jayanti.

        Mahavir Jayanti celebrates the birth of Lord Mahavira, the 24th
        Tirthankara of Jainism. It falls on the 13th day of Chaitra (March-April).
        https://en.wikipedia.org/wiki/Mahavir_Jayanti
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.mahavir_jayanti_date(self._year)
        )

    def _add_makar_sankranti(self, name) -> date | None:
        """
        Add Makar Sankranti.

        Makar Sankranti is a Hindu festival that marks the transition of the Sun
        into Capricorn (Makar). It is celebrated on January 14th or 15th every year.
        https://en.wikipedia.org/wiki/Makar_Sankranti
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.makar_sankranti_date(self._year)
        )

    def _add_onam(self, name) -> date | None:
        """
        Add Onam.

        Onam is a major festival in Kerala, celebrating the homecoming of
        King Mahabali. It falls in the month of Chingam (August-September).
        https://en.wikipedia.org/wiki/Onam
        """
        return self._add_hindu_calendar_holiday(name, self._hindu_calendar.onam_date(self._year))

    def _add_papankusha_ekadashi(self, name) -> date | None:
        """
        Add Papankusha Ekadashi.

        Papankusha Ekadashi is a Hindu festival which occurs on eleventh day on month of
        Ashwin (September-October).
        https://en.wikipedia.org/wiki/Ekadashi
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.dussehra_date(self._year), days_delta=+1
        )

    def _add_papankusha_duwadashi(self, name) -> date | None:
        """
        Add Papankusha Duwadashi.

        Papankusha Duwadashi is a Hindu festival which occurs next day of Papankusha Ekadashi.
        https://en.wikipedia.org/wiki/Ekadashi
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.dussehra_date(self._year), days_delta=+2
        )

    def _add_pongal(self, name) -> date | None:
        """
        Add Pongal.

        Pongal is a major harvest festival celebrated in Tamil Nadu, India, marking the
        beginning of the sun's northward journey (Uttarayana). It is usually observed
        on January 14th or 15th every year, coinciding with the Tamil month of Thai.
        The festival is dedicated to the Sun God and marks a season of prosperity and abundance.
        https://en.wikipedia.org/wiki/Pongal_(festival)
        """
        return self._add_hindu_calendar_holiday(name, self._hindu_calendar.pongal_date(self._year))

    def _add_raksha_bandhan(self, name) -> date | None:
        """
        Add Raksha Bandhan.

        Raksha Bandhan is a Hindu festival that celebrates the bond between
        brothers and sisters. It falls on the full moon day of the Hindu month
        of Shravana (July/August).
        https://en.wikipedia.org/wiki/Raksha_Bandhan
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.raksha_bandhan_date(self._year)
        )

    def _add_ram_navami(self, name) -> date | None:
        """
        Add Ram Navami.

        Ram Navami is a Hindu festival celebrating the birth of Lord Rama.
        It is observed on the ninth day of the Hindu month of Chaitra (March/April).
        https://en.wikipedia.org/wiki/Rama_Navami
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.ram_navami_date(self._year)
        )

    def _add_sharad_navratri(self, name) -> date | None:
        """
        Add Navratri / Sharad Navratri.

        Navratri is a Hindu festival dedicated to the worship of Goddess Durga.
        It is celebrated over nine nights and occurs in the lunar month of Ashvin
        (September/October).
        https://en.wikipedia.org/wiki/Navratri
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.sharad_navratri_date(self._year)
        )

    def _add_sonam_losar(self, name) -> date | None:
        """
        Add Sonam Losar.

        Sonam Losar is the New Year festival celebrated by the Tamang community
        in Nepal. It follows the Tibetan lunar calendar and usually falls in
        January or February.
        https://en.wikipedia.org/wiki/Sonam_Lhosar
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.sonam_losar_date(self._year)
        )

    def _add_tamu_losar(self, name) -> date | None:
        """
        Add Tamu Losar.

        Tamu Losar marks the New Year festival of the Gurung community in Nepal.
        It is traditionally celebrated on December 30th each year.
        https://en.wikipedia.org/wiki/Tamu_Lhosar
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.tamu_losar_date(self._year)
        )

    def _add_thaipusam(self, name) -> date | None:
        """
        Add Thaipusam.

        Thaipusam is a Tamil Hindu festival celebrated on the full moon
        of the Tamil month of Thai (January/February).
        https://en.wikipedia.org/wiki/Thaipusam
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.thaipusam_date(self._year)
        )

    def _add_thiruvalluvar_day(self, name) -> date | None:
        """
        Add Thiruvalluvar Day and Mattu Pongal.

        Thiruvalluvar Day and Mattu Pongal are celebrated in Tamil Nadu, India, as part
        of the Pongal festival. Thiruvalluvar Day honors the classical Tamil poet and
        philosopher Thiruvalluvar, while Mattu Pongal is dedicated to cattle, recognizing
        their importance in agriculture. Both events usually fall on January 15th or 16th
        each year during the Tamil month of Thai.
        https://en.wikipedia.org/wiki/Thiruvalluvar_Day
        https://en.wikipedia.org/wiki/Pongal_(festival)#Mattu_Pongal
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.pongal_date(self._year), days_delta=+1
        )

    def _add_uzhavar_thirunal(self, name) -> date | None:
        """
        Add Uzhavar Thirunal.

        Uzhavar Thirunal is a harvest festival celebrated in Tamil Nadu, India,
        as part of the Pongal festivities. It is dedicated to honoring farmers
        (uzhavar) and their contribution to agriculture. Uzhavar Thirunal usually
        falls on January 16th or 17th each year.
        https://en.wikipedia.org/wiki/Pongal_(festival)#Uzhavar_Thirunal
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.pongal_date(self._year), days_delta=+2
        )

    def _add_vaisakhi(self, name) -> date | None:
        """
        Add Vaisakhi.

        Vaisakhi is a major Sikh festival marking the Sikh New Year and the
        founding of the Khalsa. It falls on April 13 or 14.
        https://en.wikipedia.org/wiki/Vaisakhi
        """
        return self._add_hindu_calendar_holiday(
            name, self._hindu_calendar.vaisakhi_date(self._year)
        )
