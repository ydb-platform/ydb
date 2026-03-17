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

from __future__ import annotations

from gettext import gettext as tr
from typing import TYPE_CHECKING

from holidays.calendars import (
    _CustomBuddhistHolidays,
    _CustomChineseHolidays,
    _CustomHinduHolidays,
    _CustomIslamicHolidays,
)
from holidays.calendars.gregorian import (
    JAN,
    FEB,
    MAR,
    APR,
    MAY,
    JUN,
    JUL,
    AUG,
    SEP,
    OCT,
    NOV,
    DEC,
    FRI,
    SAT,
    SUN,
)
from holidays.groups import (
    BuddhistCalendarHolidays,
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
)
from holidays.observed_holiday_base import (
    ObservedHolidayBase,
    FRI_TO_NEXT_WORKDAY,
    SAT_TO_NEXT_WORKDAY,
    SUN_TO_NEXT_WORKDAY,
)

if TYPE_CHECKING:
    from datetime import date


class Malaysia(
    ObservedHolidayBase,
    BuddhistCalendarHolidays,
    ChineseCalendarHolidays,
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
    StaticHolidays,
):
    """Malaysia holidays.

    References:
        * [Holidays Act 1951](https://web.archive.org/web/20241202103403/https://www.kabinet.gov.my/bkpp/pdf/akta_warta/1951_12_31_act369.pdf)
        * [Holidays Ordinance (Sabah Cap. 56)](https://web.archive.org/web/20201028045259/https://sagc.sabah.gov.my/sites/default/files/law/HolidaysOrdinance.pdf)
        * [Public Holidays Ordinance (Sarawak Cap. 8)](https://web.archive.org/web/20221208142318/https://www.kabinet.gov.my/bkpp/pdf/akta_warta/sarawak_public_holidays_ord_chapter8.pdf)
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Malaysia>
        * <https://web.archive.org/web/20250123115300/https://www.nst.com.my/news/nation/2020/03/571660/agongs-birthday-moved-june-6-june-8>
        * <https://web.archive.org/web/20240228225038/https://www.nst.com.my/news/nation/2024/02/1014012/melaka-cm-suggests-declaring-feb-20-federal-public-holiday-mark>
        * <https://web.archive.org/web/20251216120554/https://www.kabinet.gov.my/storage/2025/08/HKA-2026.pdf>

    Subdivisions Holidays References:
        * Sabah:
            * [2001](https://web.archive.org/web/20230605164302/https://www.sabah.gov.my/gazette/docs/000207.pdf)
            * [2002](https://web.archive.org/web/20230605173234/https://www.sabah.gov.my/gazette/docs/000968.pdf)
            * [2003](https://web.archive.org/web/20230418041718/https://www.sabah.gov.my/gazette/docs/001736.pdf)
            * [2004](https://web.archive.org/web/20230414022317/https://www.sabah.gov.my/gazette/docs/001546.pdf)
            * [2005](https://web.archive.org/web/20230420233014/https://www.sabah.gov.my/gazette/docs/001714.pdf)
            * [2006](https://web.archive.org/web/20230414141628/https://www.sabah.gov.my/gazette/docs/001803.pdf)
            * [2007](https://web.archive.org/web/20230414125605/https://www.sabah.gov.my/gazette/docs/001866.pdf)
            * [2008](https://web.archive.org/web/20230605175854/https://www.sabah.gov.my/gazette/docs/001942.pdf)
            * [2009](https://web.archive.org/web/20230605180330/https://www.sabah.gov.my/gazette/docs/001998.pdf)
            * [Christmas Eve since 2019](https://web.archive.org/web/20221207071317/https://www.sabah.gov.my/gazette/docs/002712.pdf)
            * [Birthday of the Governor of Sabah update 2010](https://web.archive.org/web/20230414125515/https://www.sabah.gov.my/gazette/docs/002114.pdf)
            * [2025](https://web.archive.org/web/20251216152001/https://www.sabah.gov.my/gazette/docs/003106.pdf)
            * [2025 changes](https://web.archive.org/web/20250330061412/https://www.sabah.gov.my/gazette/docs/003144.pdf)
            * [2026](https://web.archive.org/web/20251010011859/https://www.sabah.gov.my/gazette/docs/003152.pdf)

    Section 3 of Holidays Act 1951:
    > If any day specified in the Schedule falls on Sunday then the day following shall be
    > a public holiday and if such day is already a public holiday, then the day following
    > shall be a public holiday".

    In Johor (until 1994 and in 2014-2024) and Kedah it's Friday to Sunday,
    in Kelantan and Terengganu - Saturday to Sunday.
    """

    country = "MY"
    default_language = "ms_MY"
    # %s (estimated).
    estimated_label = tr("%s (anggaran)")
    # %s (in lieu).
    observed_label = tr("Cuti %s")
    # %s (observed, estimated).
    observed_estimated_label = tr("Cuti %s (anggaran)")
    start_year = 1952
    subdivisions = (
        "01",  # Johor.
        "02",  # Kedah.
        "03",  # Kelantan.
        "04",  # Melaka.
        "05",  # Negeri Sembilan.
        "06",  # Pahang.
        "07",  # Pulau Pinang.
        "08",  # Perak.
        "09",  # Perlis.
        "10",  # Selangor.
        "11",  # Terengganu.
        "12",  # Sabah.
        "13",  # Sarawak.
        "14",  # Wilayah Persekutuan Kuala Lumpur.
        "15",  # Wilayah Persekutuan Labuan.
        "16",  # Wilayah Persekutuan Putrajaya.
    )
    subdivisions_aliases = {
        "Johor": "01",
        "JHR": "01",
        "Kedah": "02",
        "KDH": "02",
        "Kelantan": "03",
        "KTN": "03",
        "Melaka": "04",
        "MLK": "04",
        "Negeri Sembilan": "05",
        "NSN": "05",
        "Pahang": "06",
        "PHG": "06",
        "Pulau Pinang": "07",
        "PNG": "07",
        "Perak": "08",
        "PRK": "08",
        "Perlis": "09",
        "PLS": "09",
        "Selangor": "10",
        "SGR": "10",
        "Terengganu": "11",
        "TRG": "11",
        "Sabah": "12",
        "SBH": "12",
        "Sarawak": "13",
        "SWK": "13",
        "Wilayah Persekutuan Kuala Lumpur": "14",
        "KUL": "14",
        "Wilayah Persekutuan Labuan": "15",
        "LBN": "15",
        "Wilayah Persekutuan Putrajaya": "16",
        "PJY": "16",
    }
    supported_languages = ("en_US", "ms_MY", "th")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        BuddhistCalendarHolidays.__init__(self, cls=MalaysiaBuddhistHolidays, show_estimated=True)
        ChineseCalendarHolidays.__init__(self, cls=MalaysiaChineseHolidays, show_estimated=True)
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self, cls=MalaysiaHinduHolidays)
        InternationalHolidays.__init__(self)
        IslamicHolidays.__init__(
            self, cls=MalaysiaIslamicHolidays, show_estimated=islamic_show_estimated
        )
        StaticHolidays.__init__(self, cls=MalaysiaStaticHolidays)
        kwargs.setdefault("observed_rule", SUN_TO_NEXT_WORKDAY)
        super().__init__(*args, **kwargs)

    def _get_weekend(self, dt: date) -> set[int]:
        if (
            self.subdiv == "01" and (dt.year <= 1994 or 2014 <= dt.year <= 2024)
        ) or self.subdiv in {"02", "03", "11"}:
            return {FRI, SAT}
        else:
            return {SAT, SUN}

    def _populate_public_holidays(self):
        self.dts_observed = set()

        # Chinese New Year.
        self.dts_observed.add(self._add_chinese_new_years_day(tr("Tahun Baharu Cina")))

        self.dts_observed.add(
            # Chinese New Year (Second Day).
            self._add_chinese_new_years_day_two(tr("Tahun Baharu Cina (Hari Kedua)"))
        )

        # Vesak Day.
        self.dts_observed.add(self._add_vesak_may(tr("Hari Wesak")))

        if self._year >= 1973:
            # Labor Day.
            self.dts_observed.add(self._add_labor_day(tr("Hari Pekerja")))

        # Birthday of HM Yang di-Pertuan Agong.
        name = tr("Hari Keputeraan Rasmi Seri Paduka Baginda Yang di-Pertuan Agong")
        if self._year <= 2016:
            self.dts_observed.add(self._add_holiday_1st_sat_of_jun(name))
        elif self._year <= 2019:
            self.dts_observed.add(self._add_holiday_sep_9(name))
        elif self._year == 2020:
            self.dts_observed.add(self._add_holiday_jun_8(name))
        else:
            self.dts_observed.add(self._add_holiday_1st_mon_of_jun(name))

        # National Day.
        self.dts_observed.add(self._add_holiday_aug_31(tr("Hari Kebangsaan")))

        if self._year >= 2010:
            # Malaysia Day.
            self.dts_observed.add(self._add_holiday_sep_16(tr("Hari Malaysia")))

        # Christmas Day.
        self.dts_observed.add(self._add_christmas_day(tr("Hari Krismas")))

        if self._year >= 1995:
            # Islamic New Year.
            self._add_islamic_new_year_day(tr("Awal Muharam"))

        # Prophet Muhammad's Birthday.
        self.dts_observed.update(self._add_mawlid_day(tr("Hari Keputeraan Nabi Muhammad S.A.W.")))

        # Eid al-Fitr.
        self.dts_observed.update(self._add_eid_al_fitr_day(tr("Hari Raya Puasa")))

        # Eid al-Fitr (Second Day).
        self.dts_observed.update(self._add_eid_al_fitr_day_two(tr("Hari Raya Puasa (Hari Kedua)")))

        # Eid al-Adha.
        self.dts_observed.update(self._add_eid_al_adha_day(tr("Hari Raya Qurban")))

    def _populate_subdiv_holidays(self):
        if self.subdiv and self.subdiv not in {"13", "15"}:
            # Deepavali.
            self.dts_observed.add(self._add_diwali(tr("Hari Deepavali")))

        super()._populate_subdiv_holidays()

        if (
            self.subdiv == "01" and (self._year <= 1994 or 2014 <= self._year <= 2024)
        ) or self.subdiv == "02":
            self._observed_rule = FRI_TO_NEXT_WORKDAY
        elif self.subdiv in {"03", "11"}:
            self._observed_rule = SAT_TO_NEXT_WORKDAY
        else:
            self._observed_rule = SUN_TO_NEXT_WORKDAY

        if self.observed:
            self._populate_observed(self.dts_observed)

    def _populate_subdiv_01_public_holidays(self):
        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        if self._year >= 2015:
            # Birthday of the Sultan of Johor.
            self._add_holiday_mar_23(tr("Hari Keputeraan Sultan Johor"))

        if self._year >= 2011:
            # The Sultan of Johor Hol.
            self._add_hari_hol_johor(tr("Hari Hol Almarhum Sultan Iskandar"))

        # Beginning of Ramadan.
        self.dts_observed.update(self._add_ramadan_beginning_day(tr("Awal Ramadan")))

    def _populate_subdiv_02_public_holidays(self):
        if self._year >= 2022:
            # Thaipusam.
            self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        if self._year >= 2018:
            # Birthday of The Sultan of Kedah.
            name = tr("Hari Keputeraan Sultan Kedah")
            if self._year == 2024:
                self._add_holiday_jun_30(name)
            else:
                self._add_holiday_3rd_sun_of_jun(name)

        # Isra' and Mi'raj.
        self.dts_observed.update(self._add_isra_and_miraj_day(tr("Israk dan Mikraj")))

        # Beginning of Ramadan.
        self.dts_observed.update(self._add_ramadan_beginning_day(tr("Awal Ramadan")))

        # Eid al-Adha (Second Day).
        self.dts_observed.update(
            self._add_eid_al_adha_day_two(tr("Hari Raya Qurban (Hari Kedua)"))
        )

    def _populate_subdiv_03_public_holidays(self):
        if self._year >= 2010:
            # Birthday of the Sultan of Kelantan.
            name = tr("Hari Keputeraan Sultan Kelantan")
            if self._year >= 2023:
                self._add_holiday_sep_29(name)
                self._add_holiday_sep_30(name)
            elif self._year >= 2012:
                self._add_holiday_nov_11(name)
                self._add_holiday_nov_12(name)
            else:
                self._add_holiday_mar_30(name)
                self._add_holiday_mar_31(name)

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

        if self._year >= 2023:
            # Arafat Day.
            self.dts_observed.update(self._add_arafah_day(tr("Hari Arafah")))

        # Eid al-Adha (Second Day).
        self.dts_observed.update(
            self._add_eid_al_adha_day_two(tr("Hari Raya Qurban (Hari Kedua)"))
        )

    def _populate_subdiv_04_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        if self._year >= 2024:
            self.dts_observed.add(
                # Declaration of Independence Day.
                self._add_holiday_feb_20(tr("Hari Pengisytiharan Tarikh Kemerdekaan"))
            )
        elif self._year >= 1989:
            self.dts_observed.add(
                self._add_holiday_apr_15(
                    # Declaration of Malacca as a Historical City.
                    tr("Hari Perisytiharan Melaka Sebagai Bandaraya Bersejarah")
                )
            )

        # Birthday of the Governor of Malacca.
        name = tr("Hari Jadi Yang di-Pertua Negeri Melaka")
        self.dts_observed.add(
            self._add_holiday_aug_24(name)
            if self._year >= 2020
            else self._add_holiday_2nd_fri_of_oct(name)
        )

        if self._year >= 2025:
            self.dts_observed.update(
                # Eid al-Fitr (Third Day).
                self._add_eid_al_fitr_day_three(tr("Hari Raya Puasa (Hari Ketiga)"))
            )
        else:
            # Beginning of Ramadan.
            self.dts_observed.update(self._add_ramadan_beginning_day(tr("Awal Ramadan")))

    def _populate_subdiv_05_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        if self._year >= 2009:
            self.dts_observed.add(
                self._add_holiday_jan_14(
                    # Birthday of the Sultan of Negeri Sembilan.
                    tr("Hari Keputeraan Yang di-Pertuan Besar Negeri Sembilan")
                )
            )

        # Isra' and Mi'raj.
        self.dts_observed.update(self._add_isra_and_miraj_day(tr("Israk dan Mikraj")))

    def _populate_subdiv_06_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        if self._year >= 1975:
            # The Sultan of Pahang Hol.
            name = tr("Hari Hol Sultan Pahang")
            self.dts_observed.add(
                self._add_holiday_may_22(name)
                if self._year >= 2020
                else self._add_holiday_may_7(name)
            )

            # Birthday of the Sultan of Pahang.
            name = tr("Hari Keputeraan Sultan Pahang")
            self.dts_observed.add(
                self._add_holiday_jul_30(name)
                if self._year >= 2019
                else self._add_holiday_oct_24(name)
            )

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

    def _populate_subdiv_07_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        if self._year >= 2009:
            self.dts_observed.add(
                # George Town Heritage Day.
                self._add_holiday_jul_7(tr("Hari Ulang Tahun Perisytiharan Tapak Warisan Dunia"))
            )

        self.dts_observed.add(
            # Birthday of the Governor of Penang.
            self._add_holiday_2nd_sat_of_jul(tr("Hari Jadi Yang di-Pertua Negeri Pulau Pinang"))
        )

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

    def _populate_subdiv_08_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        # Birthday of the Sultan of Perak.
        name = tr("Hari Keputeraan Sultan Perak")
        if self._year >= 2018:
            self._add_holiday_1st_fri_of_nov(name)
        else:
            self._add_holiday_nov_27(name)

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

    def _populate_subdiv_09_public_holidays(self):
        if self._year >= 2000:
            # Birthday of the Raja of Perlis.
            name = tr("Hari Ulang Tahun Keputeraan Raja Perlis")
            self.dts_observed.add(
                self._add_holiday_jul_17(name)
                if 2018 <= self._year <= 2021
                else self._add_holiday_may_17(name)
            )

        # Isra' and Mi'raj.
        self.dts_observed.update(self._add_isra_and_miraj_day(tr("Israk dan Mikraj")))

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

        # Eid al-Adha (Second Day).
        self.dts_observed.update(
            self._add_eid_al_adha_day_two(tr("Hari Raya Qurban (Hari Kedua)"))
        )

    def _populate_subdiv_10_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        # Birthday of The Sultan of Selangor.
        self.dts_observed.add(self._add_holiday_dec_11(tr("Hari Keputeraan Sultan Selangor")))

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

    def _populate_subdiv_11_public_holidays(self):
        if self._year >= 2000:
            self.dts_observed.add(
                self._add_holiday_mar_4(
                    # Anniversary of the Installation of the Sultan of Terengganu.
                    tr("Hari Ulang Tahun Pertabalan Sultan Terengganu")
                )
            )

            self.dts_observed.add(
                # Birthday of the Sultan of Terengganu.
                self._add_holiday_apr_26(tr("Hari Keputeraan Sultan Terengganu"))
            )

        if self._year >= 2020:
            # Isra' and Mi'raj.
            self.dts_observed.update(self._add_isra_and_miraj_day(tr("Israk dan Mikraj")))

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

        # Arafat Day.
        self.dts_observed.update(self._add_arafah_day(tr("Hari Arafah")))

        self.dts_observed.update(
            # Eid al-Adha (Second Day).
            self._add_eid_al_adha_day_two(tr("Hari Raya Qurban (Hari Kedua)"))
        )

    def _populate_subdiv_12_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Pesta Kaamatan.
        name = tr("Pesta Kaamatan")
        self._add_holiday_may_30(name)
        self._add_holiday_may_31(name)

        if self._year >= 2001:
            # Birthday of the Governor of Sabah.
            name = tr("Hari Jadi Yang di-Pertua Negeri Sabah")
            if self._year >= 2025:
                self._add_holiday_mar_30(name)
            elif self._year >= 2010:
                self._add_holiday_1st_sat_of_oct(name)
            elif self._year == 2005:
                self._add_holiday_feb_11(name)
            else:
                self._add_holiday_sep_16(name)

        if self._year >= 2019:
            # Christmas Eve.
            self._add_christmas_eve(tr("Krismas (Eve)"))

    def _populate_subdiv_13_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        if self._year >= 1965:
            # Dayak Festival Day.
            name = tr("Perayaan Hari Gawai Dayak")
            self.dts_observed.add(self._add_holiday_jun_1(name))
            self.dts_observed.add(self._add_holiday_jun_2(name))

        # Birthday of the Governor of Sarawak.
        self._add_holiday_2nd_sat_of_oct(tr("Hari Jadi Yang di-Pertua Negeri Sarawak"))

        if self._year >= 2017:
            # Sarawak Independence Day.
            self.dts_observed.add(self._add_holiday_jul_22(tr("Hari Kemerdekaan Sarawak")))

    def _populate_subdiv_14_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        if self._year >= 1974:
            # Federal Territory Day.
            self.dts_observed.add(self._add_holiday_feb_1(tr("Hari Wilayah Persekutuan")))

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

    def _populate_subdiv_15_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        if self._year >= 1974:
            # Federal Territory Day.
            self.dts_observed.add(self._add_holiday_feb_1(tr("Hari Wilayah Persekutuan")))

        # Pesta Kaamatan.
        name = tr("Pesta Kaamatan")
        self._add_holiday_may_30(name)
        self._add_holiday_may_31(name)

        if self._year >= 2014:
            # Deepavali.
            self.dts_observed.add(self._add_diwali(tr("Hari Deepavali")))

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))

    def _populate_subdiv_16_public_holidays(self):
        # New Year's Day.
        self.dts_observed.add(self._add_new_years_day(tr("Tahun Baharu")))

        # Thaipusam.
        self.dts_observed.add(self._add_thaipusam(tr("Hari Thaipusam")))

        if self._year >= 1974:
            # Federal Territory Day.
            self.dts_observed.add(self._add_holiday_feb_1(tr("Hari Wilayah Persekutuan")))

        # Nuzul Al-Quran Day.
        self.dts_observed.update(self._add_nuzul_al_quran_day(tr("Hari Nuzul Al-Quran")))


class MY(Malaysia):
    pass


class MYS(Malaysia):
    pass


class MalaysiaBuddhistHolidays(_CustomBuddhistHolidays):
    VESAK_MAY_DATES = {
        2001: (MAY, 7),
        2002: (MAY, 27),
        2003: (MAY, 15),
        2004: (MAY, 3),
        2005: (MAY, 22),
        2006: (MAY, 12),
        2007: (MAY, 1),
        2008: (MAY, 19),
        2009: (MAY, 9),
        2010: (MAY, 28),
        2011: (MAY, 17),
        2012: (MAY, 5),
        2013: (MAY, 24),
        2014: (MAY, 13),
        2015: (MAY, 3),
        2016: (MAY, 21),
        2017: (MAY, 10),
        2018: (MAY, 29),
        2019: (MAY, 19),
        2020: (MAY, 7),
        2021: (MAY, 26),
        2022: (MAY, 15),
        2023: (MAY, 4),
        2024: (MAY, 22),
        2025: (MAY, 12),
        2026: (MAY, 31),
    }


class MalaysiaChineseHolidays(_CustomChineseHolidays):
    LUNAR_NEW_YEAR_DATES_CONFIRMED_YEARS = (2001, 2026)


class MalaysiaHinduHolidays(_CustomHinduHolidays):
    DIWALI_DATES = {
        2001: (NOV, 14),
        2002: (NOV, 3),
        2003: (OCT, 23),
        2004: (NOV, 11),
        2005: (NOV, 1),
        2006: (OCT, 21),
        2007: (NOV, 8),
        2008: (OCT, 27),
        2009: (OCT, 17),
        2010: (NOV, 5),
        2011: (OCT, 26),
        2012: (NOV, 13),
        2013: (NOV, 2),
        2014: (OCT, 22),
        2015: (NOV, 10),
        2016: (OCT, 29),
        2017: (OCT, 18),
        2018: (NOV, 6),
        2019: (OCT, 27),
        2020: (NOV, 14),
        2021: (NOV, 4),
        2022: (OCT, 24),
        2023: (NOV, 12),
        2024: (OCT, 31),
        2025: (OCT, 20),
        2026: (NOV, 8),
    }

    THAIPUSAM_DATES = {
        2018: (JAN, 31),
        2019: (JAN, 21),
        2020: (FEB, 8),
        2021: (JAN, 28),
        2022: (JAN, 18),
        2023: (FEB, 5),
        2024: (JAN, 25),
        2025: (FEB, 11),
        2026: (FEB, 1),
        2027: (JAN, 22),
    }


class MalaysiaIslamicHolidays(_CustomIslamicHolidays):
    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2026)
    EID_AL_ADHA_DATES = {
        2001: (MAR, 6),
        2002: (FEB, 23),
        2003: (FEB, 12),
        2004: (FEB, 2),
        2008: (DEC, 9),
        2009: (NOV, 28),
        2010: (NOV, 17),
        2011: (NOV, 7),
        2014: (OCT, 5),
        2015: (SEP, 24),
        2016: (SEP, 12),
        2018: (AUG, 22),
        2022: (JUL, 10),
        2023: (JUN, 29),
        2024: (JUN, 17),
        2025: (JUN, 7),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2026)
    EID_AL_FITR_DATES = {
        2001: (DEC, 17),
        2002: (DEC, 6),
        2003: (NOV, 26),
        2006: (OCT, 24),
        2011: (AUG, 31),
        2019: (JUN, 5),
        2023: (APR, 22),
        2025: (MAR, 31),
        2026: (MAR, 21),
    }

    HARI_HOL_JOHOR_DATES_CONFIRMED_YEARS = (2011, 2026)
    HARI_HOL_JOHOR_DATES = {
        2011: (JAN, 12),
        2012: (DEC, 20),
        2013: (DEC, 10),
        2014: (NOV, 29),
        2015: (NOV, 19),
        2016: (NOV, 7),
        2017: (OCT, 27),
        2020: (SEP, 24),
        2022: (SEP, 3),
        2023: (AUG, 23),
        2024: (AUG, 11),
        2026: (JUL, 21),
    }

    HIJRI_NEW_YEAR_DATES_CONFIRMED_YEARS = (2001, 2026)
    HIJRI_NEW_YEAR_DATES = {
        2003: (MAR, 5),
        2004: (FEB, 22),
        2010: (DEC, 8),
        2011: (NOV, 27),
        2013: (NOV, 5),
        2017: (SEP, 22),
        2019: (SEP, 1),
        2021: (AUG, 10),
        2025: (JUN, 27),
        2026: (JUN, 17),
    }

    ISRA_AND_MIRAJ_DATES_CONFIRMED_YEARS = (2001, 2026)
    ISRA_AND_MIRAJ_DATES = {
        2001: (OCT, 15),
        2006: (AUG, 22),
        2007: (AUG, 11),
        2008: (JUL, 31),
        2014: (MAY, 27),
        2016: (MAY, 5),
        2018: (APR, 14),
        2022: (MAR, 1),
        2026: (JAN, 17),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2001, 2026)
    MAWLID_DATES = {
        2003: (MAY, 14),
        2004: (MAY, 2),
        2006: (APR, 11),
        2011: (FEB, 16),
        2012: (FEB, 5),
        2014: (JAN, 14),
        2015: ((JAN, 3), (DEC, 24)),
        2016: (DEC, 12),
        2017: (DEC, 1),
        2021: (OCT, 19),
        2022: (OCT, 10),
        2023: (SEP, 28),
        2024: (SEP, 16),
        2025: (SEP, 5),
    }

    NUZUL_AL_QURAN_DATES_CONFIRMED_YEARS = (2001, 2026)
    NUZUL_AL_QURAN_DATES = {
        2001: (DEC, 3),
        2003: (NOV, 12),
        2004: (NOV, 1),
        2005: (OCT, 21),
        2008: (SEP, 18),
        2014: (JUL, 15),
        2018: (JUN, 2),
        2022: (APR, 19),
        2024: (MAR, 28),
        2025: (MAR, 18),
        2026: (MAR, 7),
    }

    RAMADAN_BEGINNING_DATES_CONFIRMED_YEARS = (2001, 2026)
    RAMADAN_BEGINNING_DATES = {
        2001: (NOV, 17),
        2003: (OCT, 27),
        2004: (OCT, 16),
        2005: (OCT, 5),
        2008: (SEP, 2),
        2014: (JUN, 29),
        2016: (JUN, 7),
        2018: (MAY, 17),
        2022: (APR, 3),
        2024: (MAR, 12),
        2025: (MAR, 2),
        2026: (FEB, 19),
    }


class MalaysiaStaticHolidays:
    # General election additional holiday.
    general_election_additional_holiday = tr("Cuti Peristiwa (pilihan raya umum)")

    # Additional holiday.
    additional_holiday = tr("Cuti Peristiwa")

    # Eid al-Adha.
    eid_al_adha = tr("Hari Raya Qurban")

    # Labor Day.
    labor_day = tr("Hari Pekerja")

    # Malaysia Cup Holiday.
    malaysia_cup_holiday = tr("Cuti Piala Malaysia")

    special_public_holidays = {
        1999: (NOV, 29, general_election_additional_holiday),
        2017: (
            # Day of Installation of the 15th Yang di-Pertuan Agong.
            (APR, 24, tr("Hari Pertabalan Yang di-Pertuan Agong ke-15")),
            # Additional holiday in commemoration of the 2017 SEA Games.
            (SEP, 4, tr("Cuti tambahan sempena memperingati SAT 2017")),
        ),
        2018: (MAY, 9, general_election_additional_holiday),
        # Day of Installation of the 16th Yang di-Pertuan Agong.
        2019: (JUL, 30, tr("Hari Pertabalan Yang di-Pertuan Agong ke-16")),
        2022: (
            (NOV, 18, general_election_additional_holiday),
            (NOV, 19, general_election_additional_holiday),
            (NOV, 28, additional_holiday),
        ),
        # Eid al-Fitr (additional holiday).
        2023: (APR, 21, tr("Hari Raya Puasa (pergantian hari)")),
        2025: (SEP, 15, additional_holiday),
    }

    special_01_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 13, additional_holiday),
        ),
    }
    special_01_public_holidays_observed = {
        2022: (MAY, 4, labor_day),
    }

    special_02_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 13, additional_holiday),
        ),
    }
    special_02_public_holidays_observed = {
        2022: (MAY, 4, labor_day),
    }

    special_03_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 13, additional_holiday),
        ),
    }
    special_03_public_holidays_observed = {
        2022: (MAY, 4, labor_day),
    }

    special_04_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_04_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_05_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_05_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_06_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_06_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_07_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_07_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_08_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_08_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_09_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_09_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_10_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
    }
    special_10_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_11_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 13, additional_holiday),
        ),
    }
    special_11_public_holidays_observed = {
        # Arafat Day.
        2007: (JAN, 2, tr("Hari Arafah")),
        2022: (MAY, 4, labor_day),
    }

    special_12_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_13_public_holidays = {
        2018: (
            (MAY, 17, additional_holiday),
            (MAY, 18, additional_holiday),
        ),
    }
    special_13_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_14_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
        2021: (DEC, 3, malaysia_cup_holiday),
    }
    special_14_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_15_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
        2021: (DEC, 3, malaysia_cup_holiday),
    }
    special_15_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }

    special_16_public_holidays = {
        2018: (
            (MAY, 10, additional_holiday),
            (MAY, 11, additional_holiday),
        ),
        2021: (DEC, 3, malaysia_cup_holiday),
    }
    special_16_public_holidays_observed = {
        2007: (JAN, 2, eid_al_adha),
    }
