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

import warnings
from gettext import gettext as tr

from holidays.calendars import _CustomIslamicHolidays
from holidays.calendars.gregorian import JAN, FEB, MAR, APR, MAY, AUG, SEP, OCT, NOV, DEC
from holidays.constants import OPTIONAL, PUBLIC
from holidays.groups import (
    ChristianHolidays,
    HinduCalendarHolidays,
    InternationalHolidays,
    IslamicHolidays,
)
from holidays.holiday_base import HolidayBase


class India(
    HolidayBase, ChristianHolidays, HinduCalendarHolidays, InternationalHolidays, IslamicHolidays
):
    """India holidays.

    References:
        * <https://web.archive.org/web/20250413193616/https://www.india.gov.in/calendar>
        * <https://web.archive.org/web/20250413193624/https://www.india.gov.in/state-and-ut-holiday-calendar>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_India>
        * <https://web.archive.org/web/20250413193633/https://www.calendarlabs.com/holidays/india/2021>
        * <https://web.archive.org/web/20231118175007/http://slusi.dacnet.nic.in/watershedatlas/list_of_state_abbreviation.htm>
        * <https://web.archive.org/web/20231008063930/https://vahan.parivahan.gov.in/vahan4dashboard/>
        * Gujarat:
            * <https://web.archive.org/web/20260122052040/https://images-gujarati.indianexpress.com/2025/11/gujarat-government-Year-2026-holiday-list.pdf>
        * Tamil Nadu:
            * [Tamil Monthly Calendar](https://web.archive.org/web/20231228103352/https://www.tamildailycalendar.com/tamil_monthly_calendar.php)
            * [Tamil Calendar](https://web.archive.org/web/20250429125140/https://www.prokerala.com/general/calendar/tamilcalendar.php)
    """

    country = "IN"
    default_language = "en_IN"
    # %s (estimated).
    estimated_label = tr("%s (estimated)")
    # India gained independence on August 15, 1947.
    start_year = 1948
    subdivisions = (
        "AN",  # Andaman and Nicobar Islands.
        "AP",  # Andhra Pradesh.
        "AR",  # Arunachal Pradesh (Arunāchal Pradesh).
        "AS",  # Assam.
        "BR",  # Bihar (Bihār).
        "CG",  # Chhattisgarh (Chhattīsgarh).
        "CH",  # Chandigarh (Chandīgarh).
        "DH",  # Dadra and Nagar Haveli and Daman and Diu(Dādra and Nagar Haveli and Damān and Diu)
        "DL",  # Delhi.
        "GA",  # Goa.
        "GJ",  # Gujarat (Gujarāt).
        "HP",  # Himachal Pradesh (Himāchal Pradesh).
        "HR",  # Haryana (Haryāna).
        "JH",  # Jharkhand (Jhārkhand).
        "JK",  # Jammu and Kashmir (Jammu and Kashmīr).
        "KA",  # Karnataka (Karnātaka).
        "KL",  # Kerala.
        "LA",  # Ladakh (Ladākh).
        "LD",  # Lakshadweep.
        "MH",  # Maharashtra (Mahārāshtra).
        "ML",  # Meghalaya (Meghālaya).
        "MN",  # Manipur.
        "MP",  # Madhya Pradesh.
        "MZ",  # Mizoram.
        "NL",  # Nagaland (Nāgāland).
        "OD",  # Odisha.
        "PB",  # Punjab.
        "PY",  # Puducherry.
        "RJ",  # Rajasthan (Rājasthān).
        "SK",  # Sikkim.
        "TN",  # Tamil Nadu (Tamil Nādu).
        "TR",  # Tripura.
        "TS",  # Telangana (Telangāna).
        "UK",  # Uttarakhand (Uttarākhand).
        "UP",  # Uttar Pradesh.
        "WB",  # West Bengal.
    )
    subdivisions_aliases = {
        "Andaman and Nicobar Islands": "AN",
        "Andhra Pradesh": "AP",
        "Arunachal Pradesh": "AR",
        "Arunāchal Pradesh": "AR",
        "Assam": "AS",
        "Bihar": "BR",
        "Bihār": "BR",
        "Chhattisgarh": "CG",
        "Chhattīsgarh": "CG",
        "Chandigarh": "CH",
        "Chandīgarh": "CH",
        "Dadra and Nagar Haveli and Daman and Diu": "DH",
        "Dādra and Nagar Haveli and Damān and Diu": "DH",
        "Delhi": "DL",
        "Goa": "GA",
        "Gujarat": "GJ",
        "Gujarāt": "GJ",
        "Himachal Pradesh": "HP",
        "Himāchal Pradesh": "HP",
        "Haryana": "HR",
        "Haryāna": "HR",
        "Jharkhand": "JH",
        "Jhārkhand": "JH",
        "Jammu and Kashmir": "JK",
        "Jammu and Kashmīr": "JK",
        "Karnataka": "KA",
        "Karnātaka": "KA",
        "Kerala": "KL",
        "Ladakh": "LA",
        "Ladākh": "LA",
        "Lakshadweep": "LD",
        "Maharashtra": "MH",
        "Mahārāshtra": "MH",
        "Meghalaya": "ML",
        "Meghālaya": "ML",
        "Manipur": "MN",
        "Madhya Pradesh": "MP",
        "Mizoram": "MZ",
        "Nagaland": "NL",
        "Nāgāland": "NL",
        "Odisha": "OD",
        "Punjab": "PB",
        "Puducherry": "PY",
        "Rajasthan": "RJ",
        "Rājasthān": "RJ",
        "Sikkim": "SK",
        "Tamil Nadu": "TN",
        "Tamil Nādu": "TN",
        "Tripura": "TR",
        "Telangana": "TS",
        "Telangāna": "TS",
        "Uttarakhand": "UK",
        "Uttarākhand": "UK",
        "Uttar Pradesh": "UP",
        "West Bengal": "WB",
    }
    supported_categories = (OPTIONAL, PUBLIC)
    supported_languages = ("en_IN", "en_US", "gu", "hi")
    _deprecated_subdivisions = (
        "DD",  # Daman and Diu.
        "OR",  # Orissa.
    )

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In India, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        ChristianHolidays.__init__(self)
        HinduCalendarHolidays.__init__(self)
        IslamicHolidays.__init__(
            self,
            cls=IndiaIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        if self._year >= 1950:
            # Republic Day.
            self._add_holiday_jan_26(tr("Republic Day"))

        # Independence Day.
        self._add_holiday_aug_15(tr("Independence Day"))

        # Gandhi Jayanti.
        self._add_holiday_oct_2(tr("Gandhi Jayanti"))

        # Hindu Holidays.
        if self._year < 2001 or self._year > 2035:
            warning_msg = "Requested Holidays are available only from 2001 to 2035."
            warnings.warn(warning_msg, Warning)

        # Buddha Purnima.
        self._add_buddha_purnima(tr("Buddha Purnima"))

        # Diwali.
        self._add_diwali_india(tr("Diwali"))

        # Janmashtami.
        self._add_janmashtami(tr("Janmashtami"))

        # Dussehra.
        self._add_dussehra(tr("Dussehra"))

        # Mahavir Jayanti.
        self._add_mahavir_jayanti(tr("Mahavir Jayanti"))

        # Maha Shivaratri.
        self._add_maha_shivaratri(tr("Maha Shivaratri"))

        # Guru Nanak Jayanti.
        self._add_guru_nanak_jayanti(tr("Guru Nanak Jayanti"))

        # Islamic holidays.

        # Ashura.
        self._add_ashura_day(tr("Muharram"))

        # Prophet's Birthday.
        self._add_mawlid_day(tr("Milad-un-Nabi"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("Id-ul-Fitr"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("Bakrid"))

        # Christian holidays.

        # Good Friday.
        self._add_good_friday(tr("Good Friday"))

        # Christmas.
        self._add_christmas_day(tr("Christmas"))

        if self.subdiv == "OR":
            self._populate_subdiv_od_public_holidays()

    def _populate_optional_holidays(self):
        # Hindu holidays.

        # Children's Day.
        self._add_holiday_nov_14(tr("Children's Day"))

        # Holi.
        self._add_holi(tr("Holi"))

        # Ganesh Chaturthi.
        self._add_ganesh_chaturthi(tr("Ganesh Chaturthi"))

        # Govardhan Puja.
        self._add_govardhan_puja(tr("Govardhan Puja"))

        # Labor Day.
        self._add_labor_day(tr("Labour Day"))

        # Maha Navami.
        self._add_maha_navami(tr("Maha Navami"))

        # Makar Sankranti.
        self._add_makar_sankranti(tr("Makar Sankranti"))

        # Raksha Bandhan.
        self._add_raksha_bandhan(tr("Raksha Bandhan"))

        # Ram Navami.
        self._add_ram_navami(tr("Ram Navami"))

        # Navratri / Sharad Navratri.
        self._add_sharad_navratri(tr("Navratri / Sharad Navratri"))

        # Christian holidays.

        # Easter Sunday.
        self._add_easter_sunday(tr("Easter Sunday"))

        # Palm Sunday.
        self._add_palm_sunday(tr("Palm Sunday"))

    # Andaman and Nicobar Islands.
    def _populate_subdiv_an_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # Andhra Pradesh.
    def _populate_subdiv_ap_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Andhra Pradesh Foundation Day.
        self._add_holiday_nov_1(tr("Andhra Pradesh Foundation Day"))
        # Ugadi.
        self._add_gudi_padwa(tr("Ugadi"))

    # Assam.
    def _populate_subdiv_as_public_holidays(self):
        # Magh Bihu.
        self._add_makar_sankranti(tr("Magh Bihu"))
        # Assam Day.
        self._add_holiday_dec_2(tr("Assam Day"))

    # Bihar.
    def _populate_subdiv_br_public_holidays(self):
        # Chhath Puja.
        self._add_chhath_puja(tr("Chhath Puja"))
        # Bihar Day.
        self._add_holiday_mar_22(tr("Bihar Day"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # Chandigarh.
    def _populate_subdiv_ch_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # Chhattisgarh.
    def _populate_subdiv_cg_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Chhattisgarh Foundation Day.
        self._add_holiday_nov_1(tr("Chhattisgarh Foundation Day"))

    # Delhi.
    def _populate_subdiv_dl_public_holidays(self):
        # Chhath Puja.
        self._add_chhath_puja(tr("Chhath Puja"))

    # Goa.
    def _populate_subdiv_ga_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Goa Liberation Day.
        self._add_holiday_dec_19(tr("Goa Liberation Day"))

    # Gujarat.
    def _populate_subdiv_gj_public_holidays(self):
        # Makar Sankranti.
        self._add_makar_sankranti(tr("Uttarayan"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Gujarat Day.
        self._add_holiday_may_1(tr("Gujarat Day"))
        # Sardar Vallabhbhai Patel Jayanti.
        self._add_holiday_oct_31(tr("Sardar Vallabhbhai Patel Jayanti"))

    # Haryana.
    def _populate_subdiv_hr_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Haryana Foundation Day.
        self._add_holiday_nov_1(tr("Haryana Foundation Day"))

    # Himachal Pradesh.
    def _populate_subdiv_hp_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Himachal Day.
        self._add_holiday_apr_15(tr("Himachal Day"))

    # Jammu and Kashmir
    def _populate_subdiv_jk_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # Jharkhand.
    def _populate_subdiv_jh_public_holidays(self):
        # Chhath Puja.
        self._add_chhath_puja(tr("Chhath Puja"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Jharkhand Formation Day.
        self._add_holiday_nov_15(tr("Jharkhand Formation Day"))

    # Karnataka.
    def _populate_subdiv_ka_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Karnataka Rajyotsav.
        self._add_holiday_nov_1(tr("Karnataka Rajyotsava"))
        # Ugadi.
        self._add_gudi_padwa(tr("Ugadi"))

    # Kerala.
    def _populate_subdiv_kl_public_holidays(self):
        # Onam.
        self._add_onam(tr("Onam"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Kerala Foundation Day.
        self._add_holiday_nov_1(tr("Kerala Foundation Day"))

    # Ladakh.
    def _populate_subdiv_la_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # Maharashtra.
    def _populate_subdiv_mh_public_holidays(self):
        # Gudi Padwa.
        self._add_gudi_padwa(tr("Gudi Padwa"))
        # Chhatrapati Shivaji Maharaj Jayanti.
        self._add_holiday_feb_19(tr("Chhatrapati Shivaji Maharaj Jayanti"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Maharashtra Day.
        self._add_holiday_may_1(tr("Maharashtra Day"))

    # Madhya Pradesh.
    def _populate_subdiv_mp_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Madhya Pradesh Foundation Day.
        self._add_holiday_nov_1(tr("Madhya Pradesh Foundation Day"))

    # Mizoram.
    def _populate_subdiv_mz_public_holidays(self):
        # Mizoram State Day.
        self._add_holiday_feb_20(tr("Mizoram State Day"))

    # Nagaland.
    def _populate_subdiv_nl_public_holidays(self):
        # Nagaland State Inauguration Day.
        self._add_holiday_dec_1(tr("Nagaland State Inauguration Day"))

    # Orissa / Odisha.
    def _populate_subdiv_od_public_holidays(self):
        # Odisha Day.
        self._add_holiday_apr_1(tr("Odisha Day (Utkala Dibasa)"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Maha Vishuva Sankranti.
        self._add_holiday_apr_15(tr("Maha Vishuva Sankranti / Pana Sankranti"))

    # Puducherry.
    def _populate_subdiv_py_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Puducherry De Jure Transfer Day.
        self._add_holiday_aug_16(tr("Puducherry De Jure Transfer Day"))
        # Puducherry Liberation Day.
        self._add_holiday_nov_1(tr("Puducherry Liberation Day"))

    # Punjab.
    def _populate_subdiv_pb_public_holidays(self):
        # Guru Gobind Singh Jayanti.
        self._add_guru_gobind_singh_jayanti(tr("Guru Gobind Singh Jayanti"))
        # Vaisakhi.
        self._add_vaisakhi(tr("Vaisakhi"))
        # Lohri.
        self._add_holiday_jan_13(tr("Lohri"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Punjabi Day.
        self._add_holiday_nov_1(tr("Punjab Day"))

    # Rajasthan.
    def _populate_subdiv_rj_public_holidays(self):
        # Rajasthan Day.
        self._add_holiday_mar_30(tr("Rajasthan Day"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Maharana Pratap Jayanti.
        self._add_maharana_pratap_jayanti(tr("Maharana Pratap Jayanti"))

    # Sikkim.
    def _populate_subdiv_sk_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Sikkim State Day.
        self._add_holiday_may_16(tr("Sikkim State Day"))

    # Tamil Nadu.
    def _populate_subdiv_tn_public_holidays(self):
        # Pongal.
        self._add_pongal(tr("Pongal"))
        # Thiruvalluvar Day / Mattu Pongal.
        self._add_thiruvalluvar_day(tr("Thiruvalluvar Day / Mattu Pongal"))
        # Uzhavar Thirunal.
        self._add_uzhavar_thirunal(tr("Uzhavar Thirunal"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Puthandu.
        self._add_holiday_apr_14(tr("Puthandu (Tamil New Year)"))

    # Telangana.
    def _populate_subdiv_ts_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Telangana Formation Day.
        self._add_holiday_jun_2(tr("Telangana Formation Day"))
        # Bathukamma Festival.
        self._add_bathukamma(tr("Bathukamma Festival"))
        # Ugadi.
        self._add_gudi_padwa(tr("Ugadi"))

    # Uttarakhand.
    def _populate_subdiv_uk_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # Uttar Pradesh.
    def _populate_subdiv_up_public_holidays(self):
        # Chhath Puja.
        self._add_chhath_puja(tr("Chhath Puja"))
        # UP Formation Day.
        self._add_holiday_jan_24(tr("UP Formation Day"))
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))

    # West Bengal.
    def _populate_subdiv_wb_public_holidays(self):
        # Dr. B. R. Ambedkar Jayanti.
        self._add_holiday_apr_14(tr("Dr. B. R. Ambedkar's Jayanti"))
        # Pohela Boisakh.
        self._add_holiday_apr_15(tr("Pohela Boishakh"))
        # Rabindra Jayanti.
        self._add_holiday_may_9(tr("Rabindra Jayanti"))


class IN(India):
    pass


class IND(India):
    pass


class IndiaIslamicHolidays(_CustomIslamicHolidays):
    ASHURA_DATES_CONFIRMED_YEARS = (2001, 2025)
    ASHURA_DATES = {
        2001: (APR, 4),
        2002: (MAR, 24),
        2005: (FEB, 19),
        2006: (FEB, 9),
        2008: (JAN, 19),
        2009: ((JAN, 7), (DEC, 28)),
        2021: (AUG, 20),
    }

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_ADHA_DATES = {
        2005: (JAN, 21),
        2006: ((JAN, 11), (DEC, 31)),
        2007: (DEC, 20),
        2014: (OCT, 6),
        2015: (SEP, 25),
        2016: (SEP, 13),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_FITR_DATES = {
        2004: (NOV, 14),
        2005: (NOV, 3),
        2007: (OCT, 13),
        2010: (SEP, 10),
        2013: (AUG, 8),
    }

    MAWLID_DATES_CONFIRMED_YEARS = (2001, 2025)
    MAWLID_DATES = {
        2003: (MAY, 15),
        2004: (MAY, 3),
        2009: (MAR, 9),
        2015: ((JAN, 4), (DEC, 25)),
        2016: (DEC, 13),
        2017: (DEC, 2),
    }
