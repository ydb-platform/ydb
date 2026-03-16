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
)
from holidays.groups import IslamicHolidays, PersianCalendarHolidays
from holidays.holiday_base import HolidayBase


class Iran(HolidayBase, IslamicHolidays, PersianCalendarHolidays):
    """Iran holidays.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Iran>
        * <https://fa.wikipedia.org/wiki/تعطیلات_عمومی_در_ایران>
        * <https://web.archive.org/web/20250426102648/https://www.time.ir/>
        * <https://web.archive.org/web/20170222200759/http://www.hvm.ir/LawDetailNews.aspx?id=9017>
        * <https://en.wikipedia.org/wiki/Workweek_and_weekend>
    """

    country = "IR"
    default_language = "fa_IR"
    # %s (estimated).
    estimated_label = tr("%s (تخمینی)")
    supported_languages = ("en_US", "fa_IR")
    start_year = 1980
    weekend = {FRI}

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.

        In Iran, the dates of the Islamic calendar usually fall a day later than
        the corresponding dates in the Umm al-Qura calendar.
        """
        IslamicHolidays.__init__(
            self,
            cls=IranIslamicHolidays,
            show_estimated=islamic_show_estimated,
            calendar_delta_days=+1,
        )
        PersianCalendarHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Persian calendar holidays.

        # Islamic Revolution Day.
        self._add_islamic_revolution_day(tr("پیروزی انقلاب اسلامی"))

        # Iranian Oil Industry Nationalization Day.
        self._add_oil_nationalization_day(tr("روز ملی شدن صنعت نفت ایران"))

        # Last Day of Year.
        self._add_last_day_of_year(tr("آخرین روز سال"))

        # Nowruz.
        self._add_nowruz_day(tr("جشن نوروز"))

        # Nowruz Holiday.
        name = tr("عیدنوروز")
        self._add_nowruz_day_two(name)
        self._add_nowruz_day_three(name)
        self._add_nowruz_day_four(name)

        # Islamic Republic Day.
        self._add_islamic_republic_day(tr("روز جمهوری اسلامی"))

        # Nature's Day.
        self._add_natures_day(tr("روز طبیعت"))

        # Death of Imam Khomeini.
        self._add_death_of_khomeini_day(tr("رحلت حضرت امام خمینی"))

        # 15 Khordad Uprising.
        self._add_khordad_uprising_day(tr("قیام 15 خرداد"))

        # Islamic holidays.

        # Tasua.
        self._add_tasua_day(tr("تاسوعای حسینی"))

        # Ashura.
        self._add_ashura_day(tr("عاشورای حسینی"))

        # Arbaeen.
        self._add_arbaeen_day(tr("اربعین حسینی"))

        # Death of Prophet Muhammad and Martyrdom of Hasan ibn Ali.
        self._add_prophet_death_day(tr("رحلت رسول اکرم؛شهادت امام حسن مجتبی علیه السلام"))

        # Martyrdom of Ali al-Rida.
        self._add_ali_al_rida_death_day(tr("شهادت امام رضا علیه السلام"))

        # Martyrdom of Hasan al-Askari.
        self._add_hasan_al_askari_death_day(tr("شهادت امام حسن عسکری علیه السلام"))

        # Birthday of Muhammad and Imam Ja'far al-Sadiq.
        self._add_sadiq_birthday_day(tr("میلاد رسول اکرم و امام جعفر صادق علیه السلام"))

        # Martyrdom of Fatima.
        self._add_fatima_death_day(tr("شهادت حضرت فاطمه زهرا سلام الله علیها"))

        # Birthday of Imam Ali.
        self._add_ali_birthday_day(tr("ولادت امام علی علیه السلام و روز پدر"))

        # Isra' and Mi'raj.
        self._add_isra_and_miraj_day(tr("مبعث رسول اکرم (ص)"))

        self._add_imam_mahdi_birthday_day(
            # Birthday of Mahdi.
            tr("ولادت حضرت قائم عجل الله تعالی فرجه و جشن نیمه شعبان")
        )

        # Martyrdom of Imam Ali.
        self._add_ali_death_day(tr("شهادت حضرت علی علیه السلام"))

        # Eid al-Fitr.
        self._add_eid_al_fitr_day(tr("عید سعید فطر"))

        # Eid al-Fitr Holiday.
        self._add_eid_al_fitr_day_two(tr("تعطیل به مناسبت عید سعید فطر"))

        # Martyrdom of Imam Ja'far al-Sadiq.
        self._add_sadiq_death_day(tr("شهادت امام جعفر صادق علیه السلام"))

        # Eid al-Adha.
        self._add_eid_al_adha_day(tr("عید سعید قربان"))

        # Eid al-Ghadeer.
        self._add_eid_al_ghadir_day(tr("عید سعید غدیر خم"))


class IR(Iran):
    pass


class IRN(Iran):
    pass


class IranIslamicHolidays(_CustomIslamicHolidays):
    ALI_AL_RIDA_DEATH_DATES_CONFIRMED_YEARS = (2001, 2025)
    ALI_AL_RIDA_DEATH_DATES = {
        2005: (APR, 9),
        2008: (MAR, 8),
        2013: (JAN, 12),
        2018: (NOV, 8),
        2020: (OCT, 17),
    }

    ALI_BIRTHDAY_DATES_CONFIRMED_YEARS = (2001, 2025)
    ALI_BIRTHDAY_DATES = {
        2003: (SEP, 10),
        2008: (JUL, 16),
        2009: (JUL, 6),
        2015: (MAY, 2),
        2019: (MAR, 20),
        2020: (MAR, 8),
        2021: (FEB, 25),
        2023: (FEB, 4),
        2024: (JAN, 25),
    }

    ALI_DEATH_DATES_CONFIRMED_YEARS = (2001, 2025)
    ALI_DEATH_DATES = {
        2007: (OCT, 3),
        2009: (SEP, 11),
        2011: (AUG, 21),
        2015: (JUL, 8),
        2017: (JUN, 16),
        2023: (APR, 12),
    }

    ARBAEEN_DATES_CONFIRMED_YEARS = (2001, 2025)
    ARBAEEN_DATES = {
        2001: (MAY, 14),
        2002: (MAY, 3),
        2007: (MAR, 10),
        2012: (JAN, 14),
        2013: ((JAN, 3), (DEC, 23)),
        2015: (DEC, 2),
        2016: (NOV, 20),
        2017: (NOV, 9),
        2019: (OCT, 19),
        2021: (SEP, 27),
        2025: (AUG, 14),
    }

    ASHURA_DATES_CONFIRMED_YEARS = (2001, 2025)
    ASHURA_DATES = {
        2006: (FEB, 9),
        2008: (JAN, 19),
        2009: ((JAN, 7), (DEC, 27)),
        2010: (DEC, 16),
        2018: (SEP, 20),
        2022: (AUG, 8),
        2023: (JUL, 28),
        2024: (JUL, 16),
    }

    EID_AL_ADHA_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_ADHA_DATES = {
        2005: (JAN, 21),
        2006: ((JAN, 11), (DEC, 31)),
        2012: (OCT, 26),
        2017: (SEP, 1),
        2020: (JUL, 31),
        2025: (JUN, 6),
    }

    EID_AL_FITR_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_FITR_DATES = {
        2001: (DEC, 16),
        2004: (NOV, 14),
        2007: (OCT, 13),
        2008: (OCT, 1),
        2009: (SEP, 20),
        2010: (SEP, 10),
        2012: (AUG, 19),
        2016: (JUL, 6),
        2018: (JUN, 15),
        2020: (MAY, 24),
        2021: (MAY, 13),
        2024: (APR, 10),
    }

    EID_AL_GHADIR_DATES_CONFIRMED_YEARS = (2001, 2025)
    EID_AL_GHADIR_DATES = {
        2005: (JAN, 29),
        2007: ((JAN, 8), (DEC, 29)),
        2012: (NOV, 3),
        2017: (SEP, 9),
        2020: (AUG, 8),
        2025: (JUN, 14),
    }

    FATIMA_DEATH_DATES_CONFIRMED_YEARS = (2001, 2025)
    FATIMA_DEATH_DATES = {
        2002: (AUG, 12),
        2006: (JUN, 29),
        2007: (JUN, 18),
        2008: (JUN, 7),
        2010: (MAY, 17),
        2014: (APR, 3),
        2017: (MAR, 2),
        2022: ((JAN, 6), (DEC, 27)),
        2025: (NOV, 24),
    }

    HASAN_AL_ASKARI_DEATH_DATES_CONFIRMED_YEARS = (2001, 2025)
    HASAN_AL_ASKARI_DEATH_DATES = {
        2005: (APR, 17),
        2008: (MAR, 16),
        2013: (JAN, 20),
        2018: (NOV, 16),
        2020: (OCT, 25),
    }

    IMAM_MAHDI_BIRTHDAY_DATES_CONFIRMED_YEARS = (2001, 2025)
    IMAM_MAHDI_BIRTHDAY_DATES = {
        2004: (OCT, 1),
        2010: (JUL, 27),
        2012: (JUL, 5),
        2013: (JUN, 24),
        2014: (JUN, 13),
        2016: (MAY, 22),
        2022: (MAR, 18),
        2024: (FEB, 25),
        2025: (FEB, 14),
    }

    ISRA_AND_MIRAJ_DATES_CONFIRMED_YEARS = (2001, 2025)
    ISRA_AND_MIRAJ_DATES = {
        2003: (SEP, 24),
        2008: (JUL, 30),
        2009: (JUL, 20),
        2015: (MAY, 16),
        2019: (APR, 3),
        2020: (MAR, 22),
        2021: (MAR, 11),
        2023: (FEB, 18),
        2024: (FEB, 8),
    }

    PROPHET_DEATH_DATES_CONFIRMED_YEARS = (2001, 2025)
    PROPHET_DEATH_DATES = {
        2001: (MAY, 22),
        2002: (MAY, 11),
        2007: (MAR, 18),
        2012: (JAN, 22),
        2013: ((JAN, 11), (DEC, 31)),
        2015: (DEC, 10),
        2016: (NOV, 28),
        2017: (NOV, 17),
        2019: (OCT, 27),
        2021: (OCT, 5),
        2025: (AUG, 22),
    }

    SADIQ_BIRTHDAY_DATES_CONFIRMED_YEARS = (2001, 2025)
    SADIQ_BIRTHDAY_DATES = {
        2005: (APR, 26),
        2008: (MAR, 25),
        2013: (JAN, 29),
        2018: (NOV, 25),
        2020: (NOV, 3),
    }

    SADIQ_DEATH_DATES_CONFIRMED_YEARS = (2001, 2025)
    SADIQ_DEATH_DATES = {
        2001: (JAN, 20),
        2002: ((JAN, 9), (DEC, 30)),
        2004: (DEC, 8),
        2007: (NOV, 6),
        2008: (OCT, 25),
        2009: (OCT, 14),
        2010: (OCT, 4),
        2012: (SEP, 12),
        2016: (JUL, 30),
        2018: (JUL, 9),
        2020: (JUN, 17),
        2021: (JUN, 6),
        2024: (MAY, 4),
    }

    TASUA_DATES_CONFIRMED_YEARS = (2001, 2025)
    TASUA_DATES = {
        2006: (FEB, 8),
        2008: (JAN, 18),
        2009: ((JAN, 6), (DEC, 26)),
        2010: (DEC, 15),
        2018: (SEP, 19),
        2022: (AUG, 7),
        2023: (JUL, 27),
        2024: (JUL, 15),
    }
