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

from holidays.groups import IslamicHolidays
from holidays.holiday_base import HolidayBase


class WesternSahara(HolidayBase, IslamicHolidays):
    """Western Sahara holidays.

    References:
        * [1999 Constitution (es)](https://web.archive.org/web/20240210155831/https://www.usc.es/export9/sites/webinstitucional/gl/institutos/ceso/descargas/Constitucion-RASD_1999_es.pdf)
        * [1999 Constitution (fr)](https://web.archive.org/web/20250824061216/https://www.arso.org/03-const.99.htm)
        * [2003 Constitution (ar)](https://web.archive.org/web/20240415014236/https://www.usc.es/export9/sites/webinstitucional/gl/institutos/ceso/descargas/Constitucion-RASD_2003_ar.pdf)
        * [2011 Constitution (es)](https://web.archive.org/web/20240415014236/https://www.usc.es/export9/sites/webinstitucional/gl/institutos/ceso/descargas/RASD_Constitucion_2011_es.pdf)
        * [2015 Constitution (en)](https://web.archive.org/web/20240605212800/https://www.usc.es/export9/sites/webinstitucional/gl/institutos/ceso/descargas/RASD_Constitution-of-SADR-2015_en.pdf)
        * [2019 Constitution (ar)](https://web.archive.org/web/20240415014236/https://www.usc.es/export9/sites/webinstitucional/gl/institutos/ceso/descargas/RASD_Const_2019_ar.pdf)
    """

    country = "EH"
    default_language = "ar"
    # %s (estimated).
    estimated_label = tr("%s (المقدرة)")
    # 1999 Constitution was adopted by the 10th Congress on September 4th, 1999.
    start_year = 2000
    supported_languages = ("ar", "en_US", "es", "fr")

    def __init__(self, *args, islamic_show_estimated: bool = True, **kwargs):
        """
        Args:
            islamic_show_estimated:
                Whether to add "estimated" label to Islamic holidays name
                if holiday date is estimated.
        """
        IslamicHolidays.__init__(self, show_estimated=islamic_show_estimated)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Proclamation of the SADR.
        self._add_holiday_feb_27(tr("إعلان الجمهورية العربية الصحراوية الديمقراطية"))

        # First Martyr.
        self._add_holiday_mar_8(tr("يوم الشهيد الأول"))

        # Creation of the Polisario Front.
        self._add_holiday_may_10(tr("تأسيس الجبهة الشعبية لتحرير الساقية الحمراء ووادي الذهب"))

        # Commencement of the Armed Struggle.
        self._add_holiday_may_20(tr("اندلاع الكفاح المسلح"))

        # Martyrs' Day.
        self._add_holiday_jun_9(tr("يوم الشهداء"))

        # Uprising Day.
        self._add_holiday_jun_17(tr("يوم الانتفاضة"))

        # National Unity Day.
        self._add_holiday_oct_12(tr("عيد الوحدة الوطنية"))

        # Added in the 2003 Constitution on October 19th, 2003.
        if self._year >= 2003:
            # Eid al-Fitr.
            self._add_eid_al_fitr_day(tr("عيد الفطر المبارك"))

            if self._year >= 2004:
                # Islamic New Year.
                self._add_islamic_new_year_day(tr("رأس السنة الهجرية"))

                # Prophet's Birthday.
                self._add_mawlid_day(tr("المولد النبوي الشريف"))

                # Eid al-Adha.
                self._add_eid_al_adha_day(tr("عيد الأضحى المبارك"))


class EH(WesternSahara):
    pass


class ESH(WesternSahara):
    pass
