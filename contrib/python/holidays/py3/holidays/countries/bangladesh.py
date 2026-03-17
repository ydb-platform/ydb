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

from holidays.calendars.gregorian import FRI, SAT
from holidays.groups import InternationalHolidays
from holidays.holiday_base import HolidayBase


class Bangladesh(HolidayBase, InternationalHolidays):
    """Bangladesh holidays.

    References:
        * <https://web.archive.org/web/20241109215908/https://mopa.gov.bd/sites/default/files/files/mopa.gov.bd/public_holiday/61c35b73_e335_462a_9bcf_4695b23b6d82/reg4-2019-212.PDF>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Bangladesh>
    """

    country = "BD"
    default_language = "bn"
    supported_languages = ("bn", "en_US")
    weekend = {FRI, SAT}

    def __init__(self, *args, **kwargs):
        InternationalHolidays.__init__(self)
        super().__init__(*args, **kwargs)

    def _populate_public_holidays(self):
        # Martyrs' Day and International Mother Language Day.
        self._add_holiday_feb_21(tr("শহীদ দিবস ও আন্তর্জাতিক মাতৃভাষা দিবস"))

        # Sheikh Mujibur Rahman's Birthday.
        self._add_holiday_mar_17(tr("জাতির পিতা বঙ্গবন্ধু শেখ মুজিবুর রহমান এর জন্মদিবস"))

        # Independence Day.
        self._add_holiday_mar_26(tr("স্বাধীনতা দিবস"))

        # Bengali New Year's Day.
        self._add_holiday_apr_14(tr("পহেলা বৈশাখ"))

        # May Day.
        self._add_labor_day(tr("মে দিবস"))

        # National Mourning Day.
        self._add_holiday_aug_15(tr("জাতীয় শোক দিবস"))

        # Victory Day.
        self._add_holiday_dec_16(tr("বিজয় দিবস"))


class BD(Bangladesh):
    pass


class BGD(Bangladesh):
    pass
