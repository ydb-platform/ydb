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
    MON,
    TUE,
    WED,
    THU,
    FRI,
    SAT,
    SUN,
    WEEKEND,
)

HOLIDAY_NAME_DELIMITER = "; "  # Holiday names separator.

# Supported holiday categories.
ARMED_FORCES = "armed_forces"
BANK = "bank"
DE_FACTO = "de_facto"
GOVERNMENT = "government"
HALF_DAY = "half_day"
OPTIONAL = "optional"
PUBLIC = "public"
SCHOOL = "school"
UNOFFICIAL = "unofficial"
WORKDAY = "workday"

CATHOLIC = "catholic"
CHINESE = "chinese"
CHRISTIAN = "christian"
HEBREW = "hebrew"
HINDU = "hindu"
ISLAMIC = "islamic"
ORTHODOX = "orthodox"
SABIAN = "sabian"
YAZIDI = "yazidi"

ALBANIAN = "albanian"
ARMENIAN = "armenian"
BOSNIAN = "bosnian"
ROMA = "roma"
SERBIAN = "serbian"
TURKISH = "turkish"
VLACH = "vlach"

DEFAULT_START_YEAR = 1901
DEFAULT_END_YEAR = 2100
