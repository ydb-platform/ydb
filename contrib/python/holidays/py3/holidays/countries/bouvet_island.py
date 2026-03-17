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

from holidays.no_holiday_base import NoHolidayBase


class BouvetIsland(NoHolidayBase):
    """Bouvet Island holidays.

    References:
        * <https://en.wikipedia.org/wiki/Bouvet_Island>
    """

    country = "BV"


class BV(BouvetIsland):
    pass


class BVT(BouvetIsland):
    pass
