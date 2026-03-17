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

from holidays.countries.finland import Finland
from holidays.mixins.child_entity import ChildEntity


class AlandIslands(ChildEntity, Finland):
    """Åland Islands holidays.

    Alias of a Finnish subdivision that is also officially assigned
    its own country code in ISO 3166-1.

    !!! note "Note"
        Åland's Autonomy Day is currently added in Finland's implementation.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Åland>
        * <https://en.wikipedia.org/wiki/Åland's_Autonomy_Day>
    """

    country = "AX"
    parent_entity = Finland
    parent_entity_subdivision_code = "01"
    # Åland Islands got its autonomy on May 7th, 1920.
    start_year = 1921


class HolidaysAX(AlandIslands):
    pass


class AX(AlandIslands):
    pass


class ALA(AlandIslands):
    pass
