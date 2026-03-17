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

from holidays.countries.france import France
from holidays.mixins.child_entity import ChildEntity


class Reunion(ChildEntity, France):
    """Réunion holidays.

    Alias of a French subdivision that is also officially assigned
    its own country code in ISO 3166-1.

    References:
        * <https://en.wikipedia.org/wiki/Réunion>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_France>
    """

    country = "RE"
    parent_entity = France
    parent_entity_subdivision_code = "974"
    # Cession from the UK on May 30th, 1814.
    start_year = 1815


class HolidaysRE(Reunion):
    pass


class RE(Reunion):
    pass


class REU(Reunion):
    pass
