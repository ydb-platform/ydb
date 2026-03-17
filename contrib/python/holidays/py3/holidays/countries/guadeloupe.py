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


class Guadeloupe(ChildEntity, France):
    """Guadeloupe holidays.

    Alias of a French subdivision that is also officially assigned
    its own country code in ISO 3166-1.

    References:
        * <https://en.wikipedia.org/wiki/Guadeloupe>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_France>
    """

    country = "GP"
    parent_entity = France
    parent_entity_subdivision_code = "971"
    # Cession from Sweden on May 30th, 1814.
    start_year = 1815


class HolidaysGP(Guadeloupe):
    pass


class GP(Guadeloupe):
    pass


class GLP(Guadeloupe):
    pass
