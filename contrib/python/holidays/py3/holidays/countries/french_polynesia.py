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


class FrenchPolynesia(ChildEntity, France):
    """French Polynesia holidays.

    Alias of a French subdivision that is also officially assigned
    its own country code in ISO 3166-1.

    References:
        * <https://en.wikipedia.org/wiki/French_Polynesia>
        * <https://en.wikipedia.org/wiki/Public_holidays_in_France>
    """

    country = "PF"
    parent_entity = France
    # Pōmare V abdicated and Tahiti became a French Colony on June 29th, 1880.
    start_year = 1881


class HolidaysPF(FrenchPolynesia):
    pass


class PF(FrenchPolynesia):
    pass


class PYF(FrenchPolynesia):
    pass
