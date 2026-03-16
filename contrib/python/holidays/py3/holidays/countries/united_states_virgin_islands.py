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

from holidays.constants import PUBLIC, UNOFFICIAL
from holidays.countries.united_states import UnitedStates
from holidays.mixins.child_entity import ChildEntity


class UnitedStatesVirginIslands(ChildEntity, UnitedStates):
    """United States Virgin Islands (the) holidays.

    Alias of a US subdivision that is also officially assigned its own country code in ISO 3166-1.
    See <https://en.wikipedia.org/wiki/ISO_3166-2:US#Subdivisions_included_in_ISO_3166-1>
    """

    country = "VI"
    parent_entity = UnitedStates
    # Became a U.S. Territory since March 31st, 1917.
    start_year = 1918
    supported_categories = (PUBLIC, UNOFFICIAL)


class HolidaysVI(UnitedStatesVirginIslands):
    pass


class VI(UnitedStatesVirginIslands):
    pass


class VIR(UnitedStatesVirginIslands):
    pass
