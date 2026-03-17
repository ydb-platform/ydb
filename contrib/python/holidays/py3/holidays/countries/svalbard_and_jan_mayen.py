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

from holidays.countries.norway import Norway
from holidays.mixins.child_entity import ChildEntity


class SvalbardAndJanMayen(ChildEntity, Norway):
    """Svalbard and Jan Mayen holidays.

    Alias of Norwegian subdivisions that are also officially assigned
    its own country code in ISO 3166-1.

    References:
        * <https://en.wikipedia.org/wiki/Public_holidays_in_Svalbard>
    """

    country = "SJ"
    parent_entity = Norway
    parent_entity_subdivision_code = "21"


class HolidaysSJ(SvalbardAndJanMayen):
    pass


class SJ(SvalbardAndJanMayen):
    pass


class SJM(SvalbardAndJanMayen):
    pass
