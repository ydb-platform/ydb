"""
This module contains enumerations used in pyproj.
"""
import sys

from pyproj.compat import string_types

if sys.version_info >= (3, 6):
    from enum import Enum
else:
    # _missing_ was only added in Python 3.6, using aenum for older versions
    from aenum import Enum


class BaseEnum(Enum):
    @classmethod
    def _missing_(cls, item):
        if isinstance(item, string_types):
            item = item.upper()
        for member in cls:
            if member.value == item:
                return member
        raise ValueError(
            "Invalid value supplied '{}'. "
            "Only {} are supported.".format(
                item, tuple(version.value for version in cls)
            )
        )


class WktVersion(BaseEnum):
    """
    Supported CRS WKT string versions
    """

    #: WKT Version 2 from 2015
    WKT2_2015 = "WKT2_2015"
    #: WKT Version 2 from 2015 Simplified
    WKT2_2015_SIMPLIFIED = "WKT2_2015_SIMPLIFIED"
    #: WKT Version 2 from 2018
    WKT2_2018 = "WKT2_2018"
    #: WKT Version 2 from 2018 Simplified
    WKT2_2018_SIMPLIFIED = "WKT2_2018_SIMPLIFIED"
    #: WKT Version 1 GDAL Style
    WKT1_GDAL = "WKT1_GDAL"
    #: WKT Version 1 ESRI Style
    WKT1_ESRI = "WKT1_ESRI"


class ProjVersion(BaseEnum):
    """
    Supported CRS PROJ string versions
    """

    #: PROJ String version 4
    PROJ_4 = 4
    #: PROJ String version 5
    PROJ_5 = 5


class TransformDirection(BaseEnum):
    """
    Supported transform directions
    """

    #: Forward direction
    FORWARD = "FORWARD"
    #: Inverse direction
    INVERSE = "INVERSE"
    #: Do nothing
    IDENT = "IDENT"
