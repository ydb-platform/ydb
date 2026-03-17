"""Enumerations."""

from enum import Enum


class WktVersion(Enum):
    """
     .. versionadded:: 1.9.0

    Supported CRS WKT string versions.
    """

    #: WKT Version 2 from 2015
    WKT2_2015 = "WKT2_2015"
    #: Alias for latest WKT Version 2
    WKT2 = "WKT2"
    #: WKT Version 2 from 2019
    WKT2_2019 = "WKT2_2018"
    #: WKT Version 1 GDAL Style
    WKT1_GDAL = "WKT1_GDAL"
    #: Alias for WKT Version 1 GDAL Style
    WKT1 = "WKT1"
    #: WKT Version 1 ESRI Style
    WKT1_ESRI = "WKT1_ESRI"

    @classmethod
    def _missing_(cls, value):
        if value == "WKT2_2019":
            # WKT2_2019 alias added in GDAL 3.2, use WKT2_2018 for compatibility
            return WktVersion.WKT2_2019
        raise ValueError(f"Invalid value for WktVersion: {value}")
