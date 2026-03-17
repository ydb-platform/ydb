"""
This module is for building coordinate systems to be used when
building a CRS.
"""

from pyproj._crs import CoordinateSystem
from pyproj.crs.enums import (
    Cartesian2DCSAxis,
    Ellipsoidal2DCSAxis,
    Ellipsoidal3DCSAxis,
    VerticalCSAxis,
)

# useful constants to use when setting PROJ JSON units
UNIT_METRE = "metre"
UNIT_DEGREE = "degree"
UNIT_FT = {"type": "LinearUnit", "name": "foot", "conversion_factor": 0.3048}
UNIT_US_FT = {
    "type": "LinearUnit",
    "name": "US survey foot",
    "conversion_factor": 0.304800609601219,
}

_ELLIPSOIDAL_2D_AXIS_MAP = {
    Ellipsoidal2DCSAxis.LONGITUDE_LATITUDE: [
        {
            "name": "Longitude",
            "abbreviation": "lon",
            "direction": "east",
            "unit": UNIT_DEGREE,
        },
        {
            "name": "Latitude",
            "abbreviation": "lat",
            "direction": "north",
            "unit": UNIT_DEGREE,
        },
    ],
    Ellipsoidal2DCSAxis.LATITUDE_LONGITUDE: [
        {
            "name": "Latitude",
            "abbreviation": "lat",
            "direction": "north",
            "unit": UNIT_DEGREE,
        },
        {
            "name": "Longitude",
            "abbreviation": "lon",
            "direction": "east",
            "unit": UNIT_DEGREE,
        },
    ],
}


class Ellipsoidal2DCS(CoordinateSystem):
    """
    .. versionadded:: 2.5.0

    This generates an Ellipsoidal 2D Coordinate System
    """

    def __new__(
        cls,
        axis: Ellipsoidal2DCSAxis | str = Ellipsoidal2DCSAxis.LONGITUDE_LATITUDE,
    ):
        """
        Parameters
        ----------
        axis: :class:`pyproj.crs.enums.Ellipsoidal2DCSAxis` or str, optional
            This is the axis order of the coordinate system. Default is
            :attr:`pyproj.crs.enums.Ellipsoidal2DCSAxis.LONGITUDE_LATITUDE`.
        """
        return cls.from_json_dict(
            {
                "type": "CoordinateSystem",
                "subtype": "ellipsoidal",
                "axis": _ELLIPSOIDAL_2D_AXIS_MAP[Ellipsoidal2DCSAxis.create(axis)],
            }
        )


_ELLIPSOIDAL_3D_AXIS_MAP = {
    Ellipsoidal3DCSAxis.LONGITUDE_LATITUDE_HEIGHT: [
        {
            "name": "Longitude",
            "abbreviation": "lon",
            "direction": "east",
            "unit": UNIT_DEGREE,
        },
        {
            "name": "Latitude",
            "abbreviation": "lat",
            "direction": "north",
            "unit": UNIT_DEGREE,
        },
        {
            "name": "Ellipsoidal height",
            "abbreviation": "h",
            "direction": "up",
            "unit": UNIT_METRE,
        },
    ],
    Ellipsoidal3DCSAxis.LATITUDE_LONGITUDE_HEIGHT: [
        {
            "name": "Latitude",
            "abbreviation": "lat",
            "direction": "north",
            "unit": UNIT_DEGREE,
        },
        {
            "name": "Longitude",
            "abbreviation": "lon",
            "direction": "east",
            "unit": UNIT_DEGREE,
        },
        {
            "name": "Ellipsoidal height",
            "abbreviation": "h",
            "direction": "up",
            "unit": UNIT_METRE,
        },
    ],
}


class Ellipsoidal3DCS(CoordinateSystem):
    """
    .. versionadded:: 2.5.0

    This generates an Ellipsoidal 3D Coordinate System
    """

    def __new__(
        cls,
        axis: Ellipsoidal3DCSAxis | str = Ellipsoidal3DCSAxis.LONGITUDE_LATITUDE_HEIGHT,
    ):
        """
        Parameters
        ----------
        axis: :class:`pyproj.crs.enums.Ellipsoidal3DCSAxis` or str, optional
            This is the axis order of the coordinate system. Default is
            :attr:`pyproj.crs.enums.Ellipsoidal3DCSAxis.LONGITUDE_LATITUDE_HEIGHT`.
        """
        return cls.from_json_dict(
            {
                "type": "CoordinateSystem",
                "subtype": "ellipsoidal",
                "axis": _ELLIPSOIDAL_3D_AXIS_MAP[Ellipsoidal3DCSAxis.create(axis)],
            }
        )


_CARTESIAN_2D_AXIS_MAP = {
    Cartesian2DCSAxis.EASTING_NORTHING: [
        {
            "name": "Easting",
            "abbreviation": "E",
            "direction": "east",
            "unit": UNIT_METRE,
        },
        {
            "name": "Northing",
            "abbreviation": "N",
            "direction": "north",
            "unit": UNIT_METRE,
        },
    ],
    Cartesian2DCSAxis.NORTHING_EASTING: [
        {
            "name": "Northing",
            "abbreviation": "N",
            "direction": "north",
            "unit": UNIT_METRE,
        },
        {
            "name": "Easting",
            "abbreviation": "E",
            "direction": "east",
            "unit": UNIT_METRE,
        },
    ],
    Cartesian2DCSAxis.EASTING_NORTHING_FT: [
        {"name": "Easting", "abbreviation": "X", "direction": "east", "unit": UNIT_FT},
        {
            "name": "Northing",
            "abbreviation": "Y",
            "direction": "north",
            "unit": UNIT_FT,
        },
    ],
    Cartesian2DCSAxis.NORTHING_EASTING_FT: [
        {
            "name": "Northing",
            "abbreviation": "Y",
            "direction": "north",
            "unit": UNIT_FT,
        },
        {"name": "Easting", "abbreviation": "X", "direction": "east", "unit": UNIT_FT},
    ],
    Cartesian2DCSAxis.EASTING_NORTHING_US_FT: [
        {
            "name": "Easting",
            "abbreviation": "X",
            "direction": "east",
            "unit": UNIT_US_FT,
        },
        {
            "name": "Northing",
            "abbreviation": "Y",
            "direction": "north",
            "unit": UNIT_US_FT,
        },
    ],
    Cartesian2DCSAxis.NORTHING_EASTING_US_FT: [
        {
            "name": "Northing",
            "abbreviation": "Y",
            "direction": "north",
            "unit": UNIT_US_FT,
        },
        {
            "name": "Easting",
            "abbreviation": "X",
            "direction": "east",
            "unit": UNIT_US_FT,
        },
    ],
    Cartesian2DCSAxis.NORTH_POLE_EASTING_SOUTH_NORTHING_SOUTH: [
        {
            "name": "Easting",
            "abbreviation": "E",
            "direction": "south",
            "unit": UNIT_METRE,
        },
        {
            "name": "Northing",
            "abbreviation": "N",
            "direction": "south",
            "unit": UNIT_METRE,
        },
    ],
    Cartesian2DCSAxis.SOUTH_POLE_EASTING_NORTH_NORTHING_NORTH: [
        {
            "name": "Easting",
            "abbreviation": "E",
            "direction": "north",
            "unit": UNIT_METRE,
        },
        {
            "name": "Northing",
            "abbreviation": "N",
            "direction": "north",
            "unit": UNIT_METRE,
        },
    ],
    Cartesian2DCSAxis.WESTING_SOUTHING: [
        {
            "name": "Easting",
            "abbreviation": "Y",
            "direction": "west",
            "unit": UNIT_METRE,
        },
        {
            "name": "Northing",
            "abbreviation": "X",
            "direction": "south",
            "unit": UNIT_METRE,
        },
    ],
}


class Cartesian2DCS(CoordinateSystem):
    """
    .. versionadded:: 2.5.0

    This generates an Cartesian 2D Coordinate System
    """

    def __new__(
        cls, axis: Cartesian2DCSAxis | str = Cartesian2DCSAxis.EASTING_NORTHING
    ):
        """
        Parameters
        ----------
        axis: :class:`pyproj.crs.enums.Cartesian2DCSAxis` or str, optional
            This is the axis order of the coordinate system.
            Default is :attr:`pyproj.crs.enums.Cartesian2DCSAxis.EASTING_NORTHING`.
        """
        return cls.from_json_dict(
            {
                "type": "CoordinateSystem",
                "subtype": "Cartesian",
                "axis": _CARTESIAN_2D_AXIS_MAP[Cartesian2DCSAxis.create(axis)],
            }
        )


_VERTICAL_AXIS_MAP = {
    VerticalCSAxis.GRAVITY_HEIGHT: {
        "name": "Gravity-related height",
        "abbreviation": "H",
        "direction": "up",
        "unit": UNIT_METRE,
    },
    VerticalCSAxis.GRAVITY_HEIGHT_US_FT: {
        "name": "Gravity-related height",
        "abbreviation": "H",
        "direction": "up",
        "unit": UNIT_US_FT,
    },
    VerticalCSAxis.GRAVITY_HEIGHT_FT: {
        "name": "Gravity-related height",
        "abbreviation": "H",
        "direction": "up",
        "unit": UNIT_FT,
    },
    VerticalCSAxis.DEPTH: {
        "name": "Depth",
        "abbreviation": "D",
        "direction": "down",
        "unit": UNIT_METRE,
    },
    VerticalCSAxis.DEPTH_US_FT: {
        "name": "Depth",
        "abbreviation": "D",
        "direction": "down",
        "unit": UNIT_US_FT,
    },
    VerticalCSAxis.DEPTH_FT: {
        "name": "Depth",
        "abbreviation": "D",
        "direction": "down",
        "unit": UNIT_FT,
    },
    VerticalCSAxis.UP: {
        "name": "up",
        "abbreviation": "H",
        "direction": "up",
        "unit": UNIT_METRE,
    },
    VerticalCSAxis.UP_FT: {
        "name": "up",
        "abbreviation": "H",
        "direction": "up",
        "unit": UNIT_FT,
    },
    VerticalCSAxis.UP_US_FT: {
        "name": "up",
        "abbreviation": "H",
        "direction": "up",
        "unit": UNIT_US_FT,
    },
}


class VerticalCS(CoordinateSystem):
    """
    .. versionadded:: 2.5.0

    This generates an Vertical Coordinate System
    """

    def __new__(cls, axis: VerticalCSAxis | str = VerticalCSAxis.GRAVITY_HEIGHT):
        """
        Parameters
        ----------
        axis: :class:`pyproj.crs.enums.VerticalCSAxis` or str, optional
            This is the axis direction of the coordinate system.
            Default is :attr:`pyproj.crs.enums.VerticalCSAxis.GRAVITY_HEIGHT`.
        """
        return cls.from_json_dict(
            {
                "type": "CoordinateSystem",
                "subtype": "vertical",
                "axis": [_VERTICAL_AXIS_MAP[VerticalCSAxis.create(axis)]],
            }
        )
