"""
This module contains enumerations used in pyproj.crs.
"""

from pyproj.enums import BaseEnum


class DatumType(BaseEnum):
    """
    .. versionadded:: 2.5.0

    Datum Types for creating datum with :meth:`pyproj.crs.Datum.from_name`

    Attributes
    ----------
    GEODETIC_REFERENCE_FRAME
    DYNAMIC_GEODETIC_REFERENCE_FRAME
    VERTICAL_REFERENCE_FRAME
    DYNAMIC_VERTICAL_REFERENCE_FRAME
    DATUM_ENSEMBLE
    """

    GEODETIC_REFERENCE_FRAME = "GEODETIC_REFERENCE_FRAME"
    DYNAMIC_GEODETIC_REFERENCE_FRAME = "DYNAMIC_GEODETIC_REFERENCE_FRAME"
    VERTICAL_REFERENCE_FRAME = "VERTICAL_REFERENCE_FRAME"
    DYNAMIC_VERTICAL_REFERENCE_FRAME = "DYNAMIC_VERTICAL_REFERENCE_FRAME"
    DATUM_ENSEMBLE = "DATUM_ENSEMBLE"


class CoordinateOperationType(BaseEnum):
    """
    .. versionadded:: 2.5.0

    Coordinate Operation Types for creating operation
    with :meth:`pyproj.crs.CoordinateOperation.from_name`

    Attributes
    ----------
    CONVERSION
    TRANSFORMATION
    CONCATENATED_OPERATION
    OTHER_COORDINATE_OPERATION
    """

    CONVERSION = "CONVERSION"
    TRANSFORMATION = "TRANSFORMATION"
    CONCATENATED_OPERATION = "CONCATENATED_OPERATION"
    OTHER_COORDINATE_OPERATION = "OTHER_COORDINATE_OPERATION"


class Cartesian2DCSAxis(BaseEnum):
    """
    .. versionadded:: 2.5.0

    Cartesian 2D Coordinate System Axis for creating axis with
    with :class:`pyproj.crs.coordinate_system.Cartesian2DCS`

    Attributes
    ----------
    EASTING_NORTHING
    NORTHING_EASTING
    EASTING_NORTHING_FT
    NORTHING_EASTING_FT
    EASTING_NORTHING_US_FT
    NORTHING_EASTING_US_FT
    NORTH_POLE_EASTING_SOUTH_NORTHING_SOUTH
    SOUTH_POLE_EASTING_NORTH_NORTHING_NORTH
    WESTING_SOUTHING
    """

    EASTING_NORTHING = "EASTING_NORTHING"
    NORTHING_EASTING = "NORTHING_EASTING"
    EASTING_NORTHING_FT = "EASTING_NORTHING_FT"
    NORTHING_EASTING_FT = "NORTHING_EASTING_FT"
    EASTING_NORTHING_US_FT = "EASTING_NORTHING_US_FT"
    NORTHING_EASTING_US_FT = "NORTHING_EASTING_US_FT"
    NORTH_POLE_EASTING_SOUTH_NORTHING_SOUTH = "NORTH_POLE_EASTING_SOUTH_NORTHING_SOUTH"
    SOUTH_POLE_EASTING_NORTH_NORTHING_NORTH = "SOUTH_POLE_EASTING_NORTH_NORTHING_NORTH"
    WESTING_SOUTHING = "WESTING_SOUTHING"


class Ellipsoidal2DCSAxis(BaseEnum):
    """
    .. versionadded:: 2.5.0

    Ellipsoidal 2D Coordinate System Axis for creating axis with
    with :class:`pyproj.crs.coordinate_system.Ellipsoidal2DCS`

    Attributes
    ----------
    LONGITUDE_LATITUDE
    LATITUDE_LONGITUDE
    """

    LONGITUDE_LATITUDE = "LONGITUDE_LATITUDE"
    LATITUDE_LONGITUDE = "LATITUDE_LONGITUDE"


class Ellipsoidal3DCSAxis(BaseEnum):
    """
    .. versionadded:: 2.5.0

    Ellipsoidal 3D Coordinate System Axis for creating axis with
    with :class:`pyproj.crs.coordinate_system.Ellipsoidal3DCS`

    Attributes
    ----------
    LONGITUDE_LATITUDE_HEIGHT
    LATITUDE_LONGITUDE_HEIGHT
    """

    LONGITUDE_LATITUDE_HEIGHT = "LONGITUDE_LATITUDE_HEIGHT"
    LATITUDE_LONGITUDE_HEIGHT = "LATITUDE_LONGITUDE_HEIGHT"


class VerticalCSAxis(BaseEnum):
    """
    .. versionadded:: 2.5.0

    Vertical Coordinate System Axis for creating axis with
    with :class:`pyproj.crs.coordinate_system.VerticalCS`

    Attributes
    ----------
    UP
    UP_FT
    UP_US_FT
    DEPTH
    DEPTH_FT
    DEPTH_US_FT
    GRAVITY_HEIGHT
    GRAVITY_HEIGHT_FT
    GRAVITY_HEIGHT_US_FT
    """

    GRAVITY_HEIGHT = "GRAVITY_HEIGHT"
    GRAVITY_HEIGHT_FT = "GRAVITY_HEIGHT_FT"
    GRAVITY_HEIGHT_US_FT = "GRAVITY_HEIGHT_US_FT"
    DEPTH = "DEPTH"
    DEPTH_FT = "DEPTH_FT"
    DEPTH_US_FT = "DEPTH_US_FT"
    UP = "UP"
    UP_FT = "UP_FT"
    UP_US_FT = "UP_US_FT"
