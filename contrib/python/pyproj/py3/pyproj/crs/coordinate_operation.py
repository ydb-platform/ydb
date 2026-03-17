"""
This module is for building operations to be used when
building a CRS.

:ref:`operations`
"""

# pylint: disable=too-many-lines
import warnings
from typing import Any, Optional

from pyproj._crs import CoordinateOperation
from pyproj._version import PROJ_VERSION
from pyproj.exceptions import CRSError


class AlbersEqualAreaConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Albers Equal Area Conversion.

    :ref:`PROJ docs <aea>`
    """

    def __new__(
        cls,
        latitude_first_parallel: float,
        latitude_second_parallel: float,
        latitude_false_origin: float = 0.0,
        longitude_false_origin: float = 0.0,
        easting_false_origin: float = 0.0,
        northing_false_origin: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_first_parallel: float
            First standard parallel (lat_1).
        latitude_second_parallel: float
            Second standard parallel (lat_2).
        latitude_false_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_false_origin: float, default=0.0
            Longitude of projection center (lon_0).
        easting_false_origin: float, default=0.0
            False easting (x_0).
        northing_false_origin: float, default=0.0
            False northing (y_0).
        """
        aea_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Albers Equal Area",
                "id": {"authority": "EPSG", "code": 9822},
            },
            "parameters": [
                {
                    "name": "Latitude of false origin",
                    "value": latitude_false_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8821},
                },
                {
                    "name": "Longitude of false origin",
                    "value": longitude_false_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8822},
                },
                {
                    "name": "Latitude of 1st standard parallel",
                    "value": latitude_first_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8823},
                },
                {
                    "name": "Latitude of 2nd standard parallel",
                    "value": latitude_second_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8824},
                },
                {
                    "name": "Easting at false origin",
                    "value": easting_false_origin,
                    "unit": {
                        "type": "LinearUnit",
                        "name": "Metre",
                        "conversion_factor": 1,
                    },
                    "id": {"authority": "EPSG", "code": 8826},
                },
                {
                    "name": "Northing at false origin",
                    "value": northing_false_origin,
                    "unit": {
                        "type": "LinearUnit",
                        "name": "Metre",
                        "conversion_factor": 1,
                    },
                    "id": {"authority": "EPSG", "code": 8827},
                },
            ],
        }
        return cls.from_json_dict(aea_json)


class AzimuthalEquidistantConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0 AzumuthalEquidistantConversion
    .. versionadded:: 3.2.0 AzimuthalEquidistantConversion

    Class for constructing the Modified Azimuthal Equidistant conversion.

    :ref:`PROJ docs <aeqd>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        aeqd_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Modified Azimuthal Equidistant",
                "id": {"authority": "EPSG", "code": 9832},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(aeqd_json)


class GeostationarySatelliteConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Geostationary Satellite conversion.

    :ref:`PROJ docs <geos>`
    """

    def __new__(
        cls,
        sweep_angle_axis: str,
        satellite_height: float,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        sweep_angle_axis: str
            Sweep angle axis of the viewing instrument. Valid options are “X” and “Y”.
        satellite_height: float
            Satellite height.
        latitude_natural_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        sweep_angle_axis = sweep_angle_axis.strip().upper()
        valid_sweep_axis = ("X", "Y")
        if sweep_angle_axis not in valid_sweep_axis:
            raise CRSError(f"sweep_angle_axis only supports {valid_sweep_axis}")

        if latitude_natural_origin != 0:
            warnings.warn(
                "The latitude of natural origin (lat_0) is not used "
                "within PROJ. It is only supported for exporting to "
                "the WKT or PROJ JSON formats."
            )

        geos_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {"name": f"Geostationary Satellite (Sweep {sweep_angle_axis})"},
            "parameters": [
                {
                    "name": "Satellite height",
                    "value": satellite_height,
                    "unit": "metre",
                },
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(geos_json)


class LambertAzimuthalEqualAreaConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0 LambertAzumuthalEqualAreaConversion
    .. versionadded:: 3.2.0 LambertAzimuthalEqualAreaConversion

    Class for constructing the Lambert Azimuthal Equal Area conversion.

    :ref:`PROJ docs <laea>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        laea_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Lambert Azimuthal Equal Area",
                "id": {"authority": "EPSG", "code": 9820},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(laea_json)


class LambertConformalConic2SPConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Lambert Conformal Conic 2SP conversion.

    :ref:`PROJ docs <lcc>`
    """

    def __new__(
        cls,
        latitude_first_parallel: float,
        latitude_second_parallel: float,
        latitude_false_origin: float = 0.0,
        longitude_false_origin: float = 0.0,
        easting_false_origin: float = 0.0,
        northing_false_origin: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_first_parallel: float
            Latitude of 1st standard parallel (lat_1).
        latitude_second_parallel: float
            Latitude of 2nd standard parallel (lat_2).
        latitude_false_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_false_origin: float, default=0.0
            Longitude of projection center (lon_0).
        easting_false_origin: float, default=0.0
            False easting (x_0).
        northing_false_origin: float, default=0.0
            False northing (y_0).

        """
        lcc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Lambert Conic Conformal (2SP)",
                "id": {"authority": "EPSG", "code": 9802},
            },
            "parameters": [
                {
                    "name": "Latitude of 1st standard parallel",
                    "value": latitude_first_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8823},
                },
                {
                    "name": "Latitude of 2nd standard parallel",
                    "value": latitude_second_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8824},
                },
                {
                    "name": "Latitude of false origin",
                    "value": latitude_false_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8821},
                },
                {
                    "name": "Longitude of false origin",
                    "value": longitude_false_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8822},
                },
                {
                    "name": "Easting at false origin",
                    "value": easting_false_origin,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8826},
                },
                {
                    "name": "Northing at false origin",
                    "value": northing_false_origin,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8827},
                },
            ],
        }
        return cls.from_json_dict(lcc_json)


class LambertConformalConic1SPConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Lambert Conformal Conic 1SP conversion.

    :ref:`PROJ docs <lcc>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
        scale_factor_natural_origin: float = 1.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        scale_factor_natural_origin: float, default=1.0
            Scale factor at natural origin (k_0).

        """
        lcc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Lambert Conic Conformal (1SP)",
                "id": {"authority": "EPSG", "code": 9801},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "Scale factor at natural origin",
                    "value": scale_factor_natural_origin,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8805},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(lcc_json)


class LambertCylindricalEqualAreaConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Lambert Cylindrical Equal Area conversion.

    :ref:`PROJ docs <cea>`
    """

    def __new__(
        cls,
        latitude_first_parallel: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_first_parallel: float, default=0.0
            Latitude of 1st standard parallel (lat_ts).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        cea_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Lambert Cylindrical Equal Area",
                "id": {"authority": "EPSG", "code": 9835},
            },
            "parameters": [
                {
                    "name": "Latitude of 1st standard parallel",
                    "value": latitude_first_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8823},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(cea_json)


class LambertCylindricalEqualAreaScaleConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Lambert Cylindrical Equal Area conversion.

    This version uses the scale factor and differs from the official version.

    The scale factor will be converted to the Latitude of 1st standard parallel (lat_ts)
    when exporting to WKT in PROJ>=7.0.0. Previous version will export it as a
    PROJ-based coordinate operation in the WKT.

    :ref:`PROJ docs <cea>`
    """

    def __new__(
        cls,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
        scale_factor_natural_origin: float = 1.0,
    ):
        """
        Parameters
        ----------
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        scale_factor_natural_origin: float, default=1.0
            Scale factor at natural origin (k or k_0).

        """
        # pylint: disable=import-outside-toplevel
        from pyproj.crs import CRS

        # hack due to: https://github.com/OSGeo/PROJ/issues/1881
        proj_string = (
            "+proj=cea "
            f"+lon_0={longitude_natural_origin} "
            f"+x_0={false_easting} "
            f"+y_0={false_northing} "
            f"+k_0={scale_factor_natural_origin}"
        )
        return cls.from_json(
            CRS(proj_string).coordinate_operation.to_json()  # type: ignore[union-attr]
        )


class MercatorAConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Mercator (variant A) conversion.

    :ref:`PROJ docs <merc>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
        scale_factor_natural_origin: float = 1.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of natural origin (lat_0). Must be 0 by `this conversion's
            definition
            <https://epsg.org/coord-operation-method_9804/Mercator-variant-A.html>`_.
        longitude_natural_origin: float, default=0.0
            Longitude of natural origin (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        scale_factor_natural_origin: float, default=1.0
            Scale factor at natural origin (k or k_0).

        """
        if latitude_natural_origin != 0:
            raise CRSError(
                "This conversion is defined for only latitude_natural_origin = 0."
            )
        merc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Mercator (variant A)",
                "id": {"authority": "EPSG", "code": 9804},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "Scale factor at natural origin",
                    "value": scale_factor_natural_origin,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8805},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(merc_json)


class MercatorBConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Mercator (variant B) conversion.

    :ref:`PROJ docs <merc>`
    """

    def __new__(
        cls,
        latitude_first_parallel: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_first_parallel: float, default=0.0
            Latitude of 1st standard parallel (lat_ts).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        merc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Mercator (variant B)",
                "id": {"authority": "EPSG", "code": 9805},
            },
            "parameters": [
                {
                    "name": "Latitude of 1st standard parallel",
                    "value": latitude_first_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8823},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(merc_json)


class HotineObliqueMercatorBConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0
    .. versionadded:: 3.7.0 azimuth_projection_centre, scale_factor_projection_centre

    Class for constructing the Hotine Oblique Mercator (variant B) conversion.

    :ref:`PROJ docs <omerc>`
    """

    def __new__(
        cls,
        latitude_projection_centre: float,
        longitude_projection_centre: float,
        angle_from_rectified_to_skew_grid: float,
        easting_projection_centre: float = 0.0,
        northing_projection_centre: float = 0.0,
        azimuth_projection_centre: Optional[float] = None,
        scale_factor_projection_centre: Optional[float] = None,
        azimuth_initial_line: Optional[float] = None,
        scale_factor_on_initial_line: Optional[float] = None,
    ):
        """
        Parameters
        ----------
        latitude_projection_centre: float
            Latitude of projection centre (lat_0).
        longitude_projection_centre: float
            Longitude of projection centre (lonc).
        azimuth_projection_centre: float
            Azimuth of initial line (alpha).
        angle_from_rectified_to_skew_grid: float
            Angle from Rectified to Skew Grid (gamma).
        scale_factor_projection_centre: float, default=1.0
            Scale factor on initial line (k or k_0).
        easting_projection_centre: float, default=0.0
            Easting at projection centre (x_0).
        northing_projection_centre: float, default=0.0
            Northing at projection centre (y_0).
        azimuth_initial_line: float
            Deprecated alias for azimuth_projection_centre,
        scale_factor_on_initial_line: float
            Deprecated alias for scale_factor_projection_centre.
        """
        if scale_factor_on_initial_line is not None:
            if scale_factor_projection_centre is not None:
                raise ValueError(
                    "scale_factor_projection_centre and scale_factor_on_initial_line "
                    "cannot be provided together."
                )
            warnings.warn(
                "scale_factor_on_initial_line is deprecated. "
                "Use scale_factor_projection_centre instead.",
                FutureWarning,
                stacklevel=2,
            )
            scale_factor_projection_centre = scale_factor_on_initial_line
        elif scale_factor_projection_centre is None:
            scale_factor_projection_centre = 1.0

        if azimuth_projection_centre is None and azimuth_initial_line is None:
            raise ValueError(
                "azimuth_projection_centre or azimuth_initial_line must be provided."
            )
        if azimuth_initial_line is not None:
            if azimuth_projection_centre is not None:
                raise ValueError(
                    "azimuth_projection_centre and azimuth_initial_line cannot be "
                    "provided together."
                )
            warnings.warn(
                "azimuth_initial_line is deprecated. "
                "Use azimuth_projection_centre instead.",
                FutureWarning,
                stacklevel=2,
            )
            azimuth_projection_centre = azimuth_initial_line

        omerc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Hotine Oblique Mercator (variant B)",
                "id": {"authority": "EPSG", "code": 9815},
            },
            "parameters": [
                {
                    "name": "Latitude of projection centre",
                    "value": latitude_projection_centre,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8811},
                },
                {
                    "name": "Longitude of projection centre",
                    "value": longitude_projection_centre,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8812},
                },
                {
                    "name": (
                        "Azimuth at projection centre"
                        if PROJ_VERSION >= (9, 5, 0)
                        else "Azimuth of initial line"
                    ),
                    "value": azimuth_projection_centre,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8813},
                },
                {
                    "name": "Angle from Rectified to Skew Grid",
                    "value": angle_from_rectified_to_skew_grid,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8814},
                },
                {
                    "name": (
                        "Scale factor at projection centre"
                        if PROJ_VERSION >= (9, 5, 0)
                        else "Scale factor on initial line"
                    ),
                    "value": scale_factor_projection_centre,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8815},
                },
                {
                    "name": "Easting at projection centre",
                    "value": easting_projection_centre,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8816},
                },
                {
                    "name": "Northing at projection centre",
                    "value": northing_projection_centre,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8817},
                },
            ],
        }
        return cls.from_json_dict(omerc_json)


class OrthographicConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Orthographic conversion.

    :ref:`PROJ docs <ortho>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        ortho_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Orthographic",
                "id": {"authority": "EPSG", "code": 9840},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(ortho_json)


class PolarStereographicAConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Polar Stereographic A conversion.

    :ref:`PROJ docs <stere>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
        scale_factor_natural_origin: float = 1.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float
            Latitude of natural origin (lat_0). Either +90 or -90.
        longitude_natural_origin: float, default=0.0
            Longitude of natural origin (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        scale_factor_natural_origin: float, default=0.0
            Scale factor at natural origin (k or k_0).

        """

        stere_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Polar Stereographic (variant A)",
                "id": {"authority": "EPSG", "code": 9810},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "Scale factor at natural origin",
                    "value": scale_factor_natural_origin,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8805},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(stere_json)


class PolarStereographicBConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Polar Stereographic B conversion.

    :ref:`PROJ docs <stere>`
    """

    def __new__(
        cls,
        latitude_standard_parallel: float = 0.0,
        longitude_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_standard_parallel: float, default=0.0
            Latitude of standard parallel (lat_ts).
        longitude_origin: float, default=0.0
            Longitude of origin (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        stere_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Polar Stereographic (variant B)",
                "id": {"authority": "EPSG", "code": 9829},
            },
            "parameters": [
                {
                    "name": "Latitude of standard parallel",
                    "value": latitude_standard_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8832},
                },
                {
                    "name": "Longitude of origin",
                    "value": longitude_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8833},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(stere_json)


class SinusoidalConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Sinusoidal conversion.

    :ref:`PROJ docs <sinu>`
    """

    def __new__(
        cls,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        sinu_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {"name": "Sinusoidal"},
            "parameters": [
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(sinu_json)


class StereographicConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Stereographic conversion.

    :ref:`PROJ docs <stere>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
        scale_factor_natural_origin: float = 1.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of natural origin (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of natural origin (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        scale_factor_natural_origin: float, default=1.0
            Scale factor at natural origin (k or k_0).

        """

        stere_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {"name": "Stereographic"},
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "Scale factor at natural origin",
                    "value": scale_factor_natural_origin,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8805},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(stere_json)


class UTMConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the UTM conversion.

    :ref:`PROJ docs <utm>`
    """

    def __new__(cls, zone: str, hemisphere: str = "N"):
        """
        Parameters
        ----------
        zone: int
            UTM Zone between 1-60.
        hemisphere: str, default="N"
            Either N for North or S for South.
        """
        return cls.from_name(f"UTM zone {zone}{hemisphere}")


class TransverseMercatorConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Transverse Mercator conversion.

    :ref:`PROJ docs <tmerc>`
    """

    def __new__(
        cls,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
        scale_factor_natural_origin: float = 1.0,
    ):
        """
        Parameters
        ----------
        latitude_natural_origin: float, default=0.0
            Latitude of projection center (lat_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        scale_factor_natural_origin: float, default=1.0
            Scale factor at natural origin (k or k_0).

        """
        tmerc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Transverse Mercator",
                "id": {"authority": "EPSG", "code": 9807},
            },
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "Scale factor at natural origin",
                    "value": scale_factor_natural_origin,
                    "unit": "unity",
                    "id": {"authority": "EPSG", "code": 8805},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(tmerc_json)


class VerticalPerspectiveConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Vertical Perspective conversion.

    :ref:`PROJ docs <nsper>`
    """

    def __new__(
        cls,
        viewpoint_height: float,
        latitude_topocentric_origin: float = 0.0,
        longitude_topocentric_origin: float = 0.0,
        ellipsoidal_height_topocentric_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        viewpoint_height: float
            Viewpoint height (h).
        latitude_topocentric_origin: float, default=0.0
            Latitude of topocentric origin (lat_0).
        longitude_topocentric_origin: float, default=0.0
            Longitude of topocentric origin (lon_0).
        ellipsoidal_height_topocentric_origin: float, default=0.0
            Ellipsoidal height of topocentric origin.
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).

        """
        nsper_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Vertical Perspective",
                "id": {"authority": "EPSG", "code": 9838},
            },
            "parameters": [
                {
                    "name": "Latitude of topocentric origin",
                    "value": latitude_topocentric_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8834},
                },
                {
                    "name": "Longitude of topocentric origin",
                    "value": longitude_topocentric_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8835},
                },
                {
                    "name": "Ellipsoidal height of topocentric origin",
                    "value": ellipsoidal_height_topocentric_origin,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8836},
                },
                {
                    "name": "Viewpoint height",
                    "value": viewpoint_height,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8840},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(nsper_json)


class RotatedLatitudeLongitudeConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Rotated Latitude Longitude conversion.

    :ref:`PROJ docs <ob_tran>`
    """

    def __new__(cls, o_lat_p: float, o_lon_p: float, lon_0: float = 0.0):
        """
        Parameters
        ----------
        o_lat_p: float
            Latitude of the North pole of the unrotated source CRS,
            expressed in the rotated geographic CRS.
        o_lon_p: float
            Longitude of the North pole of the unrotated source CRS,
            expressed in the rotated geographic CRS.
        lon_0: float, default=0.0
            Longitude of projection center.

        """
        rot_latlon_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {"name": "PROJ ob_tran o_proj=longlat"},
            "parameters": [
                {"name": "o_lat_p", "value": o_lat_p, "unit": "degree"},
                {"name": "o_lon_p", "value": o_lon_p, "unit": "degree"},
                {"name": "lon_0", "value": lon_0, "unit": "degree"},
            ],
        }
        return cls.from_json_dict(rot_latlon_json)


class PoleRotationNetCDFCFConversion(CoordinateOperation):
    """
    .. versionadded:: 3.3.0

    Class for constructing the Pole rotation (netCDF CF convention) conversion.

    http://cfconventions.org/cf-conventions/cf-conventions.html#_rotated_pole

    :ref:`PROJ docs <ob_tran>`
    """

    def __new__(
        cls,
        grid_north_pole_latitude: float,
        grid_north_pole_longitude: float,
        north_pole_grid_longitude: float = 0.0,
    ):
        """
        Parameters
        ----------
        grid_north_pole_latitude: float
            Latitude of the North pole of the unrotated source CRS,
            expressed in the rotated geographic CRS (o_lat_p)
        grid_north_pole_longitude: float
            Longitude of projection center (lon_0 - 180).
        north_pole_grid_longitude: float, default=0.0
            Longitude of the North pole of the unrotated source CRS,
            expressed in the rotated geographic CRS (o_lon_p).
        """
        rot_latlon_json = {
            "$schema": "https://proj.org/schemas/v0.4/projjson.schema.json",
            "type": "Conversion",
            "name": "Pole rotation (netCDF CF convention)",
            "method": {"name": "Pole rotation (netCDF CF convention)"},
            "parameters": [
                {
                    "name": "Grid north pole latitude (netCDF CF convention)",
                    "value": grid_north_pole_latitude,
                    "unit": "degree",
                },
                {
                    "name": "Grid north pole longitude (netCDF CF convention)",
                    "value": grid_north_pole_longitude,
                    "unit": "degree",
                },
                {
                    "name": "North pole grid longitude (netCDF CF convention)",
                    "value": north_pole_grid_longitude,
                    "unit": "degree",
                },
            ],
        }
        return cls.from_json_dict(rot_latlon_json)


class EquidistantCylindricalConversion(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the Equidistant Cylintrical (Plate Carrée) conversion.

    :ref:`PROJ docs <eqc>`
    """

    def __new__(
        cls,
        latitude_first_parallel: float = 0.0,
        latitude_natural_origin: float = 0.0,
        longitude_natural_origin: float = 0.0,
        false_easting: float = 0.0,
        false_northing: float = 0.0,
    ):
        """
        Parameters
        ----------
        latitude_first_parallel: float, default=0.0
            Latitude of 1st standard parallel (lat_ts).
        latitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        longitude_natural_origin: float, default=0.0
            Longitude of projection center (lon_0).
        false_easting: float, default=0.0
            False easting (x_0).
        false_northing: float, default=0.0
            False northing (y_0).
        """
        eqc_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Conversion",
            "name": "unknown",
            "method": {
                "name": "Equidistant Cylindrical",
                "id": {"authority": "EPSG", "code": 1028},
            },
            "parameters": [
                {
                    "name": "Latitude of 1st standard parallel",
                    "value": latitude_first_parallel,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8823},
                },
                {
                    "name": "Latitude of natural origin",
                    "value": latitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8801},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": longitude_natural_origin,
                    "unit": "degree",
                    "id": {"authority": "EPSG", "code": 8802},
                },
                {
                    "name": "False easting",
                    "value": false_easting,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8806},
                },
                {
                    "name": "False northing",
                    "value": false_northing,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8807},
                },
            ],
        }
        return cls.from_json_dict(eqc_json)


# Add an alias for PlateCarree
PlateCarreeConversion = EquidistantCylindricalConversion


class ToWGS84Transformation(CoordinateOperation):
    """
    .. versionadded:: 2.5.0

    Class for constructing the ToWGS84 Transformation.
    """

    def __new__(
        cls,
        source_crs: Any,
        x_axis_translation: float = 0,
        y_axis_translation: float = 0,
        z_axis_translation: float = 0,
        x_axis_rotation: float = 0,
        y_axis_rotation: float = 0,
        z_axis_rotation: float = 0,
        scale_difference: float = 0,
    ):
        """
        Parameters
        ----------
        source_crs: Any
            Input to create the Source CRS.
        x_axis_translation: float, default=0.0
            X-axis translation.
        y_axis_translation: float, default=0.0
            Y-axis translation.
        z_axis_translation: float, default=0.0
            Z-axis translation.
        x_axis_rotation: float, default=0.0
            X-axis rotation.
        y_axis_rotation: float, default=0.0
            Y-axis rotation.
        z_axis_rotation: float, default=0.0
            Z-axis rotation.
        scale_difference: float, default=0.0
            Scale difference.
        """
        # pylint: disable=import-outside-toplevel
        from pyproj.crs import CRS

        towgs84_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Transformation",
            "name": "Transformation from unknown to WGS84",
            "source_crs": CRS.from_user_input(source_crs).to_json_dict(),
            "target_crs": {
                "type": "GeographicCRS",
                "name": "WGS 84",
                "datum": {
                    "type": "GeodeticReferenceFrame",
                    "name": "World Geodetic System 1984",
                    "ellipsoid": {
                        "name": "WGS 84",
                        "semi_major_axis": 6378137,
                        "inverse_flattening": 298.257223563,
                    },
                },
                "coordinate_system": {
                    "subtype": "ellipsoidal",
                    "axis": [
                        {
                            "name": "Latitude",
                            "abbreviation": "lat",
                            "direction": "north",
                            "unit": "degree",
                        },
                        {
                            "name": "Longitude",
                            "abbreviation": "lon",
                            "direction": "east",
                            "unit": "degree",
                        },
                    ],
                },
                "id": {"authority": "EPSG", "code": 4326},
            },
            "method": {
                "name": "Position Vector transformation (geog2D domain)",
                "id": {"authority": "EPSG", "code": 9606},
            },
            "parameters": [
                {
                    "name": "X-axis translation",
                    "value": x_axis_translation,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8605},
                },
                {
                    "name": "Y-axis translation",
                    "value": y_axis_translation,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8606},
                },
                {
                    "name": "Z-axis translation",
                    "value": z_axis_translation,
                    "unit": "metre",
                    "id": {"authority": "EPSG", "code": 8607},
                },
                {
                    "name": "X-axis rotation",
                    "value": x_axis_rotation,
                    "unit": {
                        "type": "AngularUnit",
                        "name": "arc-second",
                        "conversion_factor": 4.84813681109536e-06,
                    },
                    "id": {"authority": "EPSG", "code": 8608},
                },
                {
                    "name": "Y-axis rotation",
                    "value": y_axis_rotation,
                    "unit": {
                        "type": "AngularUnit",
                        "name": "arc-second",
                        "conversion_factor": 4.84813681109536e-06,
                    },
                    "id": {"authority": "EPSG", "code": 8609},
                },
                {
                    "name": "Z-axis rotation",
                    "value": z_axis_rotation,
                    "unit": {
                        "type": "AngularUnit",
                        "name": "arc-second",
                        "conversion_factor": 4.84813681109536e-06,
                    },
                    "id": {"authority": "EPSG", "code": 8610},
                },
                {
                    "name": "Scale difference",
                    "value": scale_difference,
                    "unit": {
                        "type": "ScaleUnit",
                        "name": "parts per million",
                        "conversion_factor": 1e-06,
                    },
                    "id": {"authority": "EPSG", "code": 8611},
                },
            ],
        }

        return cls.from_json_dict(towgs84_json)
