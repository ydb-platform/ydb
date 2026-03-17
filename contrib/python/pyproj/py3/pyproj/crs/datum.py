"""
This module is for building datums to be used when
building a CRS.
"""

from typing import Any

from pyproj._crs import Datum, Ellipsoid, PrimeMeridian


class CustomDatum(Datum):
    """
    .. versionadded:: 2.5.0

    Class to build a datum based on an ellipsoid and prime meridian.
    """

    def __new__(
        cls,
        name: str = "undefined",
        ellipsoid: Any = "WGS 84",
        prime_meridian: Any = "Greenwich",
    ):
        """
        Parameters
        ----------
        name: str, default="undefined"
            Name of the datum.
        ellipsoid: Any, default="WGS 84"
            Anything accepted by :meth:`pyproj.crs.Ellipsoid.from_user_input`
            or a :class:`pyproj.crs.datum.CustomEllipsoid`.
        prime_meridian: Any, default="Greenwich"
            Anything accepted by :meth:`pyproj.crs.PrimeMeridian.from_user_input`.
        """
        datum_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "GeodeticReferenceFrame",
            "name": name,
            "ellipsoid": Ellipsoid.from_user_input(ellipsoid).to_json_dict(),
            "prime_meridian": PrimeMeridian.from_user_input(
                prime_meridian
            ).to_json_dict(),
        }
        return cls.from_json_dict(datum_json)


class CustomEllipsoid(Ellipsoid):
    """
    .. versionadded:: 2.5.0

    Class to build a custom ellipsoid.
    """

    def __new__(
        cls,
        name: str = "undefined",
        semi_major_axis: float | None = None,
        inverse_flattening: float | None = None,
        semi_minor_axis: float | None = None,
        radius: float | None = None,
    ):
        """
        Parameters
        ----------
        name: str, default="undefined"
            Name of the ellipsoid.
        semi_major_axis: float, optional
            The semi major axis in meters. Required if missing radius.
        inverse_flattening: float, optional
            The inverse flattening in meters.
            Required if missing semi_minor_axis and radius.
        semi_minor_axis: float, optional
            The semi minor axis in meters.
            Required if missing inverse_flattening and radius.
        radius: float, optional
            The radius in meters. Can only be used alone.
            Cannot be mixed with other parameters.
        """
        ellipsoid_json: dict[str, float | str] = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "Ellipsoid",
            "name": name,
        }
        if semi_major_axis is not None:
            ellipsoid_json["semi_major_axis"] = semi_major_axis
        if inverse_flattening is not None:
            ellipsoid_json["inverse_flattening"] = inverse_flattening
        if semi_minor_axis is not None:
            ellipsoid_json["semi_minor_axis"] = semi_minor_axis
        if radius is not None:
            ellipsoid_json["radius"] = radius
        return cls.from_json_dict(ellipsoid_json)


class CustomPrimeMeridian(PrimeMeridian):
    """
    .. versionadded:: 2.5.0

    Class to build a prime meridian based on a longitude.
    """

    def __new__(cls, longitude: float, name: str = "undefined"):
        """
        Parameters
        ----------
        longitude: float
            Longitude of prime meridian.
        name: str, optional
            Name of the prime meridian.
        """
        datum_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "PrimeMeridian",
            "name": name,
            "longitude": longitude,
        }
        return cls.from_json_dict(datum_json)
