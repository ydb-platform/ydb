# -*- coding: utf-8 -*-
"""
This module interfaces with PROJ to produce a pythonic interface
to the coordinate reference system (CRS) information through the CRS
class.

Original Author: Alan D. Snow [github.com/snowman2] (2019)

Permission to use, copy, modify, and distribute this software
and its documentation for any purpose and without fee is hereby
granted, provided that the above copyright notice appear in all
copies and that both the copyright notice and this permission
notice appear in supporting documentation. THE AUTHOR DISCLAIMS
ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT
SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, INDIRECT OR
CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""
__all__ = [
    "CRS",
    "CoordinateOperation",
    "Datum",
    "Ellipsoid",
    "PrimeMeridian",
    "is_wkt",
    "is_proj",
]

import json
import re
import warnings

from pyproj._crs import CoordinateOperation  # noqa
from pyproj._crs import Datum  # noqa
from pyproj._crs import Ellipsoid  # noqa
from pyproj._crs import PrimeMeridian  # noqa
from pyproj._crs import _CRS, is_proj, is_wkt
from pyproj.cf1x8 import (
    GRID_MAPPING_NAME_MAP,
    INVERSE_GRID_MAPPING_NAME_MAP,
    INVERSE_PROJ_PARAM_MAP,
    K_0_MAP,
    LON_0_MAP,
    PROJ_PARAM_MAP,
    PARAM_TO_CF_MAP,
    METHOD_NAME_TO_CF_MAP,
)
from pyproj.compat import string_types
from pyproj.exceptions import CRSError
from pyproj.geod import Geod


def _prepare_from_dict(projparams):
    # convert a dict to a proj string.
    pjargs = []
    for key, value in projparams.items():
        # the towgs84 as list
        if isinstance(value, (list, tuple)):
            value = ",".join([str(val) for val in value])
        # issue 183 (+ no_rot)
        if value is None or value is True:
            pjargs.append("+{key}".format(key=key))
        elif value is False:
            pass
        else:
            pjargs.append("+{key}={value}".format(key=key, value=value))
    return _prepare_from_string(" ".join(pjargs))


def _prepare_from_string(in_crs_string):
    if not in_crs_string:
        raise CRSError("CRS is empty or invalid: {!r}".format(in_crs_string))
    elif "{" in in_crs_string:
        # may be json, try to decode it
        try:
            crs_dict = json.loads(in_crs_string, strict=False)
        except ValueError:
            raise CRSError("CRS appears to be JSON but is not valid")

        if not crs_dict:
            raise CRSError("CRS is empty JSON")
        return _prepare_from_dict(crs_dict)
    elif is_proj(in_crs_string):
        in_crs_string = re.sub(r"[\s+]?=[\s+]?", "=", in_crs_string.lstrip())
        # make sure the projection starts with +proj or +init
        starting_params = ("+init", "+proj", "init", "proj")
        if not in_crs_string.startswith(starting_params):
            kvpairs = []
            first_item_inserted = False
            for kvpair in in_crs_string.split():
                if not first_item_inserted and (kvpair.startswith(starting_params)):
                    kvpairs.insert(0, kvpair)
                    first_item_inserted = True
                else:
                    kvpairs.append(kvpair)
            in_crs_string = " ".join(kvpairs)

        # make sure it is the CRS type
        if "type=crs" not in in_crs_string:
            if "+" in in_crs_string:
                in_crs_string += " +type=crs"
            else:
                in_crs_string += " type=crs"

        # look for EPSG, replace with epsg (EPSG only works
        # on case-insensitive filesystems).
        in_crs_string = in_crs_string.replace("+init=EPSG", "+init=epsg").strip()
        if in_crs_string.startswith(("+init", "init")):
            warnings.warn(
                "'+init=<authority>:<code>' syntax is deprecated."
                " '<authority>:<code>' is the preferred initialization method.",
                DeprecationWarning,
            )
    return in_crs_string


def _prepare_from_authority(auth_name, auth_code):
    return "{}:{}".format(auth_name, auth_code)


def _prepare_from_epsg(auth_code):
    return _prepare_from_authority("epsg", auth_code)


class CRS(_CRS):
    """
    A pythonic Coordinate Reference System manager.

    The functionality is based on other fantastic projects:

    * `rasterio <https://github.com/mapbox/rasterio/blob/c13f0943b95c0eaa36ff3f620bd91107aa67b381/rasterio/_crs.pyx>`_  # noqa: E501
    * `opendatacube <https://github.com/opendatacube/datacube-core/blob/83bae20d2a2469a6417097168fd4ede37fd2abe5/datacube/utils/geometry/_base.py>`_  # noqa: E501

    Attributes
    ----------
    srs: str
        The string form of the user input used to create the CRS.
    name: str
        The name of the CRS (from `proj_get_name <https://proj.org/
        development/reference/functions.html#_CPPv313proj_get_namePK2PJ>`_).
    type_name: str
        The name of the type of the CRS object.

    """

    def __init__(self, projparams=None, **kwargs):
        """
        Initialize a CRS class instance with:
          - PROJ string
          - Dictionary of PROJ parameters
          - PROJ keyword arguments for parameters
          - JSON string with PROJ parameters
          - CRS WKT string
          - An authority string [i.e. 'epsg:4326']
          - An EPSG integer code [i.e. 4326]
          - A tuple of ("auth_name": "auth_code") [i.e ('epsg', '4326')]
          - An object with a `to_wkt` method.
          - A :class:`~pyproj.CRS`

        Example usage:

        >>> from pyproj import CRS
        >>> crs_utm = CRS.from_user_input(26915)
        >>> crs_utm
        <Projected CRS: EPSG:26915>
        Name: NAD83 / UTM zone 15N
        Axis Info [cartesian]:
        - E[east]: Easting (metre)
        - N[north]: Northing (metre)
        Area of Use:
        - name: North America - 96°W to 90°W and NAD83 by country
        - bounds: (-96.0, 25.61, -90.0, 84.0)
        Coordinate Operation:
        - name: UTM zone 15N
        - method: Transverse Mercator
        Datum: North American Datum 1983
        - Ellipsoid: GRS 1980
        - Prime Meridian: Greenwich
        <BLANKLINE>
        >>> crs_utm.area_of_use.bounds
        (-96.0, 25.61, -90.0, 84.0)
        >>> crs_utm.ellipsoid
        ELLIPSOID["GRS 1980",6378137,298.257222101,
            LENGTHUNIT["metre",1],
            ID["EPSG",7019]]
        >>> crs_utm.ellipsoid.inverse_flattening
        298.257222101
        >>> crs_utm.ellipsoid.semi_major_metre
        6378137.0
        >>> crs_utm.ellipsoid.semi_minor_metre
        6356752.314140356
        >>> crs_utm.prime_meridian
        PRIMEM["Greenwich",0,
            ANGLEUNIT["degree",0.0174532925199433],
            ID["EPSG",8901]]
        >>> crs_utm.prime_meridian.unit_name
        'degree'
        >>> crs_utm.prime_meridian.unit_conversion_factor
        0.017453292519943295
        >>> crs_utm.prime_meridian.longitude
        0.0
        >>> crs_utm.datum
        DATUM["North American Datum 1983",
            ELLIPSOID["GRS 1980",6378137,298.257222101,
                LENGTHUNIT["metre",1]],
            ID["EPSG",6269]]
        >>> crs_utm.coordinate_system
        CS[Cartesian,2],
            AXIS["(E)",east,
                ORDER[1],
                LENGTHUNIT["metre",1,
                    ID["EPSG",9001]]],
            AXIS["(N)",north,
                ORDER[2],
                LENGTHUNIT["metre",1,
                    ID["EPSG",9001]]]
        >>> crs_utm.coordinate_operation
        CONVERSION["UTM zone 15N",
            METHOD["Transverse Mercator",
                ID["EPSG",9807]],
            PARAMETER["Latitude of natural origin",0,
                ANGLEUNIT["degree",0.0174532925199433],
                ID["EPSG",8801]],
            PARAMETER["Longitude of natural origin",-93,
                ANGLEUNIT["degree",0.0174532925199433],
                ID["EPSG",8802]],
            PARAMETER["Scale factor at natural origin",0.9996,
                SCALEUNIT["unity",1],
                ID["EPSG",8805]],
            PARAMETER["False easting",500000,
                LENGTHUNIT["metre",1],
                ID["EPSG",8806]],
            PARAMETER["False northing",0,
                LENGTHUNIT["metre",1],
                ID["EPSG",8807]],
            ID["EPSG",16015]]
        >>> crs = CRS(proj='utm', zone=10, ellps='WGS84')
        >>> crs.to_proj4()
        '+proj=utm +zone=10 +ellps=WGS84 +units=m +no_defs +type=crs'
        >>> print(crs.to_wkt(pretty=True))
        PROJCRS["unknown",
            BASEGEOGCRS["unknown",
                DATUM["Unknown based on WGS84 ellipsoid",
                    ELLIPSOID["WGS 84",6378137,298.257223563,
                        LENGTHUNIT["metre",1],
                        ID["EPSG",7030]]],
                PRIMEM["Greenwich",0,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8901]]],
            CONVERSION["UTM zone 10N",
                METHOD["Transverse Mercator",
                    ID["EPSG",9807]],
                PARAMETER["Latitude of natural origin",0,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8801]],
                PARAMETER["Longitude of natural origin",-123,
                    ANGLEUNIT["degree",0.0174532925199433],
                    ID["EPSG",8802]],
                PARAMETER["Scale factor at natural origin",0.9996,
                    SCALEUNIT["unity",1],
                    ID["EPSG",8805]],
                PARAMETER["False easting",500000,
                    LENGTHUNIT["metre",1],
                    ID["EPSG",8806]],
                PARAMETER["False northing",0,
                    LENGTHUNIT["metre",1],
                    ID["EPSG",8807]],
                ID["EPSG",16010]],
            CS[Cartesian,2],
                AXIS["(E)",east,
                    ORDER[1],
                    LENGTHUNIT["metre",1,
                        ID["EPSG",9001]]],
                AXIS["(N)",north,
                    ORDER[2],
                    LENGTHUNIT["metre",1,
                        ID["EPSG",9001]]]]
        >>> geod = crs.get_geod()
        >>> "+a={:.0f} +f={:.8f}".format(geod.a, geod.f)
        '+a=6378137 +f=0.00335281'
        >>> crs.is_projected
        True
        >>> crs.is_geographic
        False
        """
        if isinstance(projparams, string_types):
            projstring = _prepare_from_string(projparams)
        elif isinstance(projparams, dict):
            projstring = _prepare_from_dict(projparams)
        elif kwargs:
            projstring = _prepare_from_dict(kwargs)
        elif isinstance(projparams, int):
            projstring = _prepare_from_epsg(projparams)
        elif isinstance(projparams, (list, tuple)) and len(projparams) == 2:
            projstring = _prepare_from_authority(*projparams)
        elif hasattr(projparams, "to_wkt"):
            projstring = projparams.to_wkt()
        else:
            raise CRSError("Invalid CRS input: {!r}".format(projparams))

        super(CRS, self).__init__(projstring)

    @classmethod
    def from_authority(cls, auth_name, code):
        """Make a CRS from an authority name and authority code

        Parameters
        ----------
        auth_name: str
            The name of the authority.
        code : int or str
            The code used by the authority.

        Returns
        -------
        CRS
        """
        return cls(_prepare_from_authority(auth_name, code))

    @classmethod
    def from_epsg(cls, code):
        """Make a CRS from an EPSG code

        Parameters
        ----------
        code : int or str
            An EPSG code.

        Returns
        -------
        CRS
        """
        return cls(_prepare_from_epsg(code))

    @classmethod
    def from_proj4(cls, in_proj_string):
        """Make a CRS from a PROJ string

        Parameters
        ----------
        in_proj_string : str
            A PROJ string.

        Returns
        -------
        CRS
        """
        if not is_proj(in_proj_string):
            raise CRSError("Invalid PROJ string: {}".format(in_proj_string))
        return cls(_prepare_from_string(in_proj_string))

    @classmethod
    def from_wkt(cls, in_wkt_string):
        """Make a CRS from a WKT string

        Parameters
        ----------
        in_wkt_string : str
            A WKT string.

        Returns
        -------
        CRS
        """
        if not is_wkt(in_wkt_string):
            raise CRSError("Invalid WKT string: {}".format(in_wkt_string))
        return cls(_prepare_from_string(in_wkt_string))

    @classmethod
    def from_string(cls, in_crs_string):
        """Make a CRS from:

        Initialize a CRS class instance with:
         - PROJ string
         - JSON string with PROJ parameters
         - CRS WKT string
         - An authority string [i.e. 'epsg:4326']

        Parameters
        ----------
        in_crs_string : str
            An EPSG, PROJ, or WKT string.

        Returns
        -------
        CRS
        """
        return cls(_prepare_from_string(in_crs_string))

    def to_string(self):
        """Convert the CRS to a string.

        It attempts to convert it to the authority string.
        Otherwise, it uses the string format of the user
        input to create the CRS.

        Returns
        -------
        str: String representation of the CRS.
        """
        auth_info = self.to_authority(min_confidence=100)
        if auth_info:
            return ":".join(auth_info)
        return self.srs

    @classmethod
    def from_user_input(cls, value):
        """
        Initialize a CRS class instance with:
          - PROJ string
          - Dictionary of PROJ parameters
          - PROJ keyword arguments for parameters
          - JSON string with PROJ parameters
          - CRS WKT string
          - An authority string [i.e. 'epsg:4326']
          - An EPSG integer code [i.e. 4326]
          - A tuple of ("auth_name": "auth_code") [i.e ('epsg', '4326')]
          - An object with a `to_wkt` method.
          - A :class:`~pyproj.CRS`

        Parameters
        ----------
        value : obj
            A Python int, dict, or str.

        Returns
        -------
        CRS
        """
        if isinstance(value, _CRS):
            return value
        return cls(value)

    def get_geod(self):
        """
        Returns
        -------
        pyproj.geod.Geod: Geod object based on the ellipsoid.
        """
        if self.ellipsoid is None or not self.ellipsoid.ellipsoid_loaded:
            return None
        in_kwargs = {
            "a": self.ellipsoid.semi_major_metre,
            "rf": self.ellipsoid.inverse_flattening,
        }
        if self.ellipsoid.is_semi_minor_computed:
            in_kwargs["b"] = self.ellipsoid.semi_minor_metre
        return Geod(**in_kwargs)

    @classmethod
    def from_dict(cls, proj_dict):
        """Make a CRS from a dictionary of PROJ parameters.

        Parameters
        ----------
        proj_dict : str
            PROJ params in dict format.

        Returns
        -------
        CRS
        """
        return cls(_prepare_from_dict(proj_dict))

    def to_dict(self):
        """
        Converts the CRS to dictionary of PROJ parameters.

        .. warning:: You will likely lose important projection
          information when converting to a PROJ string from
          another format. See: https://proj.org/faq.html#what-is-the-best-format-for-describing-coordinate-reference-systems  # noqa: E501

        Returns
        -------
        dict: PROJ params in dict format.

        """

        def parse(val):
            if val.lower() == "true":
                return True
            elif val.lower() == "false":
                return False
            try:
                return int(val)
            except ValueError:
                pass
            try:
                return float(val)
            except ValueError:
                pass
            val_split = val.split(",")
            if len(val_split) > 1:
                val = [float(sval.strip()) for sval in val_split]
            return val

        proj_string = self.to_proj4()
        if proj_string is None:
            return {}

        items = map(
            lambda kv: len(kv) == 2 and (kv[0], parse(kv[1])) or (kv[0], None),
            (part.lstrip("+").split("=", 1) for part in proj_string.strip().split()),
        )

        return {key: value for key, value in items if value is not False}

    def to_cf(self, wkt_version="WKT2_2018", errcheck=False):
        """
        This converts a :obj:`~pyproj.crs.CRS` object
        to a Climate and Forecast (CF) Grid Mapping Version 1.8 dict.

        .. warning:: The full projection will be stored in the
            crs_wkt attribute. However, other parameters may be lost
            if a mapping to the CF parameter is not found.

        Parameters
        ----------
        wkt_version: str
            Version of WKT supported by ~CRS.to_wkt.
        errcheck: bool, optional
            If True, will warn when parameters are ignored. Defaults to False.

        Returns
        -------
        dict: CF-1.8 version of the projection.

        """

        cf_dict = {"crs_wkt": self.to_wkt(wkt_version)}
        if self.is_geographic and self.name != "unknown":
            cf_dict["geographic_crs_name"] = self.name
        elif self.is_projected and self.name != "unknown":
            cf_dict["projected_crs_name"] = self.name

        proj_dict = self.to_dict()
        proj_name = proj_dict.pop("proj")
        lonlat_possible_names = ("lonlat", "latlon", "longlat", "latlong")
        if proj_name in lonlat_possible_names:
            grid_mapping_name = "latitude_longitude"
        else:
            grid_mapping_name = INVERSE_GRID_MAPPING_NAME_MAP.get(proj_name, "unknown")

        if grid_mapping_name == "rotated_latitude_longitude":
            if proj_dict.pop("o_proj") not in lonlat_possible_names:
                grid_mapping_name = "unknown"

        # derive parameters from the coordinate operation
        if (
            grid_mapping_name == "unknown"
            and self.coordinate_operation
            and self.coordinate_operation.method_name in METHOD_NAME_TO_CF_MAP
        ):
            grid_mapping_name = METHOD_NAME_TO_CF_MAP[
                self.coordinate_operation.method_name
            ]
            for param in self.coordinate_operation.params:
                cf_dict[PARAM_TO_CF_MAP[param.name]] = param.value

        cf_dict["grid_mapping_name"] = grid_mapping_name

        # get best match for lon_0 value for projetion name
        lon_0 = proj_dict.pop("lon_0", None)
        if lon_0 is not None:
            try:
                cf_dict[LON_0_MAP[grid_mapping_name]] = lon_0
            except KeyError:
                cf_dict[LON_0_MAP["DEFAULT"]] = lon_0

        # get best match for k_0 value for projetion name
        k_0 = proj_dict.pop("k_0", None)
        if k_0 is not None:
            try:
                cf_dict[K_0_MAP[grid_mapping_name]] = k_0
            except KeyError:
                cf_dict[K_0_MAP["DEFAULT"]] = k_0

        # format the lat_1 and lat_2 for the standard parallel
        if "lat_1" in proj_dict and "lat_2" in proj_dict:
            cf_dict["standard_parallel"] = [
                proj_dict.pop("lat_1"),
                proj_dict.pop("lat_2"),
            ]
        elif "lat_1" in proj_dict:
            cf_dict["standard_parallel"] = proj_dict.pop("lat_1")

        skipped_params = []
        for proj_param, proj_val in proj_dict.items():
            try:
                cf_dict[INVERSE_PROJ_PARAM_MAP[proj_param]] = proj_val
            except KeyError:
                skipped_params.append(proj_param)

        if errcheck and skipped_params:
            warnings.warn(
                "PROJ parameters not mapped to CF: {}".format(tuple(skipped_params))
            )
        return cf_dict

    @staticmethod
    def from_cf(in_cf, errcheck=False):
        """
        This converts a Climate and Forecast (CF) Grid Mapping Version 1.8
        dict to a :obj:`~pyproj.crs.CRS` object.

        .. warning:: Parameters may be lost if a mapping
            from the CF parameter is not found. For best results
            store the WKT of the projection in the crs_wkt attribute.

        Parameters
        ----------
        in_cf: dict
            CF version of the projection.
        errcheck: bool, optional
            If True, will warn when parameters are ignored. Defaults to False.

        Returns
        -------
        CRS
        """
        in_cf = in_cf.copy()  # preserve user input
        if "crs_wkt" in in_cf:
            return CRS(in_cf["crs_wkt"])
        elif "spatial_ref" in in_cf:  # for previous supported WKT key
            return CRS(in_cf["spatial_ref"])

        grid_mapping_name = in_cf.pop("grid_mapping_name", None)
        if grid_mapping_name is None:
            raise CRSError("CF projection parameters missing 'grid_mapping_name'")
        proj_name = GRID_MAPPING_NAME_MAP.get(grid_mapping_name)
        if proj_name is None:
            raise CRSError(
                "Unsupported grid mapping name: {}".format(grid_mapping_name)
            )
        proj_dict = {"proj": proj_name}
        if grid_mapping_name == "rotated_latitude_longitude":
            proj_dict["o_proj"] = "latlon"
        elif grid_mapping_name == "oblique_mercator":
            try:
                proj_dict["lonc"] = in_cf.pop("longitude_of_projection_origin")
            except KeyError:
                pass

        if "standard_parallel" in in_cf:
            standard_parallel = in_cf.pop("standard_parallel")
            if isinstance(standard_parallel, list):
                proj_dict["lat_1"] = standard_parallel[0]
                proj_dict["lat_2"] = standard_parallel[1]
            else:
                proj_dict["lat_1"] = standard_parallel

        # The values are opposite to sweep_angle_axis
        if "fixed_angle_axis" in in_cf:
            proj_dict["sweep"] = {"x": "y", "y": "x"}[
                in_cf.pop("fixed_angle_axis").lower()
            ]

        skipped_params = []
        for cf_param, proj_val in in_cf.items():
            try:
                proj_dict[PROJ_PARAM_MAP[cf_param]] = proj_val
            except KeyError:
                skipped_params.append(cf_param)

        if errcheck and skipped_params:
            warnings.warn(
                "CF parameters not mapped to PROJ: {}".format(tuple(skipped_params))
            )

        return CRS(proj_dict)

    def __eq__(self, other):
        try:
            other = CRS.from_user_input(other)
        except CRSError:
            return False
        return super(CRS, self).__eq__(other)

    def __reduce__(self):
        """special method that allows CRS instance to be pickled"""
        return self.__class__, (self.srs,)

    def __hash__(self):
        return hash(self.to_wkt())

    def __str__(self):
        return self.srs

    def __repr__(self):
        # get axis/coordinate system information
        axis_info_list = []

        def extent_axis(axis_list):
            for axis_info in axis_list:
                axis_info_list.extend(["- ", str(axis_info), "\n"])

        source_crs_repr = ""
        sub_crs_repr = ""
        if self.axis_info:
            extent_axis(self.axis_info)
            coordinate_system_name = str(self.coordinate_system)
        elif self.is_bound:
            extent_axis(self.source_crs.axis_info)
            coordinate_system_name = str(self.source_crs.coordinate_system)
            source_crs_repr = "Source CRS: {}\n".format(self.source_crs.name)
        else:
            coordinate_system_names = []
            sub_crs_repr_list = ["Sub CRS:\n"]
            for sub_crs in self.sub_crs_list:
                extent_axis(sub_crs.axis_info)
                coordinate_system_names.append(str(sub_crs.coordinate_system))
                sub_crs_repr_list.extend(["- ", sub_crs.name, "\n"])
            coordinate_system_name = "|".join(coordinate_system_names)
            sub_crs_repr = "".join(sub_crs_repr_list)
        axis_info_str = "".join(axis_info_list)

        # get coordinate operation repr
        coordinate_operation = ""
        if self.coordinate_operation:
            coordinate_operation = "".join(
                [
                    "Coordinate Operation:\n",
                    "- name: ",
                    str(self.coordinate_operation),
                    "\n" "- method: ",
                    str(self.coordinate_operation.method_name),
                    "\n",
                ]
            )

        # get SRS representation
        srs_repr = self.to_string()
        srs_repr = srs_repr if len(srs_repr) <= 50 else " ".join([srs_repr[:50], "..."])
        string_repr = (
            "<{type_name}: {srs_repr}>\n"
            "Name: {name}\n"
            "Axis Info [{coordinate_system}]:\n"
            "{axis_info_str}"
            "Area of Use:\n"
            "{area_of_use}\n"
            "{coordinate_operation}"
            "Datum: {datum}\n"
            "- Ellipsoid: {ellipsoid}\n"
            "- Prime Meridian: {prime_meridian}\n"
            "{source_crs_repr}"
            "{sub_crs_repr}"
        ).format(
            type_name=self.type_name,
            srs_repr=srs_repr,
            name=self.name,
            axis_info_str=axis_info_str or "- undefined\n",
            area_of_use=self.area_of_use or "- undefined",
            coordinate_system=coordinate_system_name or "undefined",
            coordinate_operation=coordinate_operation,
            datum=self.datum,
            ellipsoid=self.ellipsoid or "undefined",
            prime_meridian=self.prime_meridian or "undefined",
            source_crs_repr=source_crs_repr,
            sub_crs_repr=sub_crs_repr,
        )
        return string_repr
