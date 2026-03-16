"""
This module interfaces with PROJ to produce a pythonic interface
to the coordinate reference system (CRS) information.
"""

# pylint: disable=too-many-lines
import json
import re
import threading
import warnings
from collections.abc import Callable
from typing import Any, Optional

from pyproj._crs import (
    _CRS,
    AreaOfUse,
    AuthorityMatchInfo,
    Axis,
    CoordinateOperation,
    CoordinateSystem,
    Datum,
    Ellipsoid,
    PrimeMeridian,
    _load_proj_json,
    is_proj,
    is_wkt,
)
from pyproj.crs._cf1x8 import (
    _GEOGRAPHIC_GRID_MAPPING_NAME_MAP,
    _GRID_MAPPING_NAME_MAP,
    _INVERSE_GEOGRAPHIC_GRID_MAPPING_NAME_MAP,
    _INVERSE_GRID_MAPPING_NAME_MAP,
    _horizontal_datum_from_params,
    _try_list_if_string,
)
from pyproj.crs.coordinate_operation import ToWGS84Transformation
from pyproj.crs.coordinate_system import Cartesian2DCS, Ellipsoidal2DCS, VerticalCS
from pyproj.enums import ProjVersion, WktVersion
from pyproj.exceptions import CRSError
from pyproj.geod import Geod

_RE_PROJ_PARAM = re.compile(
    r"""
    \+              # parameter starts with '+' character
    (?P<param>\w+)    # capture parameter name
    \=?             # match both key only and key-value parameters
    (?P<value>\S+)? # capture all characters up to next space (None if no value)
    \s*?            # consume remaining whitespace, if any
""",
    re.X,
)


class CRSLocal(threading.local):
    """
    Threading local instance for cython CRS class.

    For more details, see:
    https://github.com/pyproj4/pyproj/issues/782
    """

    def __init__(self):
        self.crs = None  # Initialises in each thread
        super().__init__()


def _prepare_from_dict(projparams: dict, allow_json: bool = True) -> str:
    if not isinstance(projparams, dict):
        raise CRSError("CRS input is not a dict")
    # check if it is a PROJ JSON dict
    if "proj" not in projparams and "init" not in projparams and allow_json:
        return json.dumps(projparams)
    # convert a dict to a proj string.
    pjargs = []
    for key, value in projparams.items():
        # the towgs84 as list
        if isinstance(value, (list, tuple)):
            value = ",".join([str(val) for val in value])
        # issue 183 (+ no_rot)
        if value is None or str(value) == "True":
            pjargs.append(f"+{key}")
        elif str(value) == "False":
            pass
        else:
            pjargs.append(f"+{key}={value}")
    return _prepare_from_string(" ".join(pjargs))


def _prepare_from_proj_string(in_crs_string: str) -> str:
    in_crs_string = re.sub(r"[\s+]?=[\s+]?", "=", in_crs_string.lstrip())
    # make sure the projection starts with +proj or +init
    starting_params = ("+init", "+proj", "init", "proj")
    if not in_crs_string.startswith(starting_params):
        kvpairs: list[str] = []
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
            "'+init=<authority>:<code>' syntax is deprecated. "
            "'<authority>:<code>' is the preferred initialization method. "
            "When making the change, be mindful of axis order changes: "
            "https://pyproj4.github.io/pyproj/stable/gotchas.html"
            "#axis-order-changes-in-proj-6",
            FutureWarning,
            stacklevel=2,
        )
    return in_crs_string


def _prepare_from_string(in_crs_string: str) -> str:
    if not isinstance(in_crs_string, str):
        raise CRSError("CRS input is not a string")
    if not in_crs_string:
        raise CRSError(f"CRS string is empty or invalid: {in_crs_string!r}")
    if "{" in in_crs_string:
        # may be json, try to decode it
        try:
            crs_dict = json.loads(in_crs_string, strict=False)
        except ValueError as err:
            raise CRSError("CRS appears to be JSON but is not valid") from err

        if not crs_dict:
            raise CRSError("CRS is empty JSON")
        in_crs_string = _prepare_from_dict(crs_dict)
    elif is_proj(in_crs_string):
        in_crs_string = _prepare_from_proj_string(in_crs_string)
    return in_crs_string


def _prepare_from_authority(auth_name: str, auth_code: str | int):
    return f"{auth_name}:{auth_code}"


def _prepare_from_epsg(auth_code: str | int):
    return _prepare_from_authority("EPSG", auth_code)


def _is_epsg_code(auth_code: Any) -> bool:
    if isinstance(auth_code, int):
        return True
    if isinstance(auth_code, str) and auth_code.isnumeric():
        return True
    if hasattr(auth_code, "shape") and auth_code.shape == ():
        return True
    return False


class CRS:
    """
    A pythonic Coordinate Reference System manager.

    .. versionadded:: 2.0.0

    See: :c:func:`proj_create`

    The functionality is based on other fantastic projects:

    * `rasterio <https://github.com/mapbox/rasterio/blob/c13f0943b95c0eaa36ff3f620bd91107aa67b381/rasterio/_crs.pyx>`_
    * `opendatacube <https://github.com/opendatacube/datacube-core/blob/83bae20d2a2469a6417097168fd4ede37fd2abe5/datacube/utils/geometry/_base.py>`_

    Attributes
    ----------
    srs: str
        The string form of the user input used to create the CRS.

    """  # noqa: E501

    def __init__(self, projparams: Any | None = None, **kwargs) -> None:
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
          - A :class:`pyproj.crs.CRS` class

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
        >>> print(crs_utm.coordinate_operation.to_wkt(pretty=True))
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
        >>> f"+a={geod.a:.0f} +f={geod.f:.8f}"
        '+a=6378137 +f=0.00335281'
        >>> crs.is_projected
        True
        >>> crs.is_geographic
        False
        """
        projstring = ""

        if projparams:
            if isinstance(projparams, _CRS):
                projstring = projparams.srs
            elif _is_epsg_code(projparams):
                projstring = _prepare_from_epsg(projparams)
            elif isinstance(projparams, str):
                projstring = _prepare_from_string(projparams)
            elif isinstance(projparams, dict):
                projstring = _prepare_from_dict(projparams)
            elif isinstance(projparams, (list, tuple)) and len(projparams) == 2:
                projstring = _prepare_from_authority(*projparams)
            elif hasattr(projparams, "to_wkt"):
                projstring = projparams.to_wkt()
            else:
                raise CRSError(f"Invalid CRS input: {projparams!r}")

        if kwargs:
            projkwargs = _prepare_from_dict(kwargs, allow_json=False)
            projstring = _prepare_from_string(" ".join((projstring, projkwargs)))

        self.srs = projstring
        self._local = CRSLocal()
        if isinstance(projparams, _CRS):
            self._local.crs = projparams
        else:
            self._local.crs = _CRS(self.srs)

    @property
    def _crs(self):
        """
        Retrieve the Cython based _CRS object for this thread.
        """
        if self._local.crs is None:
            self._local.crs = _CRS(self.srs)
        return self._local.crs

    @classmethod
    def from_authority(cls, auth_name: str, code: str | int) -> "CRS":
        """
        .. versionadded:: 2.2.0

        Make a CRS from an authority name and authority code

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
        return cls.from_user_input(_prepare_from_authority(auth_name, code))

    @classmethod
    def from_epsg(cls, code: str | int) -> "CRS":
        """Make a CRS from an EPSG code

        Parameters
        ----------
        code : int or str
            An EPSG code.

        Returns
        -------
        CRS
        """
        return cls.from_user_input(_prepare_from_epsg(code))

    @classmethod
    def from_proj4(cls, in_proj_string: str) -> "CRS":
        """
        .. versionadded:: 2.2.0

        Make a CRS from a PROJ string

        Parameters
        ----------
        in_proj_string : str
            A PROJ string.

        Returns
        -------
        CRS
        """
        if not is_proj(in_proj_string):
            raise CRSError(f"Invalid PROJ string: {in_proj_string}")
        return cls.from_user_input(_prepare_from_proj_string(in_proj_string))

    @classmethod
    def from_wkt(cls, in_wkt_string: str) -> "CRS":
        """
        .. versionadded:: 2.2.0

        Make a CRS from a WKT string

        Parameters
        ----------
        in_wkt_string : str
            A WKT string.

        Returns
        -------
        CRS
        """
        if not is_wkt(in_wkt_string):
            raise CRSError(f"Invalid WKT string: {in_wkt_string}")
        return cls.from_user_input(_prepare_from_string(in_wkt_string))

    @classmethod
    def from_string(cls, in_crs_string: str) -> "CRS":
        """
        Make a CRS from:

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
        return cls.from_user_input(_prepare_from_string(in_crs_string))

    def to_string(self) -> str:
        """
        .. versionadded:: 2.2.0

        Convert the CRS to a string.

        It attempts to convert it to the authority string.
        Otherwise, it uses the string format of the user
        input to create the CRS.

        Returns
        -------
        str
        """
        auth_info = self.to_authority(min_confidence=100)
        if auth_info:
            return ":".join(auth_info)
        return self.srs

    @classmethod
    def from_user_input(cls, value: Any, **kwargs) -> "CRS":
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
          - A :class:`pyproj.crs.CRS` class

        Parameters
        ----------
        value : obj
            A Python int, dict, or str.

        Returns
        -------
        CRS
        """
        if isinstance(value, cls):
            return value
        return cls(value, **kwargs)

    def get_geod(self) -> Geod | None:
        """
        Returns
        -------
        pyproj.geod.Geod:
            Geod object based on the ellipsoid.
        """
        if self.ellipsoid is None:
            return None
        return Geod(
            a=self.ellipsoid.semi_major_metre,
            rf=self.ellipsoid.inverse_flattening,
            b=self.ellipsoid.semi_minor_metre,
        )

    @classmethod
    def from_dict(cls, proj_dict: dict) -> "CRS":
        """
        .. versionadded:: 2.2.0

        Make a CRS from a dictionary of PROJ parameters.

        Parameters
        ----------
        proj_dict : str
            PROJ params in dict format.

        Returns
        -------
        CRS
        """
        return cls.from_user_input(_prepare_from_dict(proj_dict))

    @classmethod
    def from_json(cls, crs_json: str) -> "CRS":
        """
        .. versionadded:: 2.4.0

        Create CRS from a CRS JSON string.

        Parameters
        ----------
        crs_json: str
            CRS JSON string.

        Returns
        -------
        CRS
        """
        return cls.from_user_input(_load_proj_json(crs_json))

    @classmethod
    def from_json_dict(cls, crs_dict: dict) -> "CRS":
        """
        .. versionadded:: 2.4.0

        Create CRS from a JSON dictionary.

        Parameters
        ----------
        crs_dict: dict
            CRS dictionary.

        Returns
        -------
        CRS
        """
        return cls.from_user_input(json.dumps(crs_dict))

    def to_dict(self) -> dict:
        """
        .. versionadded:: 2.2.0

        Converts the CRS to dictionary of PROJ parameters.

        .. warning:: You will likely lose important projection
          information when converting to a PROJ string from
          another format. See: https://proj.org/faq.html#what-is-the-best-format-for-describing-coordinate-reference-systems

        Returns
        -------
        dict:
            PROJ params in dict format.

        """  # noqa: E501

        proj_string = self.to_proj4()
        if proj_string is None:
            return {}

        def _parse(val):
            if val.lower() == "true":
                return True
            if val.lower() == "false":
                return False
            try:
                return int(val)
            except ValueError:
                pass
            try:
                return float(val)
            except ValueError:
                pass
            return _try_list_if_string(val)

        proj_dict = {}
        for param in _RE_PROJ_PARAM.finditer(proj_string):
            key, value = param.groups()
            if value is not None:
                value = _parse(value)
            if value is not False:
                proj_dict[key] = value

        return proj_dict

    def to_cf(
        self,
        wkt_version: WktVersion | str = WktVersion.WKT2_2019,
        errcheck: bool = False,
    ) -> dict:
        """
        .. versionadded:: 2.2.0

        This converts a :obj:`pyproj.crs.CRS` object
        to a Climate and Forecast (CF) Grid Mapping Version 1.8 dict.

        :ref:`build_crs_cf`

        Parameters
        ----------
        wkt_version: str or pyproj.enums.WktVersion
            Version of WKT supported by CRS.to_wkt.
            Default is :attr:`pyproj.enums.WktVersion.WKT2_2019`.
        errcheck: bool, default=False
            If True, will warn when parameters are ignored.

        Returns
        -------
        dict:
            CF-1.8 version of the projection.

        """
        # pylint: disable=too-many-branches,too-many-return-statements
        cf_dict: dict[str, Any] = {"crs_wkt": self.to_wkt(wkt_version)}

        # handle bound CRS
        if (
            self.is_bound
            and self.coordinate_operation
            and self.coordinate_operation.towgs84
            and self.source_crs
        ):
            sub_cf: dict[str, Any] = self.source_crs.to_cf(
                wkt_version=wkt_version,
                errcheck=errcheck,
            )
            sub_cf.pop("crs_wkt")
            cf_dict.update(sub_cf)
            cf_dict["towgs84"] = self.coordinate_operation.towgs84
            return cf_dict

        # handle compound CRS
        if self.is_compound:
            for sub_crs in self.sub_crs_list:
                sub_cf = sub_crs.to_cf(wkt_version=wkt_version, errcheck=errcheck)
                sub_cf.pop("crs_wkt")
                cf_dict.update(sub_cf)
            return cf_dict

        # handle vertical CRS
        if self.is_vertical:
            vert_json = self.to_json_dict()
            if "geoid_model" in vert_json:
                cf_dict["geoid_name"] = vert_json["geoid_model"]["name"]
            if self.datum:
                cf_dict["geopotential_datum_name"] = self.datum.name
            return cf_dict

        # write out datum parameters
        if self.ellipsoid:
            cf_dict.update(
                semi_major_axis=self.ellipsoid.semi_major_metre,
                semi_minor_axis=self.ellipsoid.semi_minor_metre,
                inverse_flattening=self.ellipsoid.inverse_flattening,
            )
            cf_dict["reference_ellipsoid_name"] = self.ellipsoid.name
        if self.prime_meridian:
            cf_dict["longitude_of_prime_meridian"] = self.prime_meridian.longitude
            cf_dict["prime_meridian_name"] = self.prime_meridian.name

        # handle geographic CRS
        if self.geodetic_crs:
            cf_dict["geographic_crs_name"] = self.geodetic_crs.name
            if self.geodetic_crs.datum:
                cf_dict["horizontal_datum_name"] = self.geodetic_crs.datum.name

        if self.is_geographic:
            if self.coordinate_operation:
                if (
                    self.coordinate_operation.method_name.lower()
                    not in _INVERSE_GEOGRAPHIC_GRID_MAPPING_NAME_MAP
                ):
                    if errcheck:
                        warnings.warn(
                            "Unsupported coordinate operation: "
                            f"{self.coordinate_operation.method_name}"
                        )
                    return {"crs_wkt": cf_dict["crs_wkt"]}
                cf_dict.update(
                    _INVERSE_GEOGRAPHIC_GRID_MAPPING_NAME_MAP[
                        self.coordinate_operation.method_name.lower()
                    ](self.coordinate_operation)
                )
            else:
                cf_dict["grid_mapping_name"] = "latitude_longitude"
            return cf_dict

        # handle projected CRS
        coordinate_operation = None
        if not self.is_bound and self.is_projected:
            coordinate_operation = self.coordinate_operation
            cf_dict["projected_crs_name"] = self.name
        coordinate_operation_name = (
            None
            if not coordinate_operation
            else coordinate_operation.method_name.lower().replace(" ", "_")
        )
        if coordinate_operation_name not in _INVERSE_GRID_MAPPING_NAME_MAP:
            if errcheck:
                if coordinate_operation:
                    warnings.warn(
                        "Unsupported coordinate operation: "
                        f"{coordinate_operation.method_name}"
                    )
                else:
                    warnings.warn("Coordinate operation not found.")

            return {"crs_wkt": cf_dict["crs_wkt"]}

        cf_dict.update(
            _INVERSE_GRID_MAPPING_NAME_MAP[coordinate_operation_name](
                coordinate_operation
            )
        )
        return cf_dict

    @staticmethod
    def from_cf(
        in_cf: dict,
        ellipsoidal_cs: Any | None = None,
        cartesian_cs: Any | None = None,
        vertical_cs: Any | None = None,
    ) -> "CRS":
        """
        .. versionadded:: 2.2.0

        .. versionadded:: 3.0.0 ellipsoidal_cs, cartesian_cs, vertical_cs

        This converts a Climate and Forecast (CF) Grid Mapping Version 1.8
        dict to a :obj:`pyproj.crs.CRS` object.

        :ref:`build_crs_cf`

        Parameters
        ----------
        in_cf: dict
            CF version of the projection.
        ellipsoidal_cs: Any, optional
            Input to create an Ellipsoidal Coordinate System.
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or an Ellipsoidal Coordinate System created from :ref:`coordinate_system`.
        cartesian_cs: Any, optional
            Input to create a Cartesian Coordinate System.
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or :class:`pyproj.crs.coordinate_system.Cartesian2DCS`.
        vertical_cs: Any, optional
            Input to create a Vertical Coordinate System accepted by
            :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or :class:`pyproj.crs.coordinate_system.VerticalCS`

        Returns
        -------
        CRS
        """
        # pylint: disable=too-many-branches

        unknown_names = ("unknown", "undefined")
        if "crs_wkt" in in_cf:
            return CRS(in_cf["crs_wkt"])
        if "spatial_ref" in in_cf:  # for previous supported WKT key
            return CRS(in_cf["spatial_ref"])

        grid_mapping_name = in_cf.get("grid_mapping_name")
        if grid_mapping_name is None:
            raise CRSError("CF projection parameters missing 'grid_mapping_name'")

        # build datum if possible
        datum = _horizontal_datum_from_params(in_cf)

        # build geographic CRS
        try:
            geographic_conversion_method: None | Callable = (
                _GEOGRAPHIC_GRID_MAPPING_NAME_MAP[grid_mapping_name]
            )
        except KeyError:
            geographic_conversion_method = None

        geographic_crs_name = in_cf.get("geographic_crs_name")
        if datum:
            geographic_crs: CRS = GeographicCRS(
                name=geographic_crs_name or "undefined",
                datum=datum,
                ellipsoidal_cs=ellipsoidal_cs,
            )
        elif geographic_crs_name and geographic_crs_name not in unknown_names:
            geographic_crs = CRS(geographic_crs_name)
            if ellipsoidal_cs is not None:
                geographic_crs_json = geographic_crs.to_json_dict()
                geographic_crs_json["coordinate_system"] = (
                    CoordinateSystem.from_user_input(ellipsoidal_cs).to_json_dict()
                )
                geographic_crs = CRS(geographic_crs_json)
        else:
            geographic_crs = GeographicCRS(ellipsoidal_cs=ellipsoidal_cs)
        if grid_mapping_name == "latitude_longitude":
            return geographic_crs
        if geographic_conversion_method is not None:
            return DerivedGeographicCRS(
                base_crs=geographic_crs,
                conversion=geographic_conversion_method(in_cf),
                ellipsoidal_cs=ellipsoidal_cs,
            )

        # build projected CRS
        try:
            conversion_method = _GRID_MAPPING_NAME_MAP[grid_mapping_name]
        except KeyError:
            raise CRSError(
                f"Unsupported grid mapping name: {grid_mapping_name}"
            ) from None
        projected_crs = ProjectedCRS(
            name=in_cf.get("projected_crs_name", "undefined"),
            conversion=conversion_method(in_cf),
            geodetic_crs=geographic_crs,
            cartesian_cs=cartesian_cs,
        )

        # build bound CRS if exists
        bound_crs = None
        if "towgs84" in in_cf:
            bound_crs = BoundCRS(
                source_crs=projected_crs,
                target_crs="WGS 84",
                transformation=ToWGS84Transformation(
                    projected_crs.geodetic_crs, *_try_list_if_string(in_cf["towgs84"])
                ),
            )
        if "geopotential_datum_name" not in in_cf:
            return bound_crs or projected_crs

        # build Vertical CRS
        vertical_crs = VerticalCRS(
            name="undefined",
            datum=in_cf["geopotential_datum_name"],
            geoid_model=in_cf.get("geoid_name"),
            vertical_cs=vertical_cs,
        )

        # build compound CRS
        return CompoundCRS(
            name="undefined", components=[bound_crs or projected_crs, vertical_crs]
        )

    def cs_to_cf(self) -> list[dict]:
        """
        .. versionadded:: 3.0.0

        This converts all coordinate systems (cs) in the CRS
        to a list of Climate and Forecast (CF) Version 1.8 dicts.

        :ref:`build_crs_cf`

        Returns
        -------
        list[dict]:
            CF-1.8 version of the coordinate systems.
        """
        cf_axis_list = []

        def rotated_pole(crs):
            try:
                return (
                    crs.coordinate_operation
                    and crs.coordinate_operation.method_name.lower()
                    in _INVERSE_GEOGRAPHIC_GRID_MAPPING_NAME_MAP
                )
            except KeyError:
                return False

        if self.type_name == "Temporal CRS" and self.datum:
            datum_json = self.datum.to_json_dict()
            origin = datum_json.get("time_origin", "1875-05-20").strip().rstrip("zZ")
            if len(origin) == 4:
                origin = f"{origin}-01-01"
            axis = self.axis_info[0]
            cf_temporal_axis = {
                "standard_name": "time",
                "long_name": "time",
                "calendar": (
                    datum_json.get("calendar", "proleptic_gregorian")
                    .lower()
                    .replace(" ", "_")
                ),
                "axis": "T",
            }
            unit_name = axis.unit_name.lower().replace("calendar", "").strip()
            # no units for TemporalDateTime
            if unit_name:
                cf_temporal_axis["units"] = f"{unit_name} since {origin}"
            cf_axis_list.append(cf_temporal_axis)
        if self.coordinate_system:
            cf_axis_list.extend(
                self.coordinate_system.to_cf(rotated_pole=rotated_pole(self))
            )
        elif self.is_bound and self.source_crs and self.source_crs.coordinate_system:
            cf_axis_list.extend(
                self.source_crs.coordinate_system.to_cf(
                    rotated_pole=rotated_pole(self.source_crs)
                )
            )
        else:
            for sub_crs in self.sub_crs_list:
                cf_axis_list.extend(sub_crs.cs_to_cf())
        return cf_axis_list

    def is_exact_same(self, other: Any) -> bool:
        """
        Check if the CRS objects are the exact same.

        Parameters
        ----------
        other: Any
            Check if the other CRS is the exact same to this object.
            If the other object is not a CRS, it will try to create one.
            On Failure, it will return False.

        Returns
        -------
        bool
        """
        try:
            other = CRS.from_user_input(other)
        except CRSError:
            return False
        return self._crs.is_exact_same(other._crs)

    def equals(self, other: Any, ignore_axis_order: bool = False) -> bool:
        """

        .. versionadded:: 2.5.0

        Check if the CRS objects are equivalent.

        Parameters
        ----------
        other: Any
            Check if the other object is equivalent to this object.
            If the other object is not a CRS, it will try to create one.
            On Failure, it will return False.
        ignore_axis_order: bool, default=False
            If True, it will compare the CRS class and ignore the axis order.

        Returns
        -------
        bool
        """
        try:
            other = CRS.from_user_input(other)
        except CRSError:
            return False
        return self._crs.equals(other._crs, ignore_axis_order=ignore_axis_order)

    @property
    def geodetic_crs(self) -> Optional["CRS"]:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        CRS:
            The geodeticCRS / geographicCRS from the CRS.

        """
        return (
            None
            if self._crs.geodetic_crs is None
            else self.__class__(self._crs.geodetic_crs)
        )

    @property
    def source_crs(self) -> Optional["CRS"]:
        """
        The base CRS of a BoundCRS or a DerivedCRS/ProjectedCRS,
        or the source CRS of a CoordinateOperation.

        Returns
        -------
        CRS
        """
        return (
            None
            if self._crs.source_crs is None
            else self.__class__(self._crs.source_crs)
        )

    @property
    def target_crs(self) -> Optional["CRS"]:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        CRS:
            The hub CRS of a BoundCRS or the target CRS of a CoordinateOperation.

        """
        return (
            None
            if self._crs.target_crs is None
            else self.__class__(self._crs.target_crs)
        )

    @property
    def sub_crs_list(self) -> list["CRS"]:
        """
        If the CRS is a compound CRS, it will return a list of sub CRS objects.

        Returns
        -------
        list[CRS]
        """
        return [self.__class__(sub_crs) for sub_crs in self._crs.sub_crs_list]

    @property
    def utm_zone(self) -> str | None:
        """
        .. versionadded:: 2.6.0

        Finds the UTM zone in a Projected CRS, Bound CRS, or Compound CRS

        Returns
        -------
        str | None:
            The UTM zone number and letter if applicable.
        """
        if self.is_bound and self.source_crs:
            return self.source_crs.utm_zone
        if self.sub_crs_list:
            for sub_crs in self.sub_crs_list:
                if sub_crs.utm_zone:
                    return sub_crs.utm_zone
        elif (
            self.coordinate_operation
            and "UTM ZONE" in self.coordinate_operation.name.upper()
        ):
            return self.coordinate_operation.name.upper().split("UTM ZONE ")[-1]
        return None

    @property
    def name(self) -> str:
        """
        Returns
        -------
        str:
            The name of the CRS (from :cpp:func:`proj_get_name`).
        """
        return self._crs.name

    @property
    def type_name(self) -> str:
        """
        Returns
        -------
        str:
            The name of the type of the CRS object.
        """
        return self._crs.type_name

    @property
    def axis_info(self) -> list[Axis]:
        """
        Retrieves all relevant axis information in the CRS.
        If it is a Bound CRS, it gets the axis list from the Source CRS.
        If it is a Compound CRS, it gets the axis list from the Sub CRS list.

        Returns
        -------
        list[Axis]:
            The list of axis information.
        """
        return self._crs.axis_info

    @property
    def area_of_use(self) -> AreaOfUse | None:
        """
        Returns
        -------
        AreaOfUse:
            The area of use object with associated attributes.
        """
        return self._crs.area_of_use

    @property
    def ellipsoid(self) -> Ellipsoid | None:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        Ellipsoid:
            The ellipsoid object with associated attributes.
        """
        return self._crs.ellipsoid

    @property
    def prime_meridian(self) -> PrimeMeridian | None:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        PrimeMeridian:
            The prime meridian object with associated attributes.
        """
        return self._crs.prime_meridian

    @property
    def datum(self) -> Datum | None:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        Datum
        """
        return self._crs.datum

    @property
    def coordinate_system(self) -> CoordinateSystem | None:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        CoordinateSystem
        """
        return self._crs.coordinate_system

    @property
    def coordinate_operation(self) -> CoordinateOperation | None:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        CoordinateOperation
        """
        return self._crs.coordinate_operation

    @property
    def remarks(self) -> str:
        """
        .. versionadded:: 2.4.0

        Returns
        -------
        str:
            Remarks about object.
        """
        return self._crs.remarks

    @property
    def scope(self) -> str:
        """
        .. versionadded:: 2.4.0

        Returns
        -------
        str:
            Scope of object.
        """
        return self._crs.scope

    def to_wkt(
        self,
        version: WktVersion | str = WktVersion.WKT2_2019,
        pretty: bool = False,
        output_axis_rule: bool | None = None,
    ) -> str:
        """
        Convert the projection to a WKT string.

        Version options:
          - WKT2_2015
          - WKT2_2015_SIMPLIFIED
          - WKT2_2019
          - WKT2_2019_SIMPLIFIED
          - WKT1_GDAL
          - WKT1_ESRI

        .. versionadded:: 3.6.0 output_axis_rule

        Parameters
        ----------
        version: pyproj.enums.WktVersion, optional
            The version of the WKT output.
            Default is :attr:`pyproj.enums.WktVersion.WKT2_2019`.
        pretty: bool, default=False
            If True, it will set the output to be a multiline string.
        output_axis_rule: bool, optional, default=None
            If True, it will set the axis rule on any case. If false, never.
            None for AUTO, that depends on the CRS and version.

        Returns
        -------
        str
        """
        wkt = self._crs.to_wkt(
            version=version, pretty=pretty, output_axis_rule=output_axis_rule
        )
        if wkt is None:
            raise CRSError(
                f"CRS cannot be converted to a WKT string of a '{version}' version. "
                "Select a different version of a WKT string or edit your CRS."
            )
        return wkt

    def to_json(self, pretty: bool = False, indentation: int = 2) -> str:
        """
        .. versionadded:: 2.4.0

        Convert the object to a JSON string.

        Parameters
        ----------
        pretty: bool, default=False
            If True, it will set the output to be a multiline string.
        indentation: int, default=2
            If pretty is True, it will set the width of the indentation.

        Returns
        -------
        str
        """
        proj_json = self._crs.to_json(pretty=pretty, indentation=indentation)
        if proj_json is None:
            raise CRSError("CRS cannot be converted to a PROJ JSON string.")
        return proj_json

    def to_json_dict(self) -> dict:
        """
        .. versionadded:: 2.4.0

        Convert the object to a JSON dictionary.

        Returns
        -------
        dict
        """
        return self._crs.to_json_dict()

    def to_proj4(self, version: ProjVersion | int = ProjVersion.PROJ_5) -> str:
        """
        Convert the projection to a PROJ string.

        .. warning:: You will likely lose important projection
          information when converting to a PROJ string from
          another format. See:
          https://proj.org/faq.html#what-is-the-best-format-for-describing-coordinate-reference-systems

        Parameters
        ----------
        version: pyproj.enums.ProjVersion
            The version of the PROJ string output.
            Default is :attr:`pyproj.enums.ProjVersion.PROJ_4`.

        Returns
        -------
        str
        """  # noqa: E501
        proj = self._crs.to_proj4(version=version)
        if proj is None:
            raise CRSError("CRS cannot be converted to a PROJ string.")
        return proj

    def to_epsg(self, min_confidence: int = 70) -> int | None:
        """
        Return the EPSG code best matching the CRS
        or None if it a match is not found.

        Example:

        >>> from pyproj import CRS
        >>> ccs = CRS("EPSG:4328")
        >>> ccs.to_epsg()
        4328

        If the CRS is bound, you can attempt to get an epsg code from
        the source CRS:

        >>> from pyproj import CRS
        >>> ccs = CRS("+proj=geocent +datum=WGS84 +towgs84=0,0,0")
        >>> ccs.to_epsg()
        >>> ccs.source_crs.to_epsg()
        4978
        >>> ccs == CRS.from_epsg(4978)
        False

        Parameters
        ----------
        min_confidence: int, default=70
            A value between 0-100 where 100 is the most confident.
            :ref:`min_confidence`


        Returns
        -------
        int | None:
            The best matching EPSG code matching the confidence level.
        """
        return self._crs.to_epsg(min_confidence=min_confidence)

    def to_authority(self, auth_name: str | None = None, min_confidence: int = 70):
        """
        .. versionadded:: 2.2.0

        Return the authority name and code best matching the CRS
        or None if it a match is not found.

        Example:

        >>> from pyproj import CRS
        >>> ccs = CRS("EPSG:4328")
        >>> ccs.to_authority()
        ('EPSG', '4328')

        If the CRS is bound, you can get an authority from
        the source CRS:

        >>> from pyproj import CRS
        >>> ccs = CRS("+proj=geocent +datum=WGS84 +towgs84=0,0,0")
        >>> ccs.to_authority()
        >>> ccs.source_crs.to_authority()
        ('EPSG', '4978')
        >>> ccs == CRS.from_authorty('EPSG', '4978')
        False

        Parameters
        ----------
        auth_name: str, optional
            The name of the authority to filter by.
        min_confidence: int, default=70
            A value between 0-100 where 100 is the most confident.
            :ref:`min_confidence`

        Returns
        -------
        tuple(str, str) or None:
            The best matching (<auth_name>, <code>) for the confidence level.
        """
        return self._crs.to_authority(
            auth_name=auth_name, min_confidence=min_confidence
        )

    def list_authority(
        self, auth_name: str | None = None, min_confidence: int = 70
    ) -> list[AuthorityMatchInfo]:
        """
        .. versionadded:: 3.2.0

        Return the authority names and codes best matching the CRS.

        Example:

        >>> from pyproj import CRS
        >>> ccs = CRS("EPSG:4328")
        >>> ccs.list_authority()
        [AuthorityMatchInfo(auth_name='EPSG', code='4326', confidence=100)]

        If the CRS is bound, you can get an authority from
        the source CRS:

        >>> from pyproj import CRS
        >>> ccs = CRS("+proj=geocent +datum=WGS84 +towgs84=0,0,0")
        >>> ccs.list_authority()
        []
        >>> ccs.source_crs.list_authority()
        [AuthorityMatchInfo(auth_name='EPSG', code='4978', confidence=70)]
        >>> ccs == CRS.from_authorty('EPSG', '4978')
        False

        Parameters
        ----------
        auth_name: str, optional
            The name of the authority to filter by.
        min_confidence: int, default=70
            A value between 0-100 where 100 is the most confident.
            :ref:`min_confidence`

        Returns
        -------
        list[AuthorityMatchInfo]:
            List of authority matches for the CRS.
        """
        return self._crs.list_authority(
            auth_name=auth_name, min_confidence=min_confidence
        )

    def to_3d(self, name: str | None = None) -> "CRS":
        """
        .. versionadded:: 3.1.0

        Convert the current CRS to the 3D version if it makes sense.

        New vertical axis attributes:
          - ellipsoidal height
          - oriented upwards
          - metre units

        Parameters
        ----------
        name: str, optional
            CRS name. Defaults to use the name of the original CRS.

        Returns
        -------
        CRS
        """
        return self.__class__(self._crs.to_3d(name=name))

    def to_2d(self, name: str | None = None) -> "CRS":
        """
        .. versionadded:: 3.6.0

        Convert the current CRS to the 2D version if it makes sense.

        Parameters
        ----------
        name: str, optional
            CRS name. Defaults to use the name of the original CRS.

        Returns
        -------
        CRS
        """
        return self.__class__(self._crs.to_2d(name=name))

    @property
    def is_geographic(self) -> bool:
        """
        This checks if the CRS is geographic.
        It will check if it has a geographic CRS
        in the sub CRS if it is a compound CRS and will check if
        the source CRS is geographic if it is a bound CRS.

        Returns
        -------
        bool:
            True if the CRS is in geographic (lon/lat) coordinates.
        """
        return self._crs.is_geographic

    @property
    def is_projected(self) -> bool:
        """
        This checks if the CRS is projected.
        It will check if it has a projected CRS
        in the sub CRS if it is a compound CRS and will check if
        the source CRS is projected if it is a bound CRS.

        Returns
        -------
        bool:
            True if CRS is projected.
        """
        return self._crs.is_projected

    @property
    def is_vertical(self) -> bool:
        """
        .. versionadded:: 2.2.0

        This checks if the CRS is vertical.
        It will check if it has a vertical CRS
        in the sub CRS if it is a compound CRS and will check if
        the source CRS is vertical if it is a bound CRS.

        Returns
        -------
        bool:
            True if CRS is vertical.
        """
        return self._crs.is_vertical

    @property
    def is_bound(self) -> bool:
        """
        Returns
        -------
        bool:
            True if CRS is bound.
        """
        return self._crs.is_bound

    @property
    def is_compound(self) -> bool:
        """
        .. versionadded:: 3.1.0

        Returns
        -------
        bool:
            True if CRS is compound.
        """
        return self._crs.is_compound

    @property
    def is_engineering(self) -> bool:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        bool:
            True if CRS is local/engineering.
        """
        return self._crs.is_engineering

    @property
    def is_geocentric(self) -> bool:
        """
        This checks if the CRS is geocentric and
        takes into account if the CRS is bound.

        Returns
        -------
        bool:
            True if CRS is in geocentric (x/y) coordinates.
        """
        return self._crs.is_geocentric

    @property
    def is_derived(self):
        """
        .. versionadded:: 3.2.0

        Returns
        -------
        bool:
            True if CRS is a Derived CRS.
        """
        return self._crs.is_derived

    @property
    def is_deprecated(self) -> bool:
        """
        .. versionadded:: 3.7.0

        Check if the CRS is deprecated

        Returns
        -------
        bool
        """
        return self._crs.is_deprecated

    def get_non_deprecated(self) -> list["CRS"]:
        """
        .. versionadded:: 3.7.0

        Return a list of non-deprecated objects related to this.

        Returns
        -------
        list[CRS]
        """
        return self._crs.get_non_deprecated()

    def __eq__(self, other: object) -> bool:
        return self.equals(other)

    def __getstate__(self) -> dict[str, str]:
        return {"srs": self.srs}

    def __setstate__(self, state: dict[str, Any]):
        self.__dict__.update(state)
        self._local = CRSLocal()

    def __hash__(self) -> int:
        return hash(self.to_wkt())

    def __str__(self) -> str:
        return self.srs

    def __repr__(self) -> str:
        # get axis information
        axis_info_list: list[str] = []
        for axis in self.axis_info:
            axis_info_list.extend(["- ", str(axis), "\n"])
        axis_info_str = "".join(axis_info_list)

        # get coordinate system & sub CRS info
        source_crs_repr = ""
        sub_crs_repr = ""
        if self.coordinate_system and self.coordinate_system.axis_list:
            coordinate_system_name = str(self.coordinate_system)
        elif self.is_bound and self.source_crs:
            coordinate_system_name = str(self.source_crs.coordinate_system)
            source_crs_repr = f"Source CRS: {self.source_crs.name}\n"
        else:
            coordinate_system_names = []
            sub_crs_repr_list = ["Sub CRS:\n"]
            for sub_crs in self.sub_crs_list:
                coordinate_system_names.append(str(sub_crs.coordinate_system))
                sub_crs_repr_list.extend(["- ", sub_crs.name, "\n"])
            coordinate_system_name = "|".join(coordinate_system_names)
            sub_crs_repr = "".join(sub_crs_repr_list)

        # get coordinate operation repr
        coordinate_operation = ""
        if self.coordinate_operation:
            coordinate_operation = "".join(
                [
                    "Coordinate Operation:\n",
                    "- name: ",
                    str(self.coordinate_operation),
                    "\n- method: ",
                    self.coordinate_operation.method_name,
                    "\n",
                ]
            )

        # get SRS representation
        srs_repr = self.to_string()
        srs_repr = srs_repr if len(srs_repr) <= 50 else " ".join([srs_repr[:50], "..."])
        axis_info_str = axis_info_str or "- undefined\n"
        return (
            f"<{self.type_name}: {srs_repr}>\n"
            f"Name: {self.name}\n"
            f"Axis Info [{coordinate_system_name or 'undefined'}]:\n"
            f"{axis_info_str}"
            "Area of Use:\n"
            f"{self.area_of_use or '- undefined'}\n"
            f"{coordinate_operation}"
            f"Datum: {self.datum}\n"
            f"- Ellipsoid: {self.ellipsoid or 'undefined'}\n"
            f"- Prime Meridian: {self.prime_meridian or 'undefined'}\n"
            f"{source_crs_repr}"
            f"{sub_crs_repr}"
        )


class CustomConstructorCRS(CRS):
    """
    This class is a base class for CRS classes
    that use a different constructor than the main CRS class.

    .. versionadded:: 3.2.0

    See: https://github.com/pyproj4/pyproj/issues/847
    """

    @property
    def _expected_types(self) -> tuple[str, ...]:
        """
        These are the type names of the CRS class
        that are expected when using the from_* methods.
        """
        raise NotImplementedError

    def _check_type(self):
        """
        This validates that the type of the CRS is expected
        when using the from_* methods.
        """
        if self.type_name not in self._expected_types:
            raise CRSError(
                f"Invalid type {self.type_name}. Expected {self._expected_types}."
            )

    @classmethod
    def from_user_input(cls, value: Any, **kwargs) -> "CRS":
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
          - A :class:`pyproj.crs.CRS` class

        Parameters
        ----------
        value : obj
            A Python int, dict, or str.

        Returns
        -------
        CRS
        """
        if isinstance(value, cls):
            return value
        crs = cls.__new__(cls)
        super(CustomConstructorCRS, crs).__init__(value, **kwargs)
        crs._check_type()
        return crs

    @property
    def geodetic_crs(self) -> Optional["CRS"]:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        CRS:
            The geodeticCRS / geographicCRS from the CRS.

        """
        return None if self._crs.geodetic_crs is None else CRS(self._crs.geodetic_crs)

    @property
    def source_crs(self) -> Optional["CRS"]:
        """
        The base CRS of a BoundCRS or a DerivedCRS/ProjectedCRS,
        or the source CRS of a CoordinateOperation.

        Returns
        -------
        CRS
        """
        return None if self._crs.source_crs is None else CRS(self._crs.source_crs)

    @property
    def target_crs(self) -> Optional["CRS"]:
        """
        .. versionadded:: 2.2.0

        Returns
        -------
        CRS:
            The hub CRS of a BoundCRS or the target CRS of a CoordinateOperation.

        """
        return None if self._crs.target_crs is None else CRS(self._crs.target_crs)

    @property
    def sub_crs_list(self) -> list["CRS"]:
        """
        If the CRS is a compound CRS, it will return a list of sub CRS objects.

        Returns
        -------
        list[CRS]
        """
        return [CRS(sub_crs) for sub_crs in self._crs.sub_crs_list]

    def to_3d(self, name: str | None = None) -> "CRS":
        """
        .. versionadded:: 3.1.0

        Convert the current CRS to the 3D version if it makes sense.

        New vertical axis attributes:
          - ellipsoidal height
          - oriented upwards
          - metre units

        Parameters
        ----------
        name: str, optional
            CRS name. Defaults to use the name of the original CRS.

        Returns
        -------
        CRS
        """
        return CRS(self._crs.to_3d(name=name))


class GeographicCRS(CustomConstructorCRS):
    """
    .. versionadded:: 2.5.0

    This class is for building a Geographic CRS
    """

    _expected_types = ("Geographic CRS", "Geographic 2D CRS", "Geographic 3D CRS")

    def __init__(
        self,
        name: str = "undefined",
        datum: Any = "urn:ogc:def:ensemble:EPSG::6326",
        ellipsoidal_cs: Any | None = None,
    ) -> None:
        """
        Parameters
        ----------
        name: str, default="undefined"
            Name of the CRS.
        datum: Any, default="urn:ogc:def:ensemble:EPSG::6326"
            Anything accepted by :meth:`pyproj.crs.Datum.from_user_input` or
            a :class:`pyproj.crs.datum.CustomDatum`.
        ellipsoidal_cs: Any, optional
            Input to create an Ellipsoidal Coordinate System.
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or an Ellipsoidal Coordinate System created from :ref:`coordinate_system`.
        """
        datum = Datum.from_user_input(datum).to_json_dict()
        geographic_crs_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "GeographicCRS",
            "name": name,
            "coordinate_system": CoordinateSystem.from_user_input(
                ellipsoidal_cs or Ellipsoidal2DCS()
            ).to_json_dict(),
        }
        if datum["type"] == "DatumEnsemble":
            geographic_crs_json["datum_ensemble"] = datum
        else:
            geographic_crs_json["datum"] = datum
        super().__init__(geographic_crs_json)


class DerivedGeographicCRS(CustomConstructorCRS):
    """
    .. versionadded:: 2.5.0

    This class is for building a Derived Geographic CRS
    """

    _expected_types = (
        "Derived Geographic CRS",
        "Derived Geographic 2D CRS",
        "Derived Geographic 3D CRS",
    )

    def __init__(
        self,
        base_crs: Any,
        conversion: Any,
        ellipsoidal_cs: Any | None = None,
        name: str = "undefined",
    ) -> None:
        """
        Parameters
        ----------
        base_crs: Any
            Input to create the Geodetic CRS, a :class:`GeographicCRS` or
            anything accepted by :meth:`pyproj.crs.CRS.from_user_input`.
        conversion: Any
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or a conversion from :ref:`coordinate_operation`.
        ellipsoidal_cs: Any, optional
            Input to create an Ellipsoidal Coordinate System.
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or an Ellipsoidal Coordinate System created from :ref:`coordinate_system`.
        name: str, default="undefined"
            Name of the CRS.
        """
        derived_geographic_crs_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "DerivedGeographicCRS",
            "name": name,
            "base_crs": CRS.from_user_input(base_crs).to_json_dict(),
            "conversion": CoordinateOperation.from_user_input(
                conversion
            ).to_json_dict(),
            "coordinate_system": CoordinateSystem.from_user_input(
                ellipsoidal_cs or Ellipsoidal2DCS()
            ).to_json_dict(),
        }
        super().__init__(derived_geographic_crs_json)


class GeocentricCRS(CustomConstructorCRS):
    """
    .. versionadded:: 3.2.0

    This class is for building a Geocentric CRS
    """

    _expected_types = ("Geocentric CRS",)

    def __init__(
        self,
        name: str = "undefined",
        datum: Any = "urn:ogc:def:datum:EPSG::6326",
    ) -> None:
        """
        Parameters
        ----------
        name: str, default="undefined"
            Name of the CRS.
        datum: Any, default="urn:ogc:def:datum:EPSG::6326"
            Anything accepted by :meth:`pyproj.crs.Datum.from_user_input` or
            a :class:`pyproj.crs.datum.CustomDatum`.
        """
        geocentric_crs_json = {
            "$schema": ("https://proj.org/schemas/v0.2/projjson.schema.json"),
            "type": "GeodeticCRS",
            "name": name,
            "datum": Datum.from_user_input(datum).to_json_dict(),
            "coordinate_system": {
                "subtype": "Cartesian",
                "axis": [
                    {
                        "name": "Geocentric X",
                        "abbreviation": "X",
                        "direction": "geocentricX",
                        "unit": "metre",
                    },
                    {
                        "name": "Geocentric Y",
                        "abbreviation": "Y",
                        "direction": "geocentricY",
                        "unit": "metre",
                    },
                    {
                        "name": "Geocentric Z",
                        "abbreviation": "Z",
                        "direction": "geocentricZ",
                        "unit": "metre",
                    },
                ],
            },
        }
        super().__init__(geocentric_crs_json)


class ProjectedCRS(CustomConstructorCRS):
    """
    .. versionadded:: 2.5.0

    This class is for building a Projected CRS.
    """

    _expected_types = ("Projected CRS", "Derived Projected CRS")

    def __init__(
        self,
        conversion: Any,
        name: str = "undefined",
        cartesian_cs: Any | None = None,
        geodetic_crs: Any | None = None,
    ) -> None:
        """
        Parameters
        ----------
        conversion: Any
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or a conversion from :ref:`coordinate_operation`.
        name: str, optional
            The name of the Projected CRS. Default is undefined.
        cartesian_cs: Any, optional
            Input to create a Cartesian Coordinate System.
            Anything accepted by :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or :class:`pyproj.crs.coordinate_system.Cartesian2DCS`.
        geodetic_crs: Any, optional
            Input to create the Geodetic CRS, a :class:`GeographicCRS` or
            anything accepted by :meth:`pyproj.crs.CRS.from_user_input`.
        """
        proj_crs_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "ProjectedCRS",
            "name": name,
            "base_crs": CRS.from_user_input(
                geodetic_crs or GeographicCRS()
            ).to_json_dict(),
            "conversion": CoordinateOperation.from_user_input(
                conversion
            ).to_json_dict(),
            "coordinate_system": CoordinateSystem.from_user_input(
                cartesian_cs or Cartesian2DCS()
            ).to_json_dict(),
        }
        super().__init__(proj_crs_json)


class VerticalCRS(CustomConstructorCRS):
    """
    .. versionadded:: 2.5.0

    This class is for building a Vertical CRS.

    .. warning:: geoid_model support only exists in PROJ >= 6.3.0

    """

    _expected_types = ("Vertical CRS",)

    def __init__(
        self,
        name: str,
        datum: Any,
        vertical_cs: Any | None = None,
        geoid_model: str | None = None,
    ) -> None:
        """
        Parameters
        ----------
        name: str
            The name of the Vertical CRS (e.g. NAVD88 height).
        datum: Any
            Anything accepted by :meth:`pyproj.crs.Datum.from_user_input`
        vertical_cs: Any, optional
            Input to create a Vertical Coordinate System accepted by
            :meth:`pyproj.crs.CoordinateSystem.from_user_input`
            or :class:`pyproj.crs.coordinate_system.VerticalCS`
        geoid_model: str, optional
            The name of the GEOID Model (e.g. GEOID12B).
        """
        vert_crs_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "VerticalCRS",
            "name": name,
            "datum": Datum.from_user_input(datum).to_json_dict(),
            "coordinate_system": CoordinateSystem.from_user_input(
                vertical_cs or VerticalCS()
            ).to_json_dict(),
        }
        if geoid_model is not None:
            vert_crs_json["geoid_model"] = {"name": geoid_model}

        super().__init__(vert_crs_json)


class CompoundCRS(CustomConstructorCRS):
    """
    .. versionadded:: 2.5.0

    This class is for building a Compound CRS.
    """

    _expected_types = ("Compound CRS",)

    def __init__(self, name: str, components: list[Any]) -> None:
        """
        Parameters
        ----------
        name: str
            The name of the Compound CRS.
        components: list[Any], optional
            List of CRS to create a Compound Coordinate System.
            List of anything accepted by :meth:`pyproj.crs.CRS.from_user_input`
        """
        compound_crs_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "CompoundCRS",
            "name": name,
            "components": [
                CRS.from_user_input(component).to_json_dict()
                for component in components
            ],
        }

        super().__init__(compound_crs_json)


class BoundCRS(CustomConstructorCRS):
    """
    .. versionadded:: 2.5.0

    This class is for building a Bound CRS.
    """

    _expected_types = ("Bound CRS",)

    def __init__(self, source_crs: Any, target_crs: Any, transformation: Any) -> None:
        """
        Parameters
        ----------
        source_crs: Any
            Input to create a source CRS.
        target_crs: Any
            Input to create the target CRS.
        transformation: Any
            Input to create the transformation.
        """
        bound_crs_json = {
            "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
            "type": "BoundCRS",
            "source_crs": CRS.from_user_input(source_crs).to_json_dict(),
            "target_crs": CRS.from_user_input(target_crs).to_json_dict(),
            "transformation": CoordinateOperation.from_user_input(
                transformation
            ).to_json_dict(),
        }

        super().__init__(bound_crs_json)
