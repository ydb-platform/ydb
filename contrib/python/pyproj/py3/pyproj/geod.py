"""
The Geod class can perform forward and inverse geodetic, or
Great Circle, computations.  The forward computation involves
determining latitude, longitude and back azimuth of a terminus
point given the latitude and longitude of an initial point, plus
azimuth and distance. The inverse computation involves
determining the forward and back azimuths and distance given the
latitudes and longitudes of an initial and terminus point.
"""

__all__ = [
    "Geod",
    "GeodIntermediateFlag",
    "GeodIntermediateReturn",
    "geodesic_version_str",
    "pj_ellps",
    "reverse_azimuth",
]

import math
import warnings
from typing import Any

from pyproj._geod import Geod as _Geod
from pyproj._geod import GeodIntermediateReturn, geodesic_version_str
from pyproj._geod import reverse_azimuth as _reverse_azimuth
from pyproj.enums import GeodIntermediateFlag
from pyproj.exceptions import GeodError
from pyproj.list import get_ellps_map
from pyproj.utils import DataType, _convertback, _copytobuffer

pj_ellps = get_ellps_map()


def _params_from_ellps_map(ellps: str) -> tuple[float, float, float, float, bool]:
    """
    Build Geodesic parameters from PROJ ellips map

    Parameter
    ---------
    ellps: str
        The name of the ellipse in the map.

    Returns
    -------
    tuple[float, float, float, float, bool]

    """
    ellps_dict = pj_ellps[ellps]
    semi_major_axis, semi_minor_axis, flattening, eccentricity_squared = (
        _params_from_kwargs(ellps_dict)
    )
    sphere = False
    if ellps_dict["description"] == "Normal Sphere":
        sphere = True
    return semi_major_axis, semi_minor_axis, flattening, eccentricity_squared, sphere


def _params_from_kwargs(kwargs: dict) -> tuple[float, float, float, float]:
    """
    Build Geodesic parameters from input kwargs:

    - a: the semi-major axis (required).

    Need least one of these parameters.

    - b: the semi-minor axis
    - rf: the reciprocal flattening
    - f: flattening
    - es: eccentricity squared


    Parameter
    ---------
    kwargs: dict
        The input kwargs for an ellipse.

    Returns
    -------
    tuple[float, float, float, float]

    """
    semi_major_axis = kwargs["a"]
    if "b" in kwargs:
        semi_minor_axis = kwargs["b"]
        eccentricity_squared = 1.0 - semi_minor_axis**2 / semi_major_axis**2
        flattening = (semi_major_axis - semi_minor_axis) / semi_major_axis
    elif "rf" in kwargs:
        flattening = 1.0 / kwargs["rf"]
        semi_minor_axis = semi_major_axis * (1.0 - flattening)
        eccentricity_squared = 1.0 - semi_minor_axis**2 / semi_major_axis**2
    elif "f" in kwargs:
        flattening = kwargs["f"]
        semi_minor_axis = semi_major_axis * (1.0 - flattening)
        eccentricity_squared = 1.0 - (semi_minor_axis / semi_major_axis) ** 2
    elif "es" in kwargs:
        eccentricity_squared = kwargs["es"]
        semi_minor_axis = math.sqrt(
            semi_major_axis**2 - eccentricity_squared * semi_major_axis**2
        )
        flattening = (semi_major_axis - semi_minor_axis) / semi_major_axis
    elif "e" in kwargs:
        eccentricity_squared = kwargs["e"] ** 2
        semi_minor_axis = math.sqrt(
            semi_major_axis**2 - eccentricity_squared * semi_major_axis**2
        )
        flattening = (semi_major_axis - semi_minor_axis) / semi_major_axis
    else:
        semi_minor_axis = semi_major_axis
        flattening = 0.0
        eccentricity_squared = 0.0
    return semi_major_axis, semi_minor_axis, flattening, eccentricity_squared


class Geod(_Geod):
    """
    performs forward and inverse geodetic, or Great Circle,
    computations.  The forward computation (using the 'fwd' method)
    involves determining latitude, longitude and back azimuth of a
    terminus point given the latitude and longitude of an initial
    point, plus azimuth and distance. The inverse computation (using
    the 'inv' method) involves determining the forward and back
    azimuths and distance given the latitudes and longitudes of an
    initial and terminus point.

    Attributes
    ----------
    initstring: str
        The string form of the user input used to create the Geod.
    sphere: bool
        If True, it is a sphere.
    a: float
        The ellipsoid equatorial radius, or semi-major axis.
    b: float
        The ellipsoid polar radius, or semi-minor axis.
    es: float
        The 'eccentricity' of the ellipse, squared (1-b2/a2).
    f: float
        The ellipsoid 'flattening' parameter ( (a-b)/a ).

    """

    def __init__(self, initstring: str | None = None, **kwargs) -> None:
        """
        initialize a Geod class instance.

        Geodetic parameters for specifying the ellipsoid
        can be given in a dictionary 'initparams', as keyword arguments,
        or as as proj geod initialization string.

        You can get a dictionary of ellipsoids using :func:`pyproj.get_ellps_map`
        or with the variable `pyproj.pj_ellps`.

        The parameters of the ellipsoid may also be set directly using
        the 'a' (semi-major or equatorial axis radius) keyword, and
        any one of the following keywords: 'b' (semi-minor,
        or polar axis radius), 'e' (eccentricity), 'es' (eccentricity
        squared), 'f' (flattening), or 'rf' (reciprocal flattening).

        See the proj documentation (https://proj.org) for more
        information about specifying ellipsoid parameters.

        Example usage:

        >>> from pyproj import Geod
        >>> g = Geod(ellps='clrk66') # Use Clarke 1866 ellipsoid.
        >>> # specify the lat/lons of some cities.
        >>> boston_lat = 42.+(15./60.); boston_lon = -71.-(7./60.)
        >>> portland_lat = 45.+(31./60.); portland_lon = -123.-(41./60.)
        >>> newyork_lat = 40.+(47./60.); newyork_lon = -73.-(58./60.)
        >>> london_lat = 51.+(32./60.); london_lon = -(5./60.)
        >>> # compute forward and back azimuths, plus distance
        >>> # between Boston and Portland.
        >>> az12,az21,dist = g.inv(boston_lon,boston_lat,portland_lon,portland_lat)
        >>> f"{az12:.3f} {az21:.3f} {dist:.3f}"
        '-66.531 75.654 4164192.708'
        >>> # compute latitude, longitude and back azimuth of Portland,
        >>> # given Boston lat/lon, forward azimuth and distance to Portland.
        >>> endlon, endlat, backaz = g.fwd(boston_lon, boston_lat, az12, dist)
        >>> f"{endlat:.3f} {endlon:.3f} {backaz:.3f}"
        '45.517 -123.683 75.654'
        >>> # compute the azimuths, distances from New York to several
        >>> # cities (pass a list)
        >>> lons1 = 3*[newyork_lon]; lats1 = 3*[newyork_lat]
        >>> lons2 = [boston_lon, portland_lon, london_lon]
        >>> lats2 = [boston_lat, portland_lat, london_lat]
        >>> az12,az21,dist = g.inv(lons1,lats1,lons2,lats2)
        >>> for faz, baz, d in list(zip(az12,az21,dist)):
        ...     f"{faz:7.3f} {baz:8.3f} {d:12.3f}"
        ' 54.663 -123.448   288303.720'
        '-65.463   79.342  4013037.318'
        ' 51.254  -71.576  5579916.651'
        >>> g2 = Geod('+ellps=clrk66') # use proj4 style initialization string
        >>> az12,az21,dist = g2.inv(boston_lon,boston_lat,portland_lon,portland_lat)
        >>> f"{az12:.3f} {az21:.3f} {dist:.3f}"
        '-66.531 75.654 4164192.708'
        """
        # if initparams is a proj-type init string,
        # convert to dict.
        ellpsd: dict[str, str | float] = {}
        if initstring is not None:
            for kvpair in initstring.split():
                # Actually only +a and +b are needed
                # We can ignore safely any parameter that doesn't have a value
                if kvpair.find("=") == -1:
                    continue
                key, val = kvpair.split("=")
                key = key.lstrip("+")
                if key in ["a", "b", "rf", "f", "es", "e"]:
                    ellpsd[key] = float(val)
                else:
                    ellpsd[key] = val
        # merge this dict with kwargs dict.
        kwargs = dict(list(kwargs.items()) + list(ellpsd.items()))
        sphere = False
        if "ellps" in kwargs:
            (
                semi_major_axis,
                semi_minor_axis,
                flattening,
                eccentricity_squared,
                sphere,
            ) = _params_from_ellps_map(kwargs["ellps"])
        else:
            (
                semi_major_axis,
                semi_minor_axis,
                flattening,
                eccentricity_squared,
            ) = _params_from_kwargs(kwargs)

        if math.fabs(flattening) < 1.0e-8:
            sphere = True

        super().__init__(
            semi_major_axis, flattening, sphere, semi_minor_axis, eccentricity_squared
        )

    def fwd(  # pylint: disable=invalid-name
        self,
        lons: Any,
        lats: Any,
        az: Any,
        dist: Any,
        radians: bool = False,
        inplace: bool = False,
        return_back_azimuth: bool = True,
    ) -> tuple[Any, Any, Any]:
        """
        Forward transformation

        Determine longitudes, latitudes and back azimuths of terminus
        points given longitudes and latitudes of initial points,
        plus forward azimuths and distances.

        .. versionadded:: 3.5.0 inplace
        .. versionadded:: 3.5.0 return_back_azimuth

        Accepted numeric scalar or array:

        - :class:`int`
        - :class:`float`
        - :class:`numpy.floating`
        - :class:`numpy.integer`
        - :class:`list`
        - :class:`tuple`
        - :class:`array.array`
        - :class:`numpy.ndarray`
        - :class:`xarray.DataArray`
        - :class:`pandas.Series`

        Parameters
        ----------
        lons: scalar or array
            Longitude(s) of initial point(s)
        lats: scalar or array
            Latitude(s) of initial point(s)
        az: scalar or array
            Forward azimuth(s)
        dist: scalar or array
            Distance(s) between initial and terminus point(s)
            in meters
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.
        inplace: bool, default=False
            If True, will attempt to write the results to the input array
            instead of returning a new array. This will fail if the input
            is not an array in C order with the double data type.
        return_back_azimuth: bool, default=True
            If True, the third return value will be the back azimuth,
            Otherwise, it will be the forward azimuth.

        Returns
        -------
        scalar or array:
            Longitude(s) of terminus point(s)
        scalar or array:
            Latitude(s) of terminus point(s)
        scalar or array:
            Back azimuth(s) or Forward azimuth(s)
        """
        try:
            # Fast-path for scalar input, will raise if invalid types are input
            # and we can fallback below
            return self._fwd_point(
                lons,
                lats,
                az,
                dist,
                radians=radians,
                return_back_azimuth=return_back_azimuth,
            )
        except TypeError:
            pass

        # process inputs, making copies that support buffer API.
        inx, x_data_type = _copytobuffer(lons, inplace=inplace)
        iny, y_data_type = _copytobuffer(lats, inplace=inplace)
        inz, z_data_type = _copytobuffer(az, inplace=inplace)
        ind = _copytobuffer(dist, inplace=inplace)[0]
        self._fwd(
            inx, iny, inz, ind, radians=radians, return_back_azimuth=return_back_azimuth
        )
        # if inputs were lists, tuples or floats, convert back.
        outx = _convertback(x_data_type, inx)
        outy = _convertback(y_data_type, iny)
        outz = _convertback(z_data_type, inz)
        return outx, outy, outz

    def inv(
        self,
        lons1: Any,
        lats1: Any,
        lons2: Any,
        lats2: Any,
        radians: bool = False,
        inplace: bool = False,
        return_back_azimuth: bool = True,
    ) -> tuple[Any, Any, Any]:
        """

        Inverse transformation

        Determine forward and back azimuths, plus distances
        between initial points and terminus points.

        .. versionadded:: 3.5.0 inplace
        .. versionadded:: 3.5.0 return_back_azimuth

        Accepted numeric scalar or array:

        - :class:`int`
        - :class:`float`
        - :class:`numpy.floating`
        - :class:`numpy.integer`
        - :class:`list`
        - :class:`tuple`
        - :class:`array.array`
        - :class:`numpy.ndarray`
        - :class:`xarray.DataArray`
        - :class:`pandas.Series`

        Parameters
        ----------
        lons1: scalar or array
            Longitude(s) of initial point(s)
        lats1: scalar or array
            Latitude(s) of initial point(s)
        lons2: scalar or array
            Longitude(s) of terminus point(s)
        lats2: scalar or array
            Latitude(s) of terminus point(s)
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.
        inplace: bool, default=False
            If True, will attempt to write the results to the input array
            instead of returning a new array. This will fail if the input
            is not an array in C order with the double data type.
        return_back_azimuth: bool, default=True
            If True, the second return value (azi21) will be the back azimuth
            (flipped 180 degrees), Otherwise, it will also be a forward azimuth.

        Returns
        -------
        scalar or array:
            Forward azimuth(s) (azi12)
        scalar or array:
            Back azimuth(s) or Forward azimuth(s) (azi21)
        scalar or array:
            Distance(s) between initial and terminus point(s)
            in meters
        """
        try:
            # Fast-path for scalar input, will raise if invalid types are input
            # and we can fallback below
            return self._inv_point(
                lons1,
                lats1,
                lons2,
                lats2,
                radians=radians,
                return_back_azimuth=return_back_azimuth,
            )
        except TypeError:
            pass

        # process inputs, making copies that support buffer API.
        inx, x_data_type = _copytobuffer(lons1, inplace=inplace)
        iny, y_data_type = _copytobuffer(lats1, inplace=inplace)
        inz, z_data_type = _copytobuffer(lons2, inplace=inplace)
        ind = _copytobuffer(lats2, inplace=inplace)[0]
        self._inv(
            inx, iny, inz, ind, radians=radians, return_back_azimuth=return_back_azimuth
        )
        # if inputs were lists, tuples or floats, convert back.
        outx = _convertback(x_data_type, inx)
        outy = _convertback(y_data_type, iny)
        outz = _convertback(z_data_type, inz)
        return outx, outy, outz

    def npts(
        self,
        lon1: float,
        lat1: float,
        lon2: float,
        lat2: float,
        npts: int,
        radians: bool = False,
        initial_idx: int = 1,
        terminus_idx: int = 1,
    ) -> list:
        """
        .. versionadded:: 3.1.0 initial_idx, terminus_idx

        Given a single initial point and terminus point, returns
        a list of longitude/latitude pairs describing npts equally
        spaced intermediate points along the geodesic between the
        initial and terminus points.

        Similar to inv_intermediate(), but with less options.

        Example usage:

        >>> from pyproj import Geod
        >>> g = Geod(ellps='clrk66') # Use Clarke 1866 ellipsoid.
        >>> # specify the lat/lons of Boston and Portland.
        >>> boston_lat = 42.+(15./60.); boston_lon = -71.-(7./60.)
        >>> portland_lat = 45.+(31./60.); portland_lon = -123.-(41./60.)
        >>> # find ten equally spaced points between Boston and Portland.
        >>> lonlats = g.npts(boston_lon,boston_lat,portland_lon,portland_lat,10)
        >>> for lon,lat in lonlats: f'{lat:.3f} {lon:.3f}'
        '43.528 -75.414'
        '44.637 -79.883'
        '45.565 -84.512'
        '46.299 -89.279'
        '46.830 -94.156'
        '47.149 -99.112'
        '47.251 -104.106'
        '47.136 -109.100'
        '46.805 -114.051'
        '46.262 -118.924'
        >>> # test with radians=True (inputs/outputs in radians, not degrees)
        >>> import math
        >>> dg2rad = math.radians(1.)
        >>> rad2dg = math.degrees(1.)
        >>> lonlats = g.npts(
        ...    dg2rad*boston_lon,
        ...    dg2rad*boston_lat,
        ...    dg2rad*portland_lon,
        ...    dg2rad*portland_lat,
        ...    10,
        ...    radians=True
        ... )
        >>> for lon,lat in lonlats: f'{rad2dg*lat:.3f} {rad2dg*lon:.3f}'
        '43.528 -75.414'
        '44.637 -79.883'
        '45.565 -84.512'
        '46.299 -89.279'
        '46.830 -94.156'
        '47.149 -99.112'
        '47.251 -104.106'
        '47.136 -109.100'
        '46.805 -114.051'
        '46.262 -118.924'

        Parameters
        ----------
        lon1: float
            Longitude of the initial point
        lat1: float
            Latitude of the initial point
        lon2: float
            Longitude of the terminus point
        lat2: float
            Latitude of the terminus point
        npts: int
            Number of points to be returned
            (including initial and/or terminus points, if required)
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.
        initial_idx: int, default=1
            if initial_idx==0 then the initial point would be included in the output
            (as the first point)
        terminus_idx: int, default=1
            if terminus_idx==0 then the terminus point would be included in the output
            (as the last point)
        Returns
        -------
        list of tuples:
            list of (lon, lat) points along the geodesic
            between the initial and terminus points.
        """

        res = self._inv_or_fwd_intermediate(
            lon1=lon1,
            lat1=lat1,
            lon2_or_azi1=lon2,
            lat2=lat2,
            npts=npts,
            del_s=0,
            radians=radians,
            initial_idx=initial_idx,
            terminus_idx=terminus_idx,
            flags=GeodIntermediateFlag.AZIS_DISCARD,
            out_lons=None,
            out_lats=None,
            out_azis=None,
            return_back_azimuth=False,
            is_fwd=False,
        )
        return list(zip(res.lons, res.lats))

    def inv_intermediate(
        self,
        lon1: float,
        lat1: float,
        lon2: float,
        lat2: float,
        npts: int = 0,
        del_s: float = 0,
        initial_idx: int = 1,
        terminus_idx: int = 1,
        radians: bool = False,
        flags: GeodIntermediateFlag = GeodIntermediateFlag.DEFAULT,
        out_lons: Any | None = None,
        out_lats: Any | None = None,
        out_azis: Any | None = None,
        return_back_azimuth: bool | None = None,
    ) -> GeodIntermediateReturn:
        """
        .. versionadded:: 3.1.0
        .. versionadded:: 3.5.0 return_back_azimuth

        Given a single initial point and terminus point,
        and the number of points, returns
        a list of longitude/latitude pairs describing npts equally
        spaced intermediate points along the geodesic between the
        initial and terminus points.

        npts and del_s parameters are mutually exclusive:

        if npts != 0:
            it calculates the distance between the points by
            the distance between the initial point and the
            terminus point divided by npts
            (the number of intermediate points)
        else:
            it calculates the number of intermediate points by
            dividing the distance between the initial and
            terminus points by del_s
            (delimiter distance between two successive points)

        Similar to npts(), but with more options.

        Example usage:

        >>> from pyproj import Geod
        >>> g = Geod(ellps='clrk66') # Use Clarke 1866 ellipsoid.
        >>> # specify the lat/lons of Boston and Portland.
        >>> boston_lat = 42.+(15./60.); boston_lon = -71.-(7./60.)
        >>> portland_lat = 45.+(31./60.); portland_lon = -123.-(41./60.)
        >>> # find ten equally spaced points between Boston and Portland.
        >>> r = g.inv_intermediate(boston_lon,boston_lat,portland_lon,portland_lat,10)
        >>> for lon,lat in zip(r.lons, r.lats): f'{lat:.3f} {lon:.3f}'
        '43.528 -75.414'
        '44.637 -79.883'
        '45.565 -84.512'
        '46.299 -89.279'
        '46.830 -94.156'
        '47.149 -99.112'
        '47.251 -104.106'
        '47.136 -109.100'
        '46.805 -114.051'
        '46.262 -118.924'
        >>> # test with radians=True (inputs/outputs in radians, not degrees)
        >>> import math
        >>> dg2rad = math.radians(1.)
        >>> rad2dg = math.degrees(1.)
        >>> r = g.inv_intermediate(
        ...    dg2rad*boston_lon,
        ...    dg2rad*boston_lat,
        ...    dg2rad*portland_lon,
        ...    dg2rad*portland_lat,
        ...    10,
        ...    radians=True
        ... )
        >>> for lon,lat in zip(r.lons, r.lats): f'{rad2dg*lat:.3f} {rad2dg*lon:.3f}'
        '43.528 -75.414'
        '44.637 -79.883'
        '45.565 -84.512'
        '46.299 -89.279'
        '46.830 -94.156'
        '47.149 -99.112'
        '47.251 -104.106'
        '47.136 -109.100'
        '46.805 -114.051'
        '46.262 -118.924'

        Parameters
        ----------
        lon1: float
            Longitude of the initial point
        lat1: float
            Latitude of the initial point
        lon2: float
            Longitude of the terminus point
        lat2: float
            Latitude of the terminus point
        npts: int, default=0
            Number of points to be returned
            npts == 0 if del_s != 0
        del_s: float, default=0
            delimiter distance between two successive points
            del_s == 0 if npts != 0
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.
        initial_idx: int, default=1
            if initial_idx==0 then the initial point would be included in the output
            (as the first point)
        terminus_idx: int, default=1
            if terminus_idx==0 then the terminus point would be included in the output
            (as the last point)
        flags: GeodIntermediateFlag, default=GeodIntermediateFlag.DEFAULT
            * 1st - round/ceil/trunc (see ``GeodIntermediateFlag.NPTS_*``)
            * 2nd - update del_s to the new npts or not
                    (see ``GeodIntermediateFlag.DEL_S_*``)
            * 3rd - if out_azis=None, indicates if to save or discard the azimuths
                    (see ``GeodIntermediateFlag.AZIS_*``)
            * default - round npts, update del_s accordingly, discard azis
        out_lons: array, :class:`numpy.ndarray`, optional
            Longitude(s) of the intermediate point(s)
            If None then buffers would be allocated internnaly
        out_lats: array, :class:`numpy.ndarray`, optional
            Latitudes(s) of the intermediate point(s)
            If None then buffers would be allocated internnaly
        out_azis: array, :class:`numpy.ndarray`, optional
            az12(s) of the intermediate point(s)
            If None then buffers would be allocated internnaly
            unless requested otherwise by the flags
        return_back_azimuth: bool, default=True
            if True, out_azis will store the back azimuth,
            Otherwise, out_azis will store the forward azimuth.

        Returns
        -------
        GeodIntermediateReturn:
            number of points, distance and output arrays (GeodIntermediateReturn docs)
        """
        if return_back_azimuth is None:
            return_back_azimuth = True
            warnings.warn(
                "Back azimuth is being returned by default to be compatible with fwd()"
                "This is a breaking change for pyproj 3.5+."
                "To avoid this warning, set return_back_azimuth=True."
                "Otherwise, to restore old behaviour, set return_back_azimuth=False."
                "This warning will be removed in future version."
            )
        return super()._inv_or_fwd_intermediate(
            lon1=lon1,
            lat1=lat1,
            lon2_or_azi1=lon2,
            lat2=lat2,
            npts=npts,
            del_s=del_s,
            radians=radians,
            initial_idx=initial_idx,
            terminus_idx=terminus_idx,
            flags=int(flags),
            out_lons=out_lons,
            out_lats=out_lats,
            out_azis=out_azis,
            return_back_azimuth=return_back_azimuth,
            is_fwd=False,
        )

    def fwd_intermediate(
        self,
        lon1: float,
        lat1: float,
        azi1: float,
        npts: int,
        del_s: float,
        initial_idx: int = 1,
        terminus_idx: int = 1,
        radians: bool = False,
        flags: GeodIntermediateFlag = GeodIntermediateFlag.DEFAULT,
        out_lons: Any | None = None,
        out_lats: Any | None = None,
        out_azis: Any | None = None,
        return_back_azimuth: bool | None = None,
    ) -> GeodIntermediateReturn:
        """
        .. versionadded:: 3.1.0
        .. versionadded:: 3.5.0 return_back_azimuth

        Given a single initial point and azimuth, number of points (npts)
        and delimiter distance between two successive points (del_s), returns
        a list of longitude/latitude pairs describing npts equally
        spaced intermediate points along the geodesic between the
        initial and terminus points.

        Example usage:

        >>> from pyproj import Geod
        >>> g = Geod(ellps='clrk66') # Use Clarke 1866 ellipsoid.
        >>> # specify the lat/lons of Boston and Portland.
        >>> boston_lat = 42.+(15./60.); boston_lon = -71.-(7./60.)
        >>> portland_lat = 45.+(31./60.); portland_lon = -123.-(41./60.)
        >>> az12,az21,dist = g.inv(boston_lon,boston_lat,portland_lon,portland_lat)
        >>> # find ten equally spaced points between Boston and Portland.
        >>> npts = 10
        >>> del_s = dist/(npts+1)
        >>> r = g.fwd_intermediate(boston_lon,boston_lat,az12,npts=npts,del_s=del_s)
        >>> for lon,lat in zip(r.lons, r.lats): f'{lat:.3f} {lon:.3f}'
        '43.528 -75.414'
        '44.637 -79.883'
        '45.565 -84.512'
        '46.299 -89.279'
        '46.830 -94.156'
        '47.149 -99.112'
        '47.251 -104.106'
        '47.136 -109.100'
        '46.805 -114.051'
        '46.262 -118.924'
        >>> # test with radians=True (inputs/outputs in radians, not degrees)
        >>> import math
        >>> dg2rad = math.radians(1.)
        >>> rad2dg = math.degrees(1.)
        >>> r = g.fwd_intermediate(
        ...    dg2rad*boston_lon,
        ...    dg2rad*boston_lat,
        ...    dg2rad*az12,
        ...    npts=npts,
        ...    del_s=del_s,
        ...    radians=True
        ... )
        >>> for lon,lat in zip(r.lons, r.lats): f'{rad2dg*lat:.3f} {rad2dg*lon:.3f}'
        '43.528 -75.414'
        '44.637 -79.883'
        '45.565 -84.512'
        '46.299 -89.279'
        '46.830 -94.156'
        '47.149 -99.112'
        '47.251 -104.106'
        '47.136 -109.100'
        '46.805 -114.051'
        '46.262 -118.924'

        Parameters
        ----------
        lon1: float
            Longitude of the initial point
        lat1: float
            Latitude of the initial point
        azi1: float
            Azimuth from the initial point towards the terminus point
        npts: int
            Number of points to be returned
            (including initial and/or terminus points, if required)
        del_s: float
            delimiter distance between two successive points
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.
        initial_idx: int, default=1
            if initial_idx==0 then the initial point would be included in the output
            (as the first point)
        terminus_idx: int, default=1
            if terminus_idx==0 then the terminus point would be included in the output
            (as the last point)
        flags: GeodIntermediateFlag, default=GeodIntermediateFlag.DEFAULT
            * 1st - round/ceil/trunc (see ``GeodIntermediateFlag.NPTS_*``)
            * 2nd - update del_s to the new npts or not
                    (see ``GeodIntermediateFlag.DEL_S_*``)
            * 3rd - if out_azis=None, indicates if to save or discard the azimuths
                    (see ``GeodIntermediateFlag.AZIS_*``)
            * default - round npts, update del_s accordingly, discard azis
        out_lons: array, :class:`numpy.ndarray`, optional
            Longitude(s) of the intermediate point(s)
            If None then buffers would be allocated internnaly
        out_lats: array, :class:`numpy.ndarray`, optional
            Latitudes(s) of the intermediate point(s)
            If None then buffers would be allocated internnaly
        out_azis: array, :class:`numpy.ndarray`, optional
            az12(s) of the intermediate point(s)
            If None then buffers would be allocated internnaly
            unless requested otherwise by the flags
        return_back_azimuth: bool, default=True
            if True, out_azis will store the back azimuth,
            Otherwise, out_azis will store the forward azimuth.

        Returns
        -------
        GeodIntermediateReturn:
            number of points, distance and output arrays (GeodIntermediateReturn docs)
        """
        if return_back_azimuth is None:
            return_back_azimuth = True
            warnings.warn(
                "Back azimuth is being returned by default to be compatible with inv()"
                "This is a breaking change for pyproj 3.5+."
                "To avoid this warning, set return_back_azimuth=True."
                "Otherwise, to restore old behaviour, set return_back_azimuth=False."
                "This warning will be removed in future version."
            )
        return super()._inv_or_fwd_intermediate(
            lon1=lon1,
            lat1=lat1,
            lon2_or_azi1=azi1,
            lat2=math.nan,
            npts=npts,
            del_s=del_s,
            radians=radians,
            initial_idx=initial_idx,
            terminus_idx=terminus_idx,
            flags=int(flags),
            out_lons=out_lons,
            out_lats=out_lats,
            out_azis=out_azis,
            return_back_azimuth=return_back_azimuth,
            is_fwd=True,
        )

    def line_length(self, lons: Any, lats: Any, radians: bool = False) -> float:
        """
        .. versionadded:: 2.3.0

        Calculate the total distance between points along a line (meters).

        >>> from pyproj import Geod
        >>> geod = Geod('+a=6378137 +f=0.0033528106647475126')
        >>> lats = [-72.9, -71.9, -74.9, -74.3, -77.5, -77.4, -71.7, -65.9, -65.7,
        ...         -66.6, -66.9, -69.8, -70.0, -71.0, -77.3, -77.9, -74.7]
        >>> lons = [-74, -102, -102, -131, -163, 163, 172, 140, 113,
        ...         88, 59, 25, -4, -14, -33, -46, -61]
        >>> total_length = geod.line_length(lons, lats)
        >>> f"{total_length:.3f}"
        '14259605.611'


        Parameters
        ----------
        lons: array, :class:`numpy.ndarray`, list, tuple, or scalar
            The longitude points along a line.
        lats: array, :class:`numpy.ndarray`, list, tuple, or scalar
            The latitude points along a line.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.

        Returns
        -------
        float:
            The total length of the line (meters).
        """
        # process inputs, making copies that support buffer API.
        inx = _copytobuffer(lons)[0]
        iny = _copytobuffer(lats)[0]
        return self._line_length(inx, iny, radians=radians)

    def line_lengths(self, lons: Any, lats: Any, radians: bool = False) -> Any:
        """
        .. versionadded:: 2.3.0

        Calculate the distances between points along a line (meters).

        >>> from pyproj import Geod
        >>> geod = Geod(ellps="WGS84")
        >>> lats = [-72.9, -71.9, -74.9]
        >>> lons = [-74, -102, -102]
        >>> for line_length in geod.line_lengths(lons, lats):
        ...     f"{line_length:.3f}"
        '943065.744'
        '334805.010'

        Parameters
        ----------
        lons: array, :class:`numpy.ndarray`, list, tuple, or scalar
            The longitude points along a line.
        lats: array, :class:`numpy.ndarray`, list, tuple, or scalar
            The latitude points along a line.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.

        Returns
        -------
        array, :class:`numpy.ndarray`, list, tuple, or scalar:
            The total length of the line (meters).
        """
        # process inputs, making copies that support buffer API.
        inx, x_data_type = _copytobuffer(lons)
        iny = _copytobuffer(lats)[0]
        self._line_length(inx, iny, radians=radians)
        line_lengths = _convertback(x_data_type, inx)
        return line_lengths if x_data_type == DataType.FLOAT else line_lengths[:-1]

    def polygon_area_perimeter(
        self, lons: Any, lats: Any, radians: bool = False
    ) -> tuple[float, float]:
        """
        .. versionadded:: 2.3.0

        A simple interface for computing the area (meters^2) and perimeter (meters)
        of a geodesic polygon.

        Arbitrarily complex polygons are allowed. In the case self-intersecting
        of polygons the area is accumulated "algebraically", e.g., the areas of
        the 2 loops in a figure-8 polygon will partially cancel. There's no need
        to "close" the polygon by repeating the first vertex. The area returned
        is signed with counter-clockwise traversal being treated as positive.

        .. note:: lats should be in the range [-90 deg, 90 deg].


        Example usage:

        >>> from pyproj import Geod
        >>> geod = Geod('+a=6378137 +f=0.0033528106647475126')
        >>> lats = [-72.9, -71.9, -74.9, -74.3, -77.5, -77.4, -71.7, -65.9, -65.7,
        ...         -66.6, -66.9, -69.8, -70.0, -71.0, -77.3, -77.9, -74.7]
        >>> lons = [-74, -102, -102, -131, -163, 163, 172, 140, 113,
        ...         88, 59, 25, -4, -14, -33, -46, -61]
        >>> poly_area, poly_perimeter = geod.polygon_area_perimeter(lons, lats)
        >>> f"{poly_area:.1f} {poly_perimeter:.1f}"
        '13376856682207.4 14710425.4'


        Parameters
        ----------
        lons: array, :class:`numpy.ndarray`, list, tuple, or scalar
            An array of longitude values.
        lats: array, :class:`numpy.ndarray`, list, tuple, or scalar
            An array of latitude values.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.

        Returns
        -------
        (float, float):
            The geodesic area (meters^2) and perimeter (meters) of the polygon.
        """
        return self._polygon_area_perimeter(
            _copytobuffer(lons)[0], _copytobuffer(lats)[0], radians=radians
        )

    def geometry_length(self, geometry, radians: bool = False) -> float:
        """
        .. versionadded:: 2.3.0

        Returns the geodesic length (meters) of the shapely geometry.

        If it is a Polygon, it will return the sum of the
        lengths along the perimeter.
        If it is a MultiPolygon or MultiLine, it will return
        the sum of the lengths.

        Example usage:

        >>> from pyproj import Geod
        >>> from shapely.geometry import Point, LineString
        >>> line_string = LineString([Point(1, 2), Point(3, 4)])
        >>> geod = Geod(ellps="WGS84")
        >>> f"{geod.geometry_length(line_string):.3f}"
        '313588.397'

        Parameters
        ----------
        geometry: :class:`shapely.geometry.BaseGeometry`
            The geometry to calculate the length from.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.

        Returns
        -------
        float:
            The total geodesic length of the geometry (meters).
        """
        try:
            return self.line_length(*geometry.xy, radians=radians)  # type: ignore[misc]
        except (AttributeError, NotImplementedError):
            pass
        if hasattr(geometry, "exterior"):
            return self.geometry_length(geometry.exterior, radians=radians)
        if hasattr(geometry, "geoms"):
            total_length = 0.0
            for geom in geometry.geoms:
                total_length += self.geometry_length(geom, radians=radians)
            return total_length
        raise GeodError("Invalid geometry provided.")

    def geometry_area_perimeter(
        self, geometry, radians: bool = False
    ) -> tuple[float, float]:
        """
        .. versionadded:: 2.3.0

        A simple interface for computing the area (meters^2) and perimeter (meters)
        of a geodesic polygon as a shapely geometry.

        Arbitrarily complex polygons are allowed.  In the case self-intersecting
        of polygons the area is accumulated "algebraically", e.g., the areas of
        the 2 loops in a figure-8 polygon will partially cancel.  There's no need
        to "close" the polygon by repeating the first vertex.

        .. note:: lats should be in the range [-90 deg, 90 deg].

        .. note:: | There are a few limitations :
                  | - only works with areas up to half the size of the globe ;
                  | - certain large polygons may return negative values.

        .. warning:: The area returned is signed with counter-clockwise (CCW) traversal
                     being treated as positive. For polygons, holes should use the
                     opposite traversal to the exterior (if the exterior is CCW, the
                     holes/interiors should be CW). You can use `shapely.ops.orient` to
                     modify the orientation.

        If it is a Polygon, it will return the area and exterior perimeter.
        It will subtract the area of the interior holes.
        If it is a MultiPolygon or MultiLine, it will return
        the sum of the areas and perimeters of all geometries.


        Example usage:

        >>> from pyproj import Geod
        >>> from shapely.geometry import LineString, Point, Polygon
        >>> geod = Geod(ellps="WGS84")
        >>> poly_area, poly_perimeter = geod.geometry_area_perimeter(
        ...     Polygon(
        ...         LineString([
        ...             Point(1, 1), Point(10, 1), Point(10, 10), Point(1, 10)
        ...         ]),
        ...         holes=[LineString([Point(1, 2), Point(3, 4), Point(5, 2)])],
        ...     )
        ... )
        >>> f"{poly_area:.0f} {poly_perimeter:.0f}"
        '944373881400 3979008'


        Parameters
        ----------
        geometry: :class:`shapely.geometry.BaseGeometry`
            The geometry to calculate the area and perimeter from.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.
            Otherwise, the data is assumed to be in degrees.

        Returns
        -------
        (float, float):
            The geodesic area (meters^2) and perimeter (meters) of the polygon.
        """
        try:
            return self.polygon_area_perimeter(  # type: ignore[misc]
                *geometry.xy, radians=radians
            )
        except (AttributeError, NotImplementedError):
            pass
        # polygon
        if hasattr(geometry, "exterior"):
            total_area, total_perimeter = self.geometry_area_perimeter(
                geometry.exterior, radians=radians
            )
            # subtract area of holes
            for hole in geometry.interiors:
                area, _ = self.geometry_area_perimeter(hole, radians=radians)
                total_area += area
            return total_area, total_perimeter
        # multi geometries
        if hasattr(geometry, "geoms"):
            total_area = 0.0
            total_perimeter = 0.0
            for geom in geometry.geoms:
                area, perimeter = self.geometry_area_perimeter(geom, radians=radians)
                total_area += area
                total_perimeter += perimeter
            return total_area, total_perimeter
        raise GeodError("Invalid geometry provided.")

    def __repr__(self) -> str:
        # search for ellipse name
        for ellps, vals in pj_ellps.items():
            if self.a == vals["a"]:
                # self.sphere is True when self.f is zero or very close to
                # zero (0), so prevent divide by zero.
                if self.b == vals.get("b") or (
                    not self.sphere and (1.0 / self.f) == vals.get("rf")
                ):
                    return f"{self.__class__.__name__}(ellps={ellps!r})"

        # no ellipse name found, call super class
        return super().__repr__()

    def __eq__(self, other: object) -> bool:
        """
        equality operator == for Geod objects

        Example usage:

        >>> from pyproj import Geod
        >>> # Use Clarke 1866 ellipsoid.
        >>> gclrk1 = Geod(ellps='clrk66')
        >>> # Define Clarke 1866 using parameters
        >>> gclrk2 = Geod(a=6378206.4, b=6356583.8)
        >>> gclrk1 == gclrk2
        True
        >>> # WGS 66 ellipsoid, PROJ style
        >>> gwgs66 = Geod('+ellps=WGS66')
        >>> # Naval Weapons Lab., 1965 ellipsoid
        >>> gnwl9d = Geod('+ellps=NWL9D')
        >>> # these ellipsoids are the same
        >>> gnwl9d == gwgs66
        True
        >>> gclrk1 != gnwl9d  # Clarke 1866 is unlike NWL9D
        True
        """
        if not isinstance(other, _Geod):
            return False

        return self.__repr__() == other.__repr__()


def reverse_azimuth(azi: Any, radians: bool = False) -> Any:
    """
    Reverses the given azimuth (forward <-> backwards)

    .. versionadded:: 3.5.0

    Accepted numeric scalar or array:

    - :class:`int`
    - :class:`float`
    - :class:`numpy.floating`
    - :class:`numpy.integer`
    - :class:`list`
    - :class:`tuple`
    - :class:`array.array`
    - :class:`numpy.ndarray`
    - :class:`xarray.DataArray`
    - :class:`pandas.Series`

    Parameters
    ----------
    azi: scalar or array
        The azimuth.
    radians: bool, default=False
        If True, the input data is assumed to be in radians.
        Otherwise, the data is assumed to be in degrees.

    Returns
    -------
    scalar or array:
        The reversed azimuth (forward <-> backwards)
    """
    inazi, azi_data_type = _copytobuffer(azi)
    _reverse_azimuth(inazi, radians=radians)
    return _convertback(azi_data_type, inazi)
