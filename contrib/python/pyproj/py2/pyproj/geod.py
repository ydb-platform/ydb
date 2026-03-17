"""
Cython wrapper to provide python interfaces to
PROJ (https://proj.org) functions.

Performs geodetic computations.

The Geod class can perform forward and inverse geodetic, or
Great Circle, computations.  The forward computation involves
determining latitude, longitude and back azimuth of a terminus
point given the latitude and longitude of an initial point, plus
azimuth and distance. The inverse computation involves
determining the forward and back azimuths and distance given the
latitudes and longitudes of an initial and terminus point.

Contact:  Jeffrey Whitaker <jeffrey.s.whitaker@noaa.gov

copyright (c) 2006 by Jeffrey Whitaker.

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
CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. """

__all__ = ["Geod", "pj_ellps", "geodesic_version_str"]

import math

from pyproj._geod import Geod as _Geod
from pyproj._geod import geodesic_version_str
from pyproj._list import get_ellps_map
from pyproj.utils import _convertback, _copytobuffer

pj_ellps = get_ellps_map()


class Geod(_Geod):
    """
    performs forward and inverse geodetic, or Great Circle,
    computations.  The forward computation (using the 'fwd' method)
    involves determining latitude, longitude and back azimuth of a
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

    def __init__(self, initstring=None, **kwargs):
        """
        initialize a Geod class instance.

        Geodetic parameters for specifying the ellipsoid
        can be given in a dictionary 'initparams', as keyword arguments,
        or as as proj geod initialization string.

        You can get a dictionary of ellipsoids using :func:`~pyproj.get_ellps_map`
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
        >>> "%7.3f %6.3f %12.3f" % (az12,az21,dist)
        '-66.531 75.654  4164192.708'
        >>> # compute latitude, longitude and back azimuth of Portland,
        >>> # given Boston lat/lon, forward azimuth and distance to Portland.
        >>> endlon, endlat, backaz = g.fwd(boston_lon, boston_lat, az12, dist)
        >>> "%6.3f  %6.3f %13.3f" % (endlat,endlon,backaz)
        '45.517  -123.683        75.654'
        >>> # compute the azimuths, distances from New York to several
        >>> # cities (pass a list)
        >>> lons1 = 3*[newyork_lon]; lats1 = 3*[newyork_lat]
        >>> lons2 = [boston_lon, portland_lon, london_lon]
        >>> lats2 = [boston_lat, portland_lat, london_lat]
        >>> az12,az21,dist = g.inv(lons1,lats1,lons2,lats2)
        >>> for faz, baz, d in list(zip(az12,az21,dist)):
        ...     "%7.3f %7.3f %9.3f" % (faz, baz, d)
        ' 54.663 -123.448 288303.720'
        '-65.463  79.342 4013037.318'
        ' 51.254 -71.576 5579916.651'
        >>> g2 = Geod('+ellps=clrk66') # use proj4 style initialization string
        >>> az12,az21,dist = g2.inv(boston_lon,boston_lat,portland_lon,portland_lat)
        >>> "%7.3f %6.3f %12.3f" % (az12,az21,dist)
        '-66.531 75.654  4164192.708'
        """
        # if initparams is a proj-type init string,
        # convert to dict.
        ellpsd = {}
        if initstring is not None:
            for kvpair in initstring.split():
                # Actually only +a and +b are needed
                # We can ignore safely any parameter that doesn't have a value
                if kvpair.find("=") == -1:
                    continue
                k, v = kvpair.split("=")
                k = k.lstrip("+")
                if k in ["a", "b", "rf", "f", "es", "e"]:
                    v = float(v)
                ellpsd[k] = v
        # merge this dict with kwargs dict.
        kwargs = dict(list(kwargs.items()) + list(ellpsd.items()))
        sphere = False
        if "ellps" in kwargs:
            # ellipse name given, look up in pj_ellps dict
            ellps_dict = pj_ellps[kwargs["ellps"]]
            a = ellps_dict["a"]
            if ellps_dict["description"] == "Normal Sphere":
                sphere = True
            if "b" in ellps_dict:
                b = ellps_dict["b"]
                es = 1.0 - (b * b) / (a * a)
                f = (a - b) / a
            elif "rf" in ellps_dict:
                f = 1.0 / ellps_dict["rf"]
                b = a * (1.0 - f)
                es = 1.0 - (b * b) / (a * a)
        else:
            # a (semi-major axis) and one of
            # b the semi-minor axis
            # rf the reciprocal flattening
            # f flattening
            # es eccentricity squared
            # must be given.
            a = kwargs["a"]
            if "b" in kwargs:
                b = kwargs["b"]
                es = 1.0 - (b * b) / (a * a)
                f = (a - b) / a
            elif "rf" in kwargs:
                f = 1.0 / kwargs["rf"]
                b = a * (1.0 - f)
                es = 1.0 - (b * b) / (a * a)
            elif "f" in kwargs:
                f = kwargs["f"]
                b = a * (1.0 - f)
                es = 1.0 - (b / a) ** 2
            elif "es" in kwargs:
                es = kwargs["es"]
                b = math.sqrt(a ** 2 - es * a ** 2)
                f = (a - b) / a
            elif "e" in kwargs:
                es = kwargs["e"] ** 2
                b = math.sqrt(a ** 2 - es * a ** 2)
                f = (a - b) / a
            else:
                b = a
                f = 0.0
                es = 0.0
                # msg='ellipse name or a, plus one of f,es,b must be given'
                # raise ValueError(msg)
        if math.fabs(f) < 1.0e-8:
            sphere = True

        super(Geod, self).__init__(a, f, sphere, b, es)

    def fwd(self, lons, lats, az, dist, radians=False):
        """
        forward transformation - Returns longitudes, latitudes and back
        azimuths of terminus points given longitudes (lons) and
        latitudes (lats) of initial points, plus forward azimuths (az)
        and distances (dist).
        latitudes (lats) of initial points, plus forward azimuths (az)
        and distances (dist).

        Works with numpy and regular python array objects, python
        sequences and scalars.

        if radians=True, lons/lats and azimuths are radians instead of
        degrees. Distances are in meters.
        """
        # process inputs, making copies that support buffer API.
        inx, xisfloat, xislist, xistuple = _copytobuffer(lons)
        iny, yisfloat, yislist, yistuple = _copytobuffer(lats)
        inz, zisfloat, zislist, zistuple = _copytobuffer(az)
        ind, disfloat, dislist, distuple = _copytobuffer(dist)
        super(Geod, self)._fwd(inx, iny, inz, ind, radians=radians)
        # if inputs were lists, tuples or floats, convert back.
        outx = _convertback(xisfloat, xislist, xistuple, inx)
        outy = _convertback(yisfloat, yislist, xistuple, iny)
        outz = _convertback(zisfloat, zislist, zistuple, inz)
        return outx, outy, outz

    def inv(self, lons1, lats1, lons2, lats2, radians=False):
        """
        inverse transformation - Returns forward and back azimuths, plus
        distances between initial points (specified by lons1, lats1) and
        terminus points (specified by lons2, lats2).

        Works with numpy and regular python array objects, python
        sequences and scalars.

        if radians=True, lons/lats and azimuths are radians instead of
        degrees. Distances are in meters.
        """
        # process inputs, making copies that support buffer API.
        inx, xisfloat, xislist, xistuple = _copytobuffer(lons1)
        iny, yisfloat, yislist, yistuple = _copytobuffer(lats1)
        inz, zisfloat, zislist, zistuple = _copytobuffer(lons2)
        ind, disfloat, dislist, distuple = _copytobuffer(lats2)
        super(Geod, self)._inv(inx, iny, inz, ind, radians=radians)
        # if inputs were lists, tuples or floats, convert back.
        outx = _convertback(xisfloat, xislist, xistuple, inx)
        outy = _convertback(yisfloat, yislist, xistuple, iny)
        outz = _convertback(zisfloat, zislist, zistuple, inz)
        return outx, outy, outz

    def npts(self, lon1, lat1, lon2, lat2, npts, radians=False):
        """
        Given a single initial point and terminus point (specified by
        python floats lon1,lat1 and lon2,lat2), returns a list of
        longitude/latitude pairs describing npts equally spaced
        intermediate points along the geodesic between the initial and
        terminus points.

        if radians=True, lons/lats are radians instead of degrees.

        Example usage:

        >>> from pyproj import Geod
        >>> g = Geod(ellps='clrk66') # Use Clarke 1866 ellipsoid.
        >>> # specify the lat/lons of Boston and Portland.
        >>> boston_lat = 42.+(15./60.); boston_lon = -71.-(7./60.)
        >>> portland_lat = 45.+(31./60.); portland_lon = -123.-(41./60.)
        >>> # find ten equally spaced points between Boston and Portland.
        >>> lonlats = g.npts(boston_lon,boston_lat,portland_lon,portland_lat,10)
        >>> for lon,lat in lonlats: '%6.3f  %7.3f' % (lat, lon)
        '43.528  -75.414'
        '44.637  -79.883'
        '45.565  -84.512'
        '46.299  -89.279'
        '46.830  -94.156'
        '47.149  -99.112'
        '47.251  -104.106'
        '47.136  -109.100'
        '46.805  -114.051'
        '46.262  -118.924'
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
        >>> for lon,lat in lonlats: '%6.3f  %7.3f' % (rad2dg*lat, rad2dg*lon)
        '43.528  -75.414'
        '44.637  -79.883'
        '45.565  -84.512'
        '46.299  -89.279'
        '46.830  -94.156'
        '47.149  -99.112'
        '47.251  -104.106'
        '47.136  -109.100'
        '46.805  -114.051'
        '46.262  -118.924'
        """
        lons, lats = super(Geod, self)._npts(
            lon1, lat1, lon2, lat2, npts, radians=radians
        )
        return list(zip(lons, lats))

    def __repr__(self):
        # search for ellipse name
        for (ellps, vals) in pj_ellps.items():
            if self.a == vals["a"]:
                b = vals.get("b", None)
                rf = vals.get("rf", None)
                # self.sphere is True when self.f is zero or very close to
                # zero (0), so prevent divide by zero.
                if self.b == b or (not self.sphere and (1.0 / self.f) == rf):
                    return "{classname}(ellps={ellps!r})" "".format(
                        classname=self.__class__.__name__, ellps=ellps
                    )

        # no ellipse name found, call super class
        return super(Geod, self).__repr__()

    def __eq__(self, other):
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
