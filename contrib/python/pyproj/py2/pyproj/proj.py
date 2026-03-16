# -*- coding: utf-8 -*-
"""
Cython wrapper to provide python interfaces to
PROJ (https://proj.org) functions.

Performs cartographic transformations and geodetic computations.

The Proj class can convert from geographic (longitude,latitude)
to native map projection (x,y) coordinates and vice versa, or
from one map projection coordinate system directly to another.
The module variable pj_list is a dictionary containing all the
available projections and their descriptions.

Input coordinates can be given as python arrays, lists/tuples,
scalars or numpy/Numeric/numarray arrays. Optimized for objects
that support the Python buffer protocol (regular python and
numpy array objects).

Download: http://python.org/pypi/pyproj

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
import re

from pyproj import _proj
from pyproj._list import get_proj_operations_map
from pyproj.compat import cstrencode, pystrdecode
from pyproj.crs import CRS
from pyproj.utils import _convertback, _copytobuffer

# import numpy as np
proj_version_str = _proj.proj_version_str

pj_list = get_proj_operations_map()


class Proj(_proj.Proj):
    """
    Performs cartographic transformations (converts from
    longitude,latitude to native map projection x,y coordinates and
    vice versa) using proj (https://proj.org).

    A Proj class instance is initialized with proj map projection
    control parameter key/value pairs. The key/value pairs can
    either be passed in a dictionary, or as keyword arguments,
    or as a PROJ string (compatible with the proj command). See
    https://proj.org/operations/projections/index.html for examples of
    key/value pairs defining different map projections.

    Calling a Proj class instance with the arguments lon, lat will
    convert lon/lat (in degrees) to x/y native map projection
    coordinates (in meters).  If optional keyword 'inverse' is True
    (default is False), the inverse transformation from x/y to
    lon/lat is performed. If optional keyword 'errcheck' is True (default is
    False) an exception is raised if the transformation is invalid.
    If errcheck=False and the transformation is invalid, no
    exception is raised and 1.e30 is returned. If the optional keyword
    'preserve_units' is True, the units in map projection coordinates
    are not forced to be meters.

    Works with numpy and regular python array objects, python
    sequences and scalars.

    Attributes
    ----------
    srs: str
        The string form of the user input used to create the Proj.
    crs: ~pyproj.crs.CRS
        The CRS object associated with the Proj.
    proj_version: int
        The major version number for PROJ.

    """

    def __init__(self, projparams=None, preserve_units=True, **kwargs):
        """
        initialize a Proj class instance.

        See the PROJ documentation (https://proj.org)
        for more information about projection parameters.

        Parameters
        ----------
        projparams: int, str, dict, pyproj.CRS
            A PROJ or WKT string, PROJ dict, EPSG integer, or a pyproj.CRS instnace.
        preserve_units: bool
            If false, will ensure +units=m.
        **kwargs:
            PROJ projection parameters.


        Example usage:

        >>> from pyproj import Proj
        >>> p = Proj(proj='utm',zone=10,ellps='WGS84', preserve_units=False)
        >>> x,y = p(-120.108, 34.36116666)
        >>> 'x=%9.3f y=%11.3f' % (x,y)
        'x=765975.641 y=3805993.134'
        >>> 'lon=%8.3f lat=%5.3f' % p(x,y,inverse=True)
        'lon=-120.108 lat=34.361'
        >>> # do 3 cities at a time in a tuple (Fresno, LA, SF)
        >>> lons = (-119.72,-118.40,-122.38)
        >>> lats = (36.77, 33.93, 37.62 )
        >>> x,y = p(lons, lats)
        >>> 'x: %9.3f %9.3f %9.3f' % x
        'x: 792763.863 925321.537 554714.301'
        >>> 'y: %9.3f %9.3f %9.3f' % y
        'y: 4074377.617 3763936.941 4163835.303'
        >>> lons, lats = p(x, y, inverse=True) # inverse transform
        >>> 'lons: %8.3f %8.3f %8.3f' % lons
        'lons: -119.720 -118.400 -122.380'
        >>> 'lats: %8.3f %8.3f %8.3f' % lats
        'lats:   36.770   33.930   37.620'
        >>> p2 = Proj('+proj=utm +zone=10 +ellps=WGS84', preserve_units=False)
        >>> x,y = p2(-120.108, 34.36116666)
        >>> 'x=%9.3f y=%11.3f' % (x,y)
        'x=765975.641 y=3805993.134'
        >>> p = Proj("epsg:32667", preserve_units=False)
        >>> 'x=%12.3f y=%12.3f (meters)' % p(-114.057222, 51.045)
        'x=-1783506.250 y= 6193827.033 (meters)'
        >>> p = Proj("epsg:32667")
        >>> 'x=%12.3f y=%12.3f (feet)' % p(-114.057222, 51.045)
        'x=-5851386.754 y=20320914.191 (feet)'
        >>> # test data with radian inputs
        >>> p1 = Proj("epsg:4214")
        >>> x1, y1 = p1(116.366, 39.867)
        >>> '{:.3f} {:.3f}'.format(x1, y1)
        '2.031 0.696'
        >>> x2, y2 = p1(x1, y1, inverse=True)
        >>> '{:.3f} {:.3f}'.format(x2, y2)
        '116.366 39.867'
        """
        self.crs = CRS.from_user_input(projparams if projparams is not None else kwargs)
        # make sure units are meters if preserve_units is False.
        if not preserve_units and "foot" in self.crs.axis_info[0].unit_name:
            projstring = self.crs.to_proj4(4)
            projstring = re.sub(r"\s\+units=[\w-]+", "", projstring)
            projstring += " +units=m"
            self.crs = CRS(projstring)

        projstring = self.crs.to_proj4() or self.crs.srs
        projstring = re.sub(r"\s\+?type=crs", "", projstring)
        super(Proj, self).__init__(cstrencode(projstring.strip()))

    def __call__(self, *args, **kw):
        # ,lon,lat,inverse=False,errcheck=False):
        """
        Calling a Proj class instance with the arguments lon, lat will
        convert lon/lat (in degrees) to x/y native map projection
        coordinates (in meters).  If optional keyword 'inverse' is True
        (default is False), the inverse transformation from x/y to
        lon/lat is performed. If optional keyword 'errcheck' is True (default is
        False) an exception is raised if the transformation is invalid.
        If errcheck=False and the transformation is invalid, no
        exception is raised and 1.e30 is returned.

        Inputs should be doubles (they will be cast to doubles if they
        are not, causing a slight performance hit).

        Works with numpy and regular python array objects, python
        sequences and scalars, but is fastest for array objects.
        """
        inverse = kw.get("inverse", False)
        errcheck = kw.get("errcheck", False)
        lon, lat = args
        # process inputs, making copies that support buffer API.
        inx, xisfloat, xislist, xistuple = _copytobuffer(lon)
        iny, yisfloat, yislist, yistuple = _copytobuffer(lat)
        # call PROJ functions. inx and iny modified in place.
        if inverse:
            self._inv(inx, iny, errcheck=errcheck)
        else:
            self._fwd(inx, iny, errcheck=errcheck)
        # if inputs were lists, tuples or floats, convert back.
        outx = _convertback(xisfloat, xislist, xistuple, inx)
        outy = _convertback(yisfloat, yislist, xistuple, iny)
        return outx, outy

    def definition_string(self):
        """Returns formal definition string for projection

        >>> Proj("epsg:4326").definition_string()
        'proj=longlat datum=WGS84 no_defs ellps=WGS84 towgs84=0,0,0'
        >>>
        """
        return pystrdecode(self.definition)

    def to_latlong_def(self):
        """return the definition string of the geographic (lat/lon)
        coordinate version of the current projection"""
        return self.crs.geodetic_crs.to_proj4(4)

    def to_latlong(self):
        """return a new Proj instance which is the geographic (lat/lon)
        coordinate version of the current projection"""
        return Proj(self.crs.geodetic_crs)
