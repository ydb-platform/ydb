include "base.pxi"

cimport cython
from libc.math cimport ceil, isnan, round

from pyproj._compat cimport cstrencode, empty_array

import math
from collections import namedtuple

from pyproj.enums import GeodIntermediateFlag
from pyproj.exceptions import GeodError

geodesic_version_str = (
    f"{GEODESIC_VERSION_MAJOR}.{GEODESIC_VERSION_MINOR}.{GEODESIC_VERSION_PATCH}"
)

GeodIntermediateReturn = namedtuple(
    "GeodIntermediateReturn", ["npts", "del_s", "dist", "lons", "lats", "azis"]
)

GeodIntermediateReturn.__doc__ = """
.. versionadded:: 3.1.0

Geod Intermediate Return value (Named Tuple)

Parameters
----------

npts: int
    number of points
del_s: float
    delimiter distance between two successive points
dist: float
    distance between the initial and terminus points
out_lons: Any
    array of the output lons
out_lats: Any
    array of the output lats
out_azis: Any
    array of the output azis
"""


cdef:
    int GEOD_INTER_FLAG_DEFAULT = GeodIntermediateFlag.DEFAULT

    int GEOD_INTER_FLAG_NPTS_MASK = (
        GeodIntermediateFlag.NPTS_ROUND
        | GeodIntermediateFlag.NPTS_CEIL
        | GeodIntermediateFlag.NPTS_TRUNC
    )
    int GEOD_INTER_FLAG_NPTS_ROUND = GeodIntermediateFlag.NPTS_ROUND
    int GEOD_INTER_FLAG_NPTS_CEIL = GeodIntermediateFlag.NPTS_CEIL
    int GEOD_INTER_FLAG_NPTS_TRUNC = GeodIntermediateFlag.NPTS_TRUNC

    int GEOD_INTER_FLAG_DEL_S_MASK = (
        GeodIntermediateFlag.DEL_S_RECALC | GeodIntermediateFlag.DEL_S_NO_RECALC
    )
    int GEOD_INTER_FLAG_DEL_S_RECALC = GeodIntermediateFlag.DEL_S_RECALC
    int GEOD_INTER_FLAG_DEL_S_NO_RECALC = GeodIntermediateFlag.DEL_S_NO_RECALC

    int GEOD_INTER_FLAG_AZIS_MASK = (
        GeodIntermediateFlag.AZIS_DISCARD | GeodIntermediateFlag.AZIS_KEEP
    )
    int GEOD_INTER_FLAG_AZIS_DISCARD = GeodIntermediateFlag.AZIS_DISCARD
    int GEOD_INTER_FLAG_AZIS_KEEP = GeodIntermediateFlag.AZIS_KEEP


cdef double _reverse_azimuth(double azi, double factor) nogil:
    if azi > 0:
        azi = azi - factor
    else:
        azi = azi + factor
    return azi

def reverse_azimuth(object azi, bint radians=False):
    cdef PyBuffWriteManager azibuff = PyBuffWriteManager(azi)
    cdef Py_ssize_t iii
    cdef double factor = 180
    if radians:
        factor = math.pi
    with nogil:
        for iii in range(azibuff.len):
            azibuff.data[iii] = _reverse_azimuth(azibuff.data[iii], factor=factor)


cdef class Geod:
    def __init__(self, double a, double f, bint sphere, double b, double es):
        geod_init(&self._geod_geodesic, <double> a, <double> f)
        self.a = a
        self.f = f
        self.initstring = f"+{a=} +{f=}"
        self.sphere = sphere
        self.b = b
        self.es = es

    def __reduce__(self):
        """special method that allows pyproj.Geod instance to be pickled"""
        return self.__class__, (self.initstring,)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _fwd(
        self,
        object lons,
        object lats,
        object az,
        object dist,
        bint radians=False,
        bint return_back_azimuth=True,
    ):
        """
        forward transformation - determine longitude, latitude and back azimuth
        of a terminus point given an initial point longitude and latitude, plus
        forward azimuth and distance.
        if radians=True, lons/lats are radians instead of degrees.
        if return_back_azimuth=True, the return azimuth will be the forward azimuth instead of the forward azimuth.
        """
        cdef:
            PyBuffWriteManager lonbuff = PyBuffWriteManager(lons)
            PyBuffWriteManager latbuff = PyBuffWriteManager(lats)
            PyBuffWriteManager azbuff = PyBuffWriteManager(az)
            PyBuffWriteManager distbuff = PyBuffWriteManager(dist)

        # process data in buffer
        if not lonbuff.len == latbuff.len == azbuff.len == distbuff.len:
            raise GeodError("Array lengths are not the same.")

        cdef:
            double lat1
            double lon1
            double az1
            double s12
            double plon2
            double plat2
            double pazi2
            Py_ssize_t iii

        with nogil:
            for iii in range(lonbuff.len):
                if not radians:
                    lon1 = lonbuff.data[iii]
                    lat1 = latbuff.data[iii]
                    az1 = azbuff.data[iii]
                    s12 = distbuff.data[iii]
                else:
                    lon1 = _RAD2DG * lonbuff.data[iii]
                    lat1 = _RAD2DG * latbuff.data[iii]
                    az1 = _RAD2DG * azbuff.data[iii]
                    s12 = distbuff.data[iii]
                geod_direct(
                    &self._geod_geodesic,
                    lat1,
                    lon1,
                    az1,
                    s12,
                    &plat2,
                    &plon2,
                    &pazi2,
                )
                # by default (return_back_azimuth=True),
                # forward azimuth needs to be flipped 180 degrees
                # to match the (back azimuth) output of PROJ geod utilities.
                if return_back_azimuth:
                    pazi2 = _reverse_azimuth(pazi2, factor=180)
                if not radians:
                    lonbuff.data[iii] = plon2
                    latbuff.data[iii] = plat2
                    azbuff.data[iii] = pazi2
                else:
                    lonbuff.data[iii] = _DG2RAD * plon2
                    latbuff.data[iii] = _DG2RAD * plat2
                    azbuff.data[iii] = _DG2RAD * pazi2

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _fwd_point(
        self,
        object lon1in,
        object lat1in,
        object az1in,
        object s12in,
        bint radians=False,
        bint return_back_azimuth=True,
    ):
        """
        Scalar optimized function
        forward transformation - determine longitude, latitude and back azimuth
        of a terminus point given an initial point longitude and latitude, plus
        forward azimuth and distance.
        if radians=True, lons/lats are radians instead of degrees.
        """
        cdef:
            double plon2
            double plat2
            double pazi2
            double lon1 = lon1in
            double lat1 = lat1in
            double az1 = az1in
            double s12 = s12in

        # We do the type-checking internally here due to automatically
        # casting length-1 arrays to float that we don't want to return scalar for.
        # Ex: float(np.array([0])) works and we don't want to accept numpy arrays
        for x_in in (lon1in, lat1in, az1in, s12in):
            if not isinstance(x_in, (float, int)):
                raise TypeError("Scalar input is required for point based functions")

        with nogil:
            if radians:
                lon1 = _RAD2DG * lon1
                lat1 = _RAD2DG * lat1
                az1 = _RAD2DG * az1
            geod_direct(
                &self._geod_geodesic,
                lat1,
                lon1,
                az1,
                s12,
                &plat2,
                &plon2,
                &pazi2,
            )
            # back azimuth needs to be flipped 180 degrees
            # to match what PROJ geod utility produces.
            if return_back_azimuth:
                pazi2 =_reverse_azimuth(pazi2, factor=180)
            if radians:
                plon2 = _DG2RAD * plon2
                plat2 = _DG2RAD * plat2
                pazi2 = _DG2RAD * pazi2
        return plon2, plat2, pazi2

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _inv(
        self,
        object lons1,
        object lats1,
        object lons2,
        object lats2,
        bint radians=False,
        bint return_back_azimuth=True,
    ):
        """
        inverse transformation - return forward azimuth (azi12) and back azimuths (azi21), plus distance
        between an initial and terminus lat/lon pair.
        if radians=True, lons/lats are radians instead of degree
        if return_back_azimuth=True, azi21 is a back azimuth (180 degrees flipped),
        otherwise azi21 is also a forward azimuth.
        """
        cdef:
            PyBuffWriteManager lon1buff = PyBuffWriteManager(lons1)
            PyBuffWriteManager lat1buff = PyBuffWriteManager(lats1)
            PyBuffWriteManager lon2buff = PyBuffWriteManager(lons2)
            PyBuffWriteManager lat2buff = PyBuffWriteManager(lats2)

        # process data in buffer
        if not lon1buff.len == lat1buff.len == lon2buff.len == lat2buff.len:
            raise GeodError("Array lengths are not the same.")

        cdef:
            double lat1
            double lon1
            double lat2
            double lon2
            double pazi1
            double pazi2
            double ps12
            Py_ssize_t iii

        with nogil:
            for iii in range(lon1buff.len):
                if radians:
                    lon1 = _RAD2DG * lon1buff.data[iii]
                    lat1 = _RAD2DG * lat1buff.data[iii]
                    lon2 = _RAD2DG * lon2buff.data[iii]
                    lat2 = _RAD2DG * lat2buff.data[iii]
                else:
                    lon1 = lon1buff.data[iii]
                    lat1 = lat1buff.data[iii]
                    lon2 = lon2buff.data[iii]
                    lat2 = lat2buff.data[iii]
                geod_inverse(
                    &self._geod_geodesic,
                    lat1, lon1, lat2, lon2,
                    &ps12, &pazi1, &pazi2,
                )

                # by default (return_back_azimuth=True),
                # forward azimuth needs to be flipped 180 degrees
                # to match the (back azimuth) output of PROJ geod utilities.
                if return_back_azimuth:
                    pazi2 = _reverse_azimuth(pazi2, factor=180)
                if radians:
                    lon1buff.data[iii] = _DG2RAD * pazi1
                    lat1buff.data[iii] = _DG2RAD * pazi2
                else:
                    lon1buff.data[iii] = pazi1
                    lat1buff.data[iii] = pazi2
                # write azimuth data into lon2 buffer
                lon2buff.data[iii] = ps12

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _inv_point(
        self,
        object lon1in,
        object lat1in,
        object lon2in,
        object lat2in,
        bint radians=False,
        bint return_back_azimuth=True,
    ):
        """
        Scalar optimized function
        inverse transformation - return forward and back azimuth, plus distance
        between an initial and terminus lat/lon pair.
        if radians=True, lons/lats are radians instead of degree
        """
        cdef:
            double pazi1
            double pazi2
            double ps12
            double lon1 = lon1in
            double lat1 = lat1in
            double lon2 = lon2in
            double lat2 = lat2in

        # We do the type-checking internally here due to automatically
        # casting length-1 arrays to float that we don't want to return scalar for.
        # Ex: float(np.array([0])) works and we don't want to accept numpy arrays
        for x_in in (lon1in, lat1in, lon2in, lat2in):
            if not isinstance(x_in, (float, int)):
                raise TypeError("Scalar input is required for point based functions")

        with nogil:
            if radians:
                lon1 = _RAD2DG * lon1
                lat1 = _RAD2DG * lat1
                lon2 = _RAD2DG * lon2
                lat2 = _RAD2DG * lat2
            geod_inverse(
                &self._geod_geodesic,
                lat1, lon1, lat2, lon2,
                &ps12, &pazi1, &pazi2,
            )
            # back azimuth needs to be flipped 180 degrees
            # to match what proj4 geod utility produces.
            if return_back_azimuth:
                pazi2 =_reverse_azimuth(pazi2, factor=180)
            if radians:
                pazi1 = _DG2RAD * pazi1
                pazi2 = _DG2RAD * pazi2
        return pazi1, pazi2, ps12

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _inv_or_fwd_intermediate(
        self,
        double lon1,
        double lat1,
        double lon2_or_azi1,
        double lat2,
        int npts,
        double del_s,
        bint radians,
        int initial_idx,
        int terminus_idx,
        int flags,
        object out_lons,
        object out_lats,
        object out_azis,
        bint return_back_azimuth,
        bint is_fwd,
    ) -> GeodIntermediateReturn:
        """
        .. versionadded:: 3.1.0

        given initial and terminus lat/lon, find npts intermediate points.
        using given lons, lats buffers
        """
        cdef:
            Py_ssize_t iii
            double pazi2
            double s12
            double plon2
            double plat2
            geod_geodesicline line
            bint store_az = (
                out_azis is not None or
                (flags & GEOD_INTER_FLAG_AZIS_MASK) == GEOD_INTER_FLAG_AZIS_KEEP
            )
            PyBuffWriteManager lons_buff
            PyBuffWriteManager lats_buff
            PyBuffWriteManager azis_buff

        if not is_fwd and (del_s == 0) == (npts == 0):
            raise GeodError("inv_intermediate: "
                            "npts and del_s are mutually exclusive, "
                            "only one of them must be != 0.")
        with nogil:
            if radians:
                lon1 *= _RAD2DG
                lat1 *= _RAD2DG
                lon2_or_azi1 *= _RAD2DG
                if not is_fwd:
                    lat2 *= _RAD2DG

            if is_fwd:
                # do fwd computation to set azimuths, distance.
                geod_lineinit(&line, &self._geod_geodesic, lat1, lon1, lon2_or_azi1, 0u)
                line.s13 = del_s * (npts + initial_idx + terminus_idx - 1)
            else:
                # do inverse computation to set azimuths, distance.
                geod_inverseline(&line, &self._geod_geodesic, lat1, lon1,
                                 lat2, lon2_or_azi1, 0u)

                if npts == 0:
                    # calc the number of required points by the distance increment
                    # s12 holds a temporary float value of npts (just reusing this var)
                    s12 = line.s13 / del_s - initial_idx - terminus_idx + 1
                    if (flags & GEOD_INTER_FLAG_NPTS_MASK) == \
                            GEOD_INTER_FLAG_NPTS_ROUND:
                        s12 = round(s12)
                    elif (flags & GEOD_INTER_FLAG_NPTS_MASK) == \
                            GEOD_INTER_FLAG_NPTS_CEIL:
                        s12 = ceil(s12)
                    npts = int(s12)
                if (flags & GEOD_INTER_FLAG_DEL_S_MASK) == GEOD_INTER_FLAG_DEL_S_RECALC:
                    # calc the distance increment by the number of required points
                    del_s = line.s13 / (npts + initial_idx + terminus_idx - 1)

            with gil:
                if out_lons is None:
                    out_lons = empty_array(npts)
                if out_lats is None:
                    out_lats = empty_array(npts)
                if out_azis is None and store_az:
                    out_azis = empty_array(npts)

                lons_buff = PyBuffWriteManager(out_lons)
                lats_buff = PyBuffWriteManager(out_lats)
                if store_az:
                    azis_buff = PyBuffWriteManager(out_azis)

                if lons_buff.len < npts \
                        or lats_buff.len < npts \
                        or (store_az and azis_buff.len < npts):
                    raise GeodError(
                        "Arrays are not long enough ("
                        f"{lons_buff.len}, {lats_buff.len}, "
                        f"{azis_buff.len if store_az else -1}) < {npts}.")

            # loop over intermediate points, compute lat/lons.
            for iii in range(0, npts):
                s12 = (iii + initial_idx) * del_s
                geod_position(&line, s12, &plat2, &plon2, &pazi2)
                if radians:
                    plat2 *= _DG2RAD
                    plon2 *= _DG2RAD
                lats_buff.data[iii] = plat2
                lons_buff.data[iii] = plon2
                if store_az:
                    # by default (return_back_azimuth=True),
                    # forward azimuth needs to be flipped 180 degrees
                    # to match the (back azimuth) output of PROJ geod utilities.
                    if return_back_azimuth:
                        pazi2 =_reverse_azimuth(pazi2, factor=180)
                    azis_buff.data[iii] = pazi2

        return GeodIntermediateReturn(
            npts, del_s, line.s13, out_lons, out_lats, out_azis)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _line_length(self, object lons, object lats, bint radians=False):
        """
        Calculate the distance between points along a line.


        Parameters
        ----------
        lons: array
            The longitude points along a line.
        lats: array
            The latitude points along a line.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.

        Returns
        -------
        float:
            The total distance.

        """
        cdef PyBuffWriteManager lonbuff = PyBuffWriteManager(lons)
        cdef PyBuffWriteManager latbuff = PyBuffWriteManager(lats)

        # process data in buffer
        if lonbuff.len != latbuff.len:
            raise GeodError("Array lengths are not the same.")

        if lonbuff.len == 1:
            lonbuff.data[0] = 0
            return 0.0

        cdef:
            double lat1
            double lon1
            double lat2
            double lon2
            double pazi1
            double pazi2
            double ps12
            double total_distance = 0.0
            Py_ssize_t iii

        with nogil:
            for iii in range(lonbuff.len - 1):
                if radians:
                    lon1 = _RAD2DG * lonbuff.data[iii]
                    lat1 = _RAD2DG * latbuff.data[iii]
                    lon2 = _RAD2DG * lonbuff.data[iii + 1]
                    lat2 = _RAD2DG * latbuff.data[iii + 1]
                else:
                    lon1 = lonbuff.data[iii]
                    lat1 = latbuff.data[iii]
                    lon2 = lonbuff.data[iii + 1]
                    lat2 = latbuff.data[iii + 1]
                geod_inverse(
                    &self._geod_geodesic,
                    lat1, lon1, lat2, lon2,
                    &ps12, &pazi1, &pazi2,
                )
                lonbuff.data[iii] = ps12
                total_distance += ps12
        return total_distance

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def _polygon_area_perimeter(self, object lons, object lats, bint radians=False):
        """
        A simple interface for computing the area of a geodesic polygon.

        lats should be in the range [-90 deg, 90 deg].

        Only simple polygons (which are not self-intersecting) are allowed.
        There's no need to "close" the polygon by repeating the first vertex.
        The area returned is signed with counter-clockwise traversal being treated as
        positive.

        Parameters
        ----------
        lons: array
            An array of longitude values.
        lats: array
            An array of latitude values.
        radians: bool, default=False
            If True, the input data is assumed to be in radians.

        Returns
        -------
        (float, float):
            The area (meter^2) and perimeter (meters) of the polygon.

        """
        cdef PyBuffWriteManager lonbuff = PyBuffWriteManager(lons)
        cdef PyBuffWriteManager latbuff = PyBuffWriteManager(lats)

        # process data in buffer
        if not lonbuff.len == latbuff.len:
            raise GeodError("Array lengths are not the same.")

        cdef double polygon_area
        cdef double polygon_perimeter
        cdef Py_ssize_t iii

        with nogil:
            if radians:
                for iii in range(lonbuff.len):
                    lonbuff.data[iii] *= _RAD2DG
                    latbuff.data[iii] *= _RAD2DG

            geod_polygonarea(
                &self._geod_geodesic,
                latbuff.data, lonbuff.data, lonbuff.len,
                &polygon_area, &polygon_perimeter
            )
        return (polygon_area, polygon_perimeter)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.initstring!r})"
