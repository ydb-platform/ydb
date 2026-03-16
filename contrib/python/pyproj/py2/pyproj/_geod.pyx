include "base.pxi"

from pyproj.compat import cstrencode, pystrdecode
from pyproj.exceptions import GeodError

geodesic_version_str = "{0}.{1}.{2}".format(
    GEODESIC_VERSION_MAJOR,
    GEODESIC_VERSION_MINOR,
    GEODESIC_VERSION_PATCH
)

cdef class Geod:
    def __init__(self, a, f, sphere, b, es):
        geod_init(&self._geod_geodesic, <double> a, <double> f)
        self.a = a
        self.f = f
        if isinstance(a, float) and a.is_integer():
            # convert 'a' only for initstring
            a = int(a)
        if f == 0.0:
            f = 0
        self.initstring = pystrdecode(cstrencode("+a=%s +f=%s" % (a, f)))
        self.sphere = sphere
        self.b = b
        self.es = es

    def __reduce__(self):
        """special method that allows pyproj.Geod instance to be pickled"""
        return self.__class__,(self.initstring,)

    def _fwd(self, object lons, object lats, object az, object dist, radians=False):
        """
 forward transformation - determine longitude, latitude and back azimuth
 of a terminus point given an initial point longitude and latitude, plus
 forward azimuth and distance.
 if radians=True, lons/lats are radians instead of degrees.
        """
        cdef Py_ssize_t buflenlons, buflenlats, buflenaz, buflend, ndim, i
        cdef double lat1,lon1,az1,s12,plon2,plat2,pazi2
        cdef double *lonsdata
        cdef double *latsdata
        cdef double *azdata
        cdef double *distdata
        cdef void *londata
        cdef void *latdata
        cdef void *azdat
        cdef void *distdat
        # if buffer api is supported, get pointer to data buffers.
        if PyObject_AsWriteBuffer(lons, &londata, &buflenlons) <> 0:
            raise GeodError
        if PyObject_AsWriteBuffer(lats, &latdata, &buflenlats) <> 0:
            raise GeodError
        if PyObject_AsWriteBuffer(az, &azdat, &buflenaz) <> 0:
            raise GeodError
        if PyObject_AsWriteBuffer(dist, &distdat, &buflend) <> 0:
            raise GeodError
        # process data in buffer
        if not buflenlons == buflenlats == buflenaz == buflend:
            raise GeodError("Buffer lengths not the same")
        ndim = buflenlons//_DOUBLESIZE
        lonsdata = <double *>londata
        latsdata = <double *>latdata
        azdata = <double *>azdat
        distdata = <double *>distdat
        for i from 0 <= i < ndim:
            if not radians:
                lon1 = lonsdata[i]
                lat1 = latsdata[i]
                az1 = azdata[i]
                s12 = distdata[i]
            else:
                lon1 = _RAD2DG*lonsdata[i]
                lat1 = _RAD2DG*latsdata[i]
                az1 = _RAD2DG*azdata[i]
                s12 = distdata[i]
            geod_direct(&self._geod_geodesic, lat1, lon1, az1, s12,\
                   &plat2, &plon2, &pazi2)
            # back azimuth needs to be flipped 180 degrees
            # to match what proj4 geod utility produces.
            if pazi2 > 0:
                pazi2 = pazi2-180.
            elif pazi2 <= 0:
                pazi2 = pazi2+180.
            if not radians:
                lonsdata[i] = plon2
                latsdata[i] = plat2
                azdata[i] = pazi2
            else:
                lonsdata[i] = _DG2RAD*plon2
                latsdata[i] = _DG2RAD*plat2
                azdata[i] = _DG2RAD*pazi2

    def _inv(self, object lons1, object lats1, object lons2, object lats2, radians=False):
        """
 inverse transformation - return forward and back azimuths, plus distance
 between an initial and terminus lat/lon pair.
 if radians=True, lons/lats are radians instead of degrees.
        """
        cdef double lat1,lon1,lat2,lon2,pazi1,pazi2,ps12
        cdef Py_ssize_t buflenlons, buflenlats, buflenaz, buflend, ndim, i
        cdef double *lonsdata
        cdef double *latsdata
        cdef double *azdata
        cdef double *distdata
        cdef void *londata
        cdef void *latdata
        cdef void *azdat
        cdef void *distdat
        # if buffer api is supported, get pointer to data buffers.
        if PyObject_AsWriteBuffer(lons1, &londata, &buflenlons) <> 0:
            raise GeodError
        if PyObject_AsWriteBuffer(lats1, &latdata, &buflenlats) <> 0:
            raise GeodError
        if PyObject_AsWriteBuffer(lons2, &azdat, &buflenaz) <> 0:
            raise GeodError
        if PyObject_AsWriteBuffer(lats2, &distdat, &buflend) <> 0:
            raise GeodError
        # process data in buffer
        if not buflenlons == buflenlats == buflenaz == buflend:
            raise GeodError("Buffer lengths not the same")
        ndim = buflenlons//_DOUBLESIZE
        lonsdata = <double *>londata
        latsdata = <double *>latdata
        azdata = <double *>azdat
        distdata = <double *>distdat
        for i from 0 <= i < ndim:
            if radians:
                lon1 = _RAD2DG*lonsdata[i]
                lat1 = _RAD2DG*latsdata[i]
                lon2 = _RAD2DG*azdata[i]
                lat2 = _RAD2DG*distdata[i]
            else:
                lon1 = lonsdata[i]
                lat1 = latsdata[i]
                lon2 = azdata[i]
                lat2 = distdata[i]
            geod_inverse(&self._geod_geodesic, lat1, lon1, lat2, lon2,
                    &ps12, &pazi1, &pazi2)
            # back azimuth needs to be flipped 180 degrees
            # to match what proj4 geod utility produces.
            if pazi2 > 0:
                pazi2 = pazi2-180.
            elif pazi2 <= 0:
                pazi2 = pazi2+180.
            if radians:
                lonsdata[i] = _DG2RAD*pazi1
                latsdata[i] = _DG2RAD*pazi2
            else:
                lonsdata[i] = pazi1
                latsdata[i] = pazi2
            azdata[i] = ps12

    def _npts(self, double lon1, double lat1, double lon2, double lat2, int npts, radians=False):
        """
 given initial and terminus lat/lon, find npts intermediate points."""
        cdef int i
        cdef double del_s,ps12,pazi1,pazi2,s12,plon2,plat2
        cdef geod_geodesicline line
        if radians:
            lon1 = _RAD2DG*lon1
            lat1 = _RAD2DG*lat1
            lon2 = _RAD2DG*lon2
            lat2 = _RAD2DG*lat2
        # do inverse computation to set azimuths, distance.
        # in proj 4.9.3 and later the next two steps can be replace by a call
        # to geod_inverseline with del_s = line.s13/(npts+1)
        geod_inverse(&self._geod_geodesic, lat1, lon1,  lat2, lon2,
                &ps12, &pazi1, &pazi2)
        geod_lineinit(&line, &self._geod_geodesic, lat1, lon1, pazi1, 0u)
        # distance increment.
        del_s = ps12/(npts+1)
        # initialize output tuples.
        lats = ()
        lons = ()
        # loop over intermediate points, compute lat/lons.
        for i from 1 <= i < npts+1:
            s12 = i*del_s
            geod_position(&line, s12, &plat2, &plon2, &pazi2);
            if radians:
                lats = lats + (_DG2RAD*plat2,)
                lons = lons + (_DG2RAD*plon2,)
            else:
                lats = lats + (plat2,)
                lons = lons + (plon2,)
        return lons, lats

    def __repr__(self):
        return "{classname}({init!r})".format(classname=self.__class__.__name__,
                                              init=self.initstring)
