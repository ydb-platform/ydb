include "base.pxi"

import warnings

from pyproj.compat import cstrencode, pystrdecode
from pyproj._datadir cimport get_pyproj_context
from pyproj.exceptions import ProjError


# # version number string for PROJ
proj_version_str = "{0}.{1}.{2}".format(
    PROJ_VERSION_MAJOR,
    PROJ_VERSION_MINOR,
    PROJ_VERSION_PATCH
)

cdef class Proj:
    def __cinit__(self):
        self.projctx = NULL
        self.projpj = NULL

    def __init__(self, const char *projstring):
        self.srs = pystrdecode(projstring)
        # setup the context
        self.projctx = get_pyproj_context()
        # initialize projection
        self.projpj = proj_create(self.projctx, projstring)
        if self.projpj is NULL:
            raise ProjError("Invalid projection {}.".format(projstring))
        self.projpj_info = proj_pj_info(self.projpj)
        self._proj_version = PROJ_VERSION_MAJOR

    def __dealloc__(self):
        """destroy projection definition"""
        if self.projpj is not NULL:
            proj_destroy(self.projpj)
        if self.projctx is not NULL:
            proj_context_destroy(self.projctx)

    @property
    def proj_version(self):
        warnings.warn(
            "'Proj.proj_version' is deprecated. "
            "Please use `pyproj.proj_version_str` instead.",
            DeprecationWarning,
        )
        return self._proj_version

    @property
    def definition(self):
        return self.projpj_info.definition

    @property
    def has_inverse(self):
        """Returns true if this projection has an inverse"""
        return self.projpj_info.has_inverse == 1

    def __reduce__(self):
        """special method that allows pyproj.Proj instance to be pickled"""
        return self.__class__,(self.crs.srs,)

    def _fwd(self, object lons, object lats, errcheck=False):
        """
        forward transformation - lons,lats to x,y (done in place).
        if errcheck=True, an exception is raised if the forward transformation is invalid.
        if errcheck=False and the forward transformation is invalid, no exception is
        raised and 1.e30 is returned.
        """
        cdef PJ_COORD projxyout
        cdef PJ_COORD projlonlatin
        cdef Py_ssize_t buflenx, bufleny, ndim, i
        cdef double *lonsdata
        cdef double *latsdata
        cdef void *londata
        cdef void *latdata
        cdef int err
        # if buffer api is supported, get pointer to data buffers.
        if PyObject_AsWriteBuffer(lons, &londata, &buflenx) <> 0:
            raise ProjError
        if PyObject_AsWriteBuffer(lats, &latdata, &bufleny) <> 0:
            raise ProjError
        # process data in buffer
        if buflenx != bufleny:
            raise ProjError("Buffer lengths not the same")
        ndim = buflenx//_DOUBLESIZE
        lonsdata = <double *>londata
        latsdata = <double *>latdata
        for i from 0 <= i < ndim:
            # if inputs are nan's, return big number.
            if lonsdata[i] != lonsdata[i] or latsdata[i] != latsdata[i]:
                lonsdata[i]=1.e30; latsdata[i]=1.e30
                if errcheck:
                    raise ProjError('projection undefined')
                continue
            if proj_angular_input(self.projpj, PJ_FWD):
                projlonlatin.uv.u = _DG2RAD*lonsdata[i]
                projlonlatin.uv.v = _DG2RAD*latsdata[i]
            else:
                projlonlatin.uv.u = lonsdata[i]
                projlonlatin.uv.v = latsdata[i]
            projxyout = proj_trans(self.projpj, PJ_FWD, projlonlatin)
            if errcheck:
                err = proj_errno(self.projpj)
                if err != 0:
                     raise ProjError(pystrdecode(proj_errno_string(err)))
            # since HUGE_VAL can be 'inf',
            # change it to a real (but very large) number.
            # also check for NaNs.
            if projxyout.xy.x == HUGE_VAL or\
                    projxyout.xy.x != projxyout.xy.x or\
                    projxyout.xy.y == HUGE_VAL or\
                    projxyout.xy.x != projxyout.xy.x:
                if errcheck:
                    raise ProjError('projection undefined')
                lonsdata[i] = 1.e30
                latsdata[i] = 1.e30
            else:
                lonsdata[i] = projxyout.xy.x
                latsdata[i] = projxyout.xy.y

    def _inv(self, object x, object y, errcheck=False):
        """
        inverse transformation - x,y to lons,lats (done in place).
        if errcheck=True, an exception is raised if the inverse transformation is invalid.
        if errcheck=False and the inverse transformation is invalid, no exception is
        raised and 1.e30 is returned.
        """
        if not self.has_inverse:
            raise ProjError('inverse projection undefined')

        cdef PJ_COORD projxyin
        cdef PJ_COORD projlonlatout
        cdef Py_ssize_t buflenx, bufleny, ndim, i
        cdef void *xdata
        cdef void *ydata
        cdef double *xdatab
        cdef double *ydatab
        # if buffer api is supported, get pointer to data buffers.
        if PyObject_AsWriteBuffer(x, &xdata, &buflenx) <> 0:
            raise ProjError
        if PyObject_AsWriteBuffer(y, &ydata, &bufleny) <> 0:
            raise ProjError
        # process data in buffer
        # (for numpy/regular python arrays).
        if buflenx != bufleny:
            raise ProjError("Buffer lengths not the same")
        ndim = buflenx//_DOUBLESIZE
        xdatab = <double *>xdata
        ydatab = <double *>ydata
        for i from 0 <= i < ndim:
            # if inputs are nan's, return big number.
            if xdatab[i] != xdatab[i] or ydatab[i] != ydatab[i]:
                xdatab[i]=1.e30; ydatab[i]=1.e30
                if errcheck:
                    raise ProjError('projection undefined')
                continue
            projxyin.uv.u = xdatab[i]
            projxyin.uv.v = ydatab[i]
            projlonlatout = proj_trans(self.projpj, PJ_INV, projxyin)
            if errcheck:
                err = proj_errno(self.projpj)
                if err != 0:
                     raise ProjError(pystrdecode(proj_errno_string(err)))
            # since HUGE_VAL can be 'inf',
            # change it to a real (but very large) number.
            # also check for NaNs.
            if projlonlatout.uv.u == HUGE_VAL or \
                    projlonlatout.uv.u != projlonlatout.uv.u or \
                    projlonlatout.uv.v == HUGE_VAL or \
                    projlonlatout.uv.v != projlonlatout.uv.v:
                if errcheck:
                    raise ProjError('projection undefined')
                xdatab[i] = 1.e30
                ydatab[i] = 1.e30
            elif proj_angular_output(self.projpj, PJ_INV):
                xdatab[i] = _RAD2DG*projlonlatout.uv.u
                ydatab[i] = _RAD2DG*projlonlatout.uv.v
            else:
                xdatab[i] = projlonlatout.uv.u
                ydatab[i] = projlonlatout.uv.v

    def __repr__(self):
        return "Proj('{srs}', preserve_units=True)".format(srs=self.srs)

    def _is_exact_same(self, Proj other):
        return proj_is_equivalent_to(
            self.projpj, other.projpj, PJ_COMP_STRICT) == 1

    def _is_equivalent(self, Proj other):
        return proj_is_equivalent_to(
            self.projpj, other.projpj, PJ_COMP_EQUIVALENT) == 1

    def __eq__(self, other):
        if not isinstance(other, Proj):
            return False
        return self._is_equivalent(other)

    def is_exact_same(self, other):
        """Compares Proj objects to see if they are exactly the same."""
        if not isinstance(other, Proj):
            return False
        return self._is_exact_same(other)
