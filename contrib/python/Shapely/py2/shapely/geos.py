"""
Proxies for libgeos, GEOS-specific exceptions, and utilities
"""

import atexit
from ctypes import (
    CDLL, cdll, pointer, string_at, DEFAULT_MODE, c_void_p, c_size_t, c_char_p)
from ctypes.util import find_library
import glob
import logging
import os
import re
import sys
import threading
from functools import partial

from contrib.libs.geos.capi.ctypes import Geos

from .ctypes_declarations import prototype, EXCEPTION_HANDLER_FUNCTYPE
from .errors import WKBReadingError, WKTReadingError, TopologicalError, PredicateError

_lgeos = Geos()

# Add message handler to this module's logger
LOG = logging.getLogger(__name__)

if sys.version_info[0] >= 3:
    text_types = str
else:
    text_types = (str, unicode)


def _geos_version():
    GEOSversion = _lgeos.GEOSversion
    GEOSversion.restype = c_char_p
    GEOSversion.argtypes = []
    geos_version_string = GEOSversion()
    if sys.version_info[0] >= 3:
        geos_version_string = geos_version_string.decode('ascii')
    res = re.findall(r'(\d+)\.(\d+)\.(\d+)', geos_version_string)
    assert len(res) == 2, res
    geos_version = tuple(int(x) for x in res[0])
    capi_version = tuple(int(x) for x in res[1])
    return geos_version_string, geos_version, capi_version

geos_version_string, geos_version, geos_capi_version = _geos_version()


# If we have the new interface, then record a baseline so that we know what
# additional functions are declared in ctypes_declarations.
if geos_version >= (3, 1, 0):
    start_set = set(_lgeos.__dict__)

# Apply prototypes for the libgeos_c functions
prototype(_lgeos, geos_version)

# If we have the new interface, automatically detect all function
# declarations, and declare their re-entrant counterpart.
if geos_version >= (3, 1, 0):
    end_set = set(_lgeos.__dict__)
    new_func_names = end_set - start_set

    for func_name in new_func_names:
        new_func_name = "%s_r" % func_name
        has_attr = True
        try:
            getattr(_lgeos, new_func_name)
        except:
            has_attr = False
        if has_attr:
            new_func = getattr(_lgeos, new_func_name)
            old_func = getattr(_lgeos, func_name)
            new_func.restype = old_func.restype
            if old_func.argtypes is None:
                # Handle functions that didn't take an argument before,
                # finishGEOS.
                new_func.argtypes = [c_void_p]
            else:
                new_func.argtypes = [c_void_p] + list(old_func.argtypes)
            if old_func.errcheck is not None:
                new_func.errcheck = old_func.errcheck

    # Handle special case.
    _lgeos.initGEOS_r.restype = c_void_p
    _lgeos.initGEOS_r.argtypes = \
        [EXCEPTION_HANDLER_FUNCTYPE, EXCEPTION_HANDLER_FUNCTYPE]
    _lgeos.finishGEOS_r.argtypes = [c_void_p]


def make_logging_callback(func):
    """Error or notice handler callback producr

    Wraps a logger method, func, as a GEOS callback.
    """
    def callback(fmt, *fmt_args):
        fmt = fmt.decode('ascii')
        conversions = re.findall(r'%.', fmt)
        args = [
            string_at(arg).decode('ascii')
            for spec, arg in zip(conversions, fmt_args)
            if spec == '%s' and arg is not None]

        func(fmt, *args)

    return callback

error_handler = make_logging_callback(LOG.error)
notice_handler = make_logging_callback(LOG.info)

error_h = EXCEPTION_HANDLER_FUNCTYPE(error_handler)
notice_h = EXCEPTION_HANDLER_FUNCTYPE(notice_handler)


class WKTReader(object):

    _lgeos = None
    _reader = None

    def __init__(self, lgeos):
        """Create WKT Reader"""
        self._lgeos = lgeos
        self._reader = self._lgeos.GEOSWKTReader_create()

    def __del__(self):
        """Destroy WKT Reader"""
        if self._lgeos is not None:
            self._lgeos.GEOSWKTReader_destroy(self._reader)
            self._reader = None
            self._lgeos = None

    def read(self, text):
        """Returns geometry from WKT"""
        if not isinstance(text, text_types):
            raise TypeError("Only str is accepted.")
        if sys.version_info[0] >= 3:
            text = text.encode()
        c_string = c_char_p(text)
        geom = self._lgeos.GEOSWKTReader_read(self._reader, c_string)
        if not geom:
            raise WKTReadingError(
                "Could not create geometry because of errors "
                "while reading input.")
        # avoid circular import dependency
        from shapely.geometry.base import geom_factory
        return geom_factory(geom)


class WKTWriter(object):

    _lgeos = None
    _writer = None

    # Establish default output settings
    defaults = {}

    if geos_version >= (3, 3, 0):

        defaults['trim'] = True
        defaults['output_dimension'] = 3

        # GEOS' defaults for methods without "get"
        _trim = False
        _rounding_precision = -1
        _old_3d = False

        @property
        def trim(self):
            """Trimming of unnecessary decimals (default: True)"""
            return getattr(self, '_trim')

        @trim.setter
        def trim(self, value):
            self._trim = bool(value)
            self._lgeos.GEOSWKTWriter_setTrim(self._writer, self._trim)

        @property
        def rounding_precision(self):
            """Rounding precision when writing the WKT.
            A precision of -1 (default) disables it."""
            return getattr(self, '_rounding_precision')

        @rounding_precision.setter
        def rounding_precision(self, value):
            self._rounding_precision = int(value)
            self._lgeos.GEOSWKTWriter_setRoundingPrecision(
                self._writer, self._rounding_precision)

        @property
        def output_dimension(self):
            """Output dimension, either 2 or 3 (default)"""
            return self._lgeos.GEOSWKTWriter_getOutputDimension(
                self._writer)

        @output_dimension.setter
        def output_dimension(self, value):
            self._lgeos.GEOSWKTWriter_setOutputDimension(
                self._writer, int(value))

        @property
        def old_3d(self):
            """Show older style for 3D WKT, without 'Z' (default: False)"""
            return getattr(self, '_old_3d')

        @old_3d.setter
        def old_3d(self, value):
            self._old_3d = bool(value)
            self._lgeos.GEOSWKTWriter_setOld3D(self._writer, self._old_3d)

    def __init__(self, lgeos, **settings):
        """Create WKT Writer

        Note: writer defaults are set differently for GEOS 3.3.0 and up.
        For example, with 'POINT Z (1 2 3)':

            newer: POINT Z (1 2 3)
            older: POINT (1.0000000000000000 2.0000000000000000)

        The older formatting can be achieved for GEOS 3.3.0 and up by setting
        the properties:
            trim = False
            output_dimension = 2
        """
        self._lgeos = lgeos
        self._writer = self._lgeos.GEOSWKTWriter_create()

        applied_settings = self.defaults.copy()
        applied_settings.update(settings)
        for name in applied_settings:
            setattr(self, name, applied_settings[name])

    def __setattr__(self, name, value):
        """Limit setting attributes"""
        if hasattr(self, name):
            object.__setattr__(self, name, value)
        else:
            raise AttributeError('%r object has no attribute %r' %
                                 (self.__class__.__name__, name))

    def __del__(self):
        """Destroy WKT Writer"""
        if self._lgeos is not None:
            self._lgeos.GEOSWKTWriter_destroy(self._writer)
            self._writer = None
            self._lgeos = None

    def write(self, geom):
        """Returns WKT string for geometry"""
        if geom is None or geom._geom is None:
            raise ValueError("Null geometry supports no operations")
        result = self._lgeos.GEOSWKTWriter_write(self._writer, geom._geom)
        text = string_at(result)
        lgeos.GEOSFree(result)
        if sys.version_info[0] >= 3:
            return text.decode('ascii')
        else:
            return text


class WKBReader(object):

    _lgeos = None
    _reader = None

    def __init__(self, lgeos):
        """Create WKB Reader"""
        self._lgeos = lgeos
        self._reader = self._lgeos.GEOSWKBReader_create()

    def __del__(self):
        """Destroy WKB Reader"""
        if self._lgeos is not None:
            self._lgeos.GEOSWKBReader_destroy(self._reader)
            self._reader = None
            self._lgeos = None

    def read(self, data):
        """Returns geometry from WKB"""
        geom = self._lgeos.GEOSWKBReader_read(
            self._reader, c_char_p(data), c_size_t(len(data)))
        if not geom:
            raise WKBReadingError(
                "Could not create geometry because of errors "
                "while reading input.")
        # avoid circular import dependency
        from shapely import geometry
        return geometry.base.geom_factory(geom)

    def read_hex(self, data):
        """Returns geometry from WKB hex"""
        if sys.version_info[0] >= 3:
            data = data.encode('ascii')
        geom = self._lgeos.GEOSWKBReader_readHEX(
            self._reader, c_char_p(data), c_size_t(len(data)))
        if not geom:
            raise WKBReadingError(
                "Could not create geometry because of errors "
                "while reading input.")
        # avoid circular import dependency
        from shapely import geometry
        return geometry.base.geom_factory(geom)


class WKBWriter(object):

    _lgeos = None
    _writer = None

    # EndianType enum in ByteOrderValues.h
    _ENDIAN_BIG = 0
    _ENDIAN_LITTLE = 1

    # Establish default output setting
    defaults = {'output_dimension': 3}

    @property
    def output_dimension(self):
        """Output dimension, either 2 or 3 (default)"""
        return self._lgeos.GEOSWKBWriter_getOutputDimension(self._writer)

    @output_dimension.setter
    def output_dimension(self, value):
        self._lgeos.GEOSWKBWriter_setOutputDimension(
            self._writer, int(value))

    @property
    def big_endian(self):
        """Byte order is big endian, True (default) or False"""
        return (self._lgeos.GEOSWKBWriter_getByteOrder(self._writer) ==
                self._ENDIAN_BIG)

    @big_endian.setter
    def big_endian(self, value):
        self._lgeos.GEOSWKBWriter_setByteOrder(
            self._writer, self._ENDIAN_BIG if value else self._ENDIAN_LITTLE)

    @property
    def include_srid(self):
        """Include SRID, True or False (default)"""
        return bool(self._lgeos.GEOSWKBWriter_getIncludeSRID(self._writer))

    @include_srid.setter
    def include_srid(self, value):
        self._lgeos.GEOSWKBWriter_setIncludeSRID(self._writer, bool(value))

    def __init__(self, lgeos, **settings):
        """Create WKB Writer"""
        self._lgeos = lgeos
        self._writer = self._lgeos.GEOSWKBWriter_create()

        applied_settings = self.defaults.copy()
        applied_settings.update(settings)
        for name in applied_settings:
            setattr(self, name, applied_settings[name])

    def __setattr__(self, name, value):
        """Limit setting attributes"""
        if hasattr(self, name):
            object.__setattr__(self, name, value)
        else:
            raise AttributeError('%r object has no attribute %r' %
                                 (self.__class__.__name__, name))

    def __del__(self):
        """Destroy WKB Writer"""
        if self._lgeos is not None:
            self._lgeos.GEOSWKBWriter_destroy(self._writer)
            self._writer = None
            self._lgeos = None

    def write(self, geom):
        """Returns WKB byte string for geometry"""
        if geom is None or geom._geom is None:
            raise ValueError("Null geometry supports no operations")
        size = c_size_t()
        result = self._lgeos.GEOSWKBWriter_write(
            self._writer, geom._geom, pointer(size))
        data = string_at(result, size.value)
        lgeos.GEOSFree(result)
        return data

    def write_hex(self, geom):
        """Returns WKB hex string for geometry"""
        if geom is None or geom._geom is None:
            raise ValueError("Null geometry supports no operations")
        size = c_size_t()
        result = self._lgeos.GEOSWKBWriter_writeHEX(
            self._writer, geom._geom, pointer(size))
        data = string_at(result, size.value)
        lgeos.GEOSFree(result)
        if sys.version_info[0] >= 3:
            return data.decode('ascii')
        else:
            return data


# Errcheck functions for ctypes

def errcheck_wkb(result, func, argtuple):
    """Returns bytes from a C pointer"""
    if not result:
        return None
    size_ref = argtuple[-1]
    size = size_ref.contents
    retval = string_at(result, size.value)[:]
    lgeos.GEOSFree(result)
    return retval


def errcheck_just_free(result, func, argtuple):
    """Returns string from a C pointer"""
    retval = string_at(result)
    lgeos.GEOSFree(result)
    if sys.version_info[0] >= 3:
        return retval.decode('ascii')
    else:
        return retval


def errcheck_null_exception(result, func, argtuple):
    """Wraps errcheck_just_free

    Raises TopologicalError if result is NULL.
    """
    if not result:
        raise TopologicalError(
            "The operation '{}' could not be performed."
            "Likely cause is invalidity of the geometry.".format(
                func.__name__))
    return errcheck_just_free(result, func, argtuple)


def errcheck_predicate(result, func, argtuple):
    """Result is 2 on exception, 1 on True, 0 on False"""
    if result == 2:
        raise PredicateError("Failed to evaluate %s" % repr(func))
    return result


class LGEOSBase(threading.local):
    """Proxy for GEOS C API

    This is a base class. Do not instantiate.
    """
    methods = {}

    def __init__(self, dll):
        self._lgeos = dll
        self.geos_handle = None

    def __del__(self):
        """Cleanup GEOS related processes"""
        if self._lgeos is not None:
            self._lgeos.finishGEOS()
            self._lgeos = None
            self.geos_handle = None


class LGEOS300(LGEOSBase):
    """Proxy for GEOS 3.0.0-CAPI-1.4.1
    """
    geos_version = (3, 0, 0)
    geos_capi_version = (1, 4, 0)

    def __init__(self, dll):
        super(LGEOS300, self).__init__(dll)
        self.geos_handle = self._lgeos.initGEOS(notice_h, error_h)
        keys = list(self._lgeos.__dict__.keys())
        for key in keys:
            setattr(self, key, getattr(self._lgeos, key))
        self.GEOSFree = self._lgeos.free
        # Deprecated
        self.GEOSGeomToWKB_buf.errcheck = errcheck_wkb
        self.GEOSGeomToWKT.errcheck = errcheck_just_free
        self.GEOSRelate.errcheck = errcheck_null_exception
        for pred in (
                self.GEOSDisjoint,
                self.GEOSTouches,
                self.GEOSIntersects,
                self.GEOSCrosses,
                self.GEOSWithin,
                self.GEOSContains,
                self.GEOSOverlaps,
                self.GEOSEquals,
                self.GEOSEqualsExact,
                self.GEOSRelatePattern,
                self.GEOSisEmpty,
                self.GEOSisValid,
                self.GEOSisSimple,
                self.GEOSisRing,
                self.GEOSHasZ):
            pred.errcheck = errcheck_predicate

        self.methods['area'] = self.GEOSArea
        self.methods['boundary'] = self.GEOSBoundary
        self.methods['buffer'] = self.GEOSBuffer
        self.methods['centroid'] = self.GEOSGetCentroid
        self.methods['representative_point'] = self.GEOSPointOnSurface
        self.methods['convex_hull'] = self.GEOSConvexHull
        self.methods['distance'] = self.GEOSDistance
        self.methods['envelope'] = self.GEOSEnvelope
        self.methods['length'] = self.GEOSLength
        self.methods['has_z'] = self.GEOSHasZ
        self.methods['is_empty'] = self.GEOSisEmpty
        self.methods['is_ring'] = self.GEOSisRing
        self.methods['is_simple'] = self.GEOSisSimple
        self.methods['is_valid'] = self.GEOSisValid
        self.methods['disjoint'] = self.GEOSDisjoint
        self.methods['touches'] = self.GEOSTouches
        self.methods['intersects'] = self.GEOSIntersects
        self.methods['crosses'] = self.GEOSCrosses
        self.methods['within'] = self.GEOSWithin
        self.methods['contains'] = self.GEOSContains
        self.methods['overlaps'] = self.GEOSOverlaps
        self.methods['equals'] = self.GEOSEquals
        self.methods['equals_exact'] = self.GEOSEqualsExact
        self.methods['relate'] = self.GEOSRelate
        self.methods['difference'] = self.GEOSDifference
        self.methods['symmetric_difference'] = self.GEOSSymDifference
        self.methods['union'] = self.GEOSUnion
        self.methods['intersection'] = self.GEOSIntersection
        self.methods['relate_pattern'] = self.GEOSRelatePattern
        self.methods['simplify'] = self.GEOSSimplify
        self.methods['topology_preserve_simplify'] = \
            self.GEOSTopologyPreserveSimplify


class LGEOS310(LGEOSBase):
    """Proxy for GEOS 3.1.0-CAPI-1.5.0
    """
    geos_version = (3, 1, 0)
    geos_capi_version = (1, 5, 0)

    def __init__(self, dll):
        super(LGEOS310, self).__init__(dll)
        self.geos_handle = self._lgeos.initGEOS_r(notice_h, error_h)
        keys = list(self._lgeos.__dict__.keys())
        for key in [x for x in keys if not x.endswith('_r')]:
            if key + '_r' in keys:
                reentr_func = getattr(self._lgeos, key + '_r')
                attr = partial(reentr_func, self.geos_handle)
                attr.__name__ = reentr_func.__name__
                setattr(self, key, attr)
            else:
                setattr(self, key, getattr(self._lgeos, key))
        if not hasattr(self, 'GEOSFree'):
            # GEOS < 3.1.1
            self.GEOSFree = self._lgeos.free
        # Deprecated
        self.GEOSGeomToWKB_buf.func.errcheck = errcheck_wkb
        self.GEOSGeomToWKT.func.errcheck = errcheck_just_free
        self.GEOSRelate.func.errcheck = errcheck_null_exception
        for pred in (
                self.GEOSDisjoint,
                self.GEOSTouches,
                self.GEOSIntersects,
                self.GEOSCrosses,
                self.GEOSWithin,
                self.GEOSContains,
                self.GEOSOverlaps,
                self.GEOSCovers,
                self.GEOSEquals,
                self.GEOSEqualsExact,
                self.GEOSPreparedDisjoint,
                self.GEOSPreparedTouches,
                self.GEOSPreparedCrosses,
                self.GEOSPreparedWithin,
                self.GEOSPreparedOverlaps,
                self.GEOSPreparedContains,
                self.GEOSPreparedContainsProperly,
                self.GEOSPreparedCovers,
                self.GEOSPreparedIntersects,
                self.GEOSRelatePattern,
                self.GEOSisEmpty,
                self.GEOSisValid,
                self.GEOSisSimple,
                self.GEOSisRing,
                self.GEOSHasZ):
            pred.func.errcheck = errcheck_predicate

        self.GEOSisValidReason.func.errcheck = errcheck_just_free

        self.methods['area'] = self.GEOSArea
        self.methods['boundary'] = self.GEOSBoundary
        self.methods['buffer'] = self.GEOSBuffer
        self.methods['centroid'] = self.GEOSGetCentroid
        self.methods['representative_point'] = self.GEOSPointOnSurface
        self.methods['convex_hull'] = self.GEOSConvexHull
        self.methods['distance'] = self.GEOSDistance
        self.methods['envelope'] = self.GEOSEnvelope
        self.methods['length'] = self.GEOSLength
        self.methods['has_z'] = self.GEOSHasZ
        self.methods['is_empty'] = self.GEOSisEmpty
        self.methods['is_ring'] = self.GEOSisRing
        self.methods['is_simple'] = self.GEOSisSimple
        self.methods['is_valid'] = self.GEOSisValid
        self.methods['disjoint'] = self.GEOSDisjoint
        self.methods['touches'] = self.GEOSTouches
        self.methods['intersects'] = self.GEOSIntersects
        self.methods['crosses'] = self.GEOSCrosses
        self.methods['within'] = self.GEOSWithin
        self.methods['contains'] = self.GEOSContains
        self.methods['overlaps'] = self.GEOSOverlaps
        self.methods['covers'] = self.GEOSCovers
        self.methods['equals'] = self.GEOSEquals
        self.methods['equals_exact'] = self.GEOSEqualsExact
        self.methods['relate'] = self.GEOSRelate
        self.methods['difference'] = self.GEOSDifference
        self.methods['symmetric_difference'] = self.GEOSSymDifference
        self.methods['union'] = self.GEOSUnion
        self.methods['intersection'] = self.GEOSIntersection
        self.methods['prepared_disjoint'] = self.GEOSPreparedDisjoint
        self.methods['prepared_touches'] = self.GEOSPreparedTouches
        self.methods['prepared_intersects'] = self.GEOSPreparedIntersects
        self.methods['prepared_crosses'] = self.GEOSPreparedCrosses
        self.methods['prepared_within'] = self.GEOSPreparedWithin
        self.methods['prepared_contains'] = self.GEOSPreparedContains
        self.methods['prepared_contains_properly'] = \
            self.GEOSPreparedContainsProperly
        self.methods['prepared_overlaps'] = self.GEOSPreparedOverlaps
        self.methods['prepared_covers'] = self.GEOSPreparedCovers
        self.methods['relate_pattern'] = self.GEOSRelatePattern
        self.methods['simplify'] = self.GEOSSimplify
        self.methods['topology_preserve_simplify'] = \
            self.GEOSTopologyPreserveSimplify
        self.methods['cascaded_union'] = self.GEOSUnionCascaded


class LGEOS311(LGEOS310):
    """Proxy for GEOS 3.1.1-CAPI-1.6.0
    """
    geos_version = (3, 1, 1)
    geos_capi_version = (1, 6, 0)

    def __init__(self, dll):
        super(LGEOS311, self).__init__(dll)


class LGEOS320(LGEOS311):
    """Proxy for GEOS 3.2.0-CAPI-1.6.0
    """
    geos_version = (3, 2, 0)
    geos_capi_version = (1, 6, 0)

    def __init__(self, dll):
        super(LGEOS320, self).__init__(dll)

        if geos_version >= (3, 2, 0):

            def parallel_offset(geom, distance, resolution=16, join_style=1,
                                mitre_limit=5.0, side='right'):
                side = side == 'left'
                if distance < 0:
                    distance = abs(distance)
                    side = not side
                return self.GEOSSingleSidedBuffer(
                    geom, distance, resolution, join_style, mitre_limit, side)

            self.methods['parallel_offset'] = parallel_offset

        self.methods['project'] = self.GEOSProject
        self.methods['project_normalized'] = self.GEOSProjectNormalized
        self.methods['interpolate'] = self.GEOSInterpolate
        self.methods['interpolate_normalized'] = \
            self.GEOSInterpolateNormalized
        self.methods['buffer_with_style'] = self.GEOSBufferWithStyle
        self.methods['hausdorff_distance'] = self.GEOSHausdorffDistance


class LGEOS330(LGEOS320):
    """Proxy for GEOS 3.3.0-CAPI-1.7.0
    """
    geos_version = (3, 3, 0)
    geos_capi_version = (1, 7, 0)

    def __init__(self, dll):
        super(LGEOS330, self).__init__(dll)

        # GEOS 3.3.8 from homebrew has, but doesn't advertise
        # GEOSPolygonize_full. We patch it in explicitly here.
        key = 'GEOSPolygonize_full'
        func = getattr(self._lgeos, key + '_r')
        attr = partial(func, self.geos_handle)
        attr.__name__ = func.__name__
        setattr(self, key, attr)

        for pred in (self.GEOSisClosed,):
            pred.func.errcheck = errcheck_predicate

        def parallel_offset(geom, distance, resolution=16, join_style=1,
                            mitre_limit=5.0, side='right'):
            if side == 'right':
                distance *= -1
            return self.GEOSOffsetCurve(
                geom, distance, resolution, join_style, mitre_limit)

        self.methods['parallel_offset'] = parallel_offset
        self.methods['unary_union'] = self.GEOSUnaryUnion
        self.methods['is_closed'] = self.GEOSisClosed
        self.methods['cascaded_union'] = self.methods['unary_union']
        self.methods['snap'] = self.GEOSSnap
        self.methods['shared_paths'] = self.GEOSSharedPaths
        self.methods['buffer_with_params'] = self.GEOSBufferWithParams


class LGEOS340(LGEOS330):
    """Proxy for GEOS 3.4.0-CAPI-1.8.0
    """
    geos_version = (3, 4, 0)
    geos_capi_version = (1, 8, 0)

    def __init__(self, dll):
        super(LGEOS340, self).__init__(dll)
        self.methods['delaunay_triangulation'] = self.GEOSDelaunayTriangulation
        self.methods['nearest_points'] = self.GEOSNearestPoints


class LGEOS350(LGEOS340):
    """Proxy for GEOS 3.5.0-CAPI-1.9.0
    """
    geos_version = (3, 5, 0)
    geos_capi_version = (1, 9, 0)

    def __init__(self, dll):
        super(LGEOS350, self).__init__(dll)
        self.methods['clip_by_rect'] = self.GEOSClipByRect


class LGEOS360(LGEOS350):
    """Proxy for GEOS 3.6.0-CAPI-1.10.0
    """
    geos_version = (3, 6, 0)
    geos_capi_version = (1, 10, 0)

    def __init__(self, dll):
        super(LGEOS360, self).__init__(dll)
        self.methods['minimum_clearance'] = self.GEOSMinimumClearance


if geos_version >= (3, 6, 0):
    L = LGEOS360
elif geos_version >= (3, 5, 0):
    L = LGEOS350
elif geos_version >= (3, 4, 0):
    L = LGEOS340
elif geos_version >= (3, 3, 0):
    L = LGEOS330
elif geos_version >= (3, 2, 0):
    L = LGEOS320
elif geos_version >= (3, 1, 1):
    L = LGEOS311
elif geos_version >= (3, 1, 0):
    L = LGEOS310
else:
    L = LGEOS300

lgeos = L(_lgeos)


def cleanup(proxy):
    del proxy

atexit.register(cleanup, lgeos)
