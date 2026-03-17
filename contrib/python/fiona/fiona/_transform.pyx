# distutils: language = c++
#
# Coordinate and geometry transformations.

include "gdal.pxi"

import logging
import warnings
from collections import UserDict

from fiona cimport _cpl, _csl, _geometry
from fiona.crs cimport OGRSpatialReferenceH, osr_set_traditional_axis_mapping_strategy

from fiona.compat import DICT_TYPES
from fiona.crs import CRS
from fiona.errors import TransformError
from fiona.model import Geometry


cdef extern from "ogr_geometry.h" nogil:

    cdef cppclass OGRGeometry:
        pass

    cdef cppclass OGRGeometryFactory:
        void * transformWithOptions(void *geom, void *ct, char **options)


cdef extern from "ogr_spatialref.h":

    cdef cppclass OGRCoordinateTransformation:
        pass


log = logging.getLogger(__name__)
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log.addHandler(NullHandler())


cdef void *_crs_from_crs(object crs):
    cdef char *wkt_c = NULL
    cdef OGRSpatialReferenceH osr = NULL
    osr = OSRNewSpatialReference(NULL)

    if osr == NULL:
        raise ValueError("NULL spatial reference")

    params = []

    wkt = CRS.from_user_input(crs).to_wkt()

    wkt_b = wkt.encode('utf-8')
    wkt_c = wkt_b
    OSRSetFromUserInput(osr, wkt_c)

    osr_set_traditional_axis_mapping_strategy(osr)
    return osr


def _transform(src_crs, dst_crs, xs, ys):
    cdef double *x
    cdef double *y
    cdef char *proj_c = NULL
    cdef OGRSpatialReferenceH src = NULL
    cdef OGRSpatialReferenceH dst = NULL
    cdef void *transform = NULL
    cdef int i

    assert len(xs) == len(ys)

    src = _crs_from_crs(src_crs)
    dst = _crs_from_crs(dst_crs)

    n = len(xs)
    x = <double *>_cpl.CPLMalloc(n*sizeof(double))
    y = <double *>_cpl.CPLMalloc(n*sizeof(double))
    for i in range(n):
        x[i] = xs[i]
        y[i] = ys[i]

    transform = OCTNewCoordinateTransformation(src, dst)
    res = OCTTransform(transform, n, x, y, NULL)

    res_xs = [0]*n
    res_ys = [0]*n

    for i in range(n):
        res_xs[i] = x[i]
        res_ys[i] = y[i]

    _cpl.CPLFree(x)
    _cpl.CPLFree(y)
    OCTDestroyCoordinateTransformation(transform)
    OSRRelease(src)
    OSRRelease(dst)
    return res_xs, res_ys


cdef object _transform_single_geom(
    object single_geom,
    OGRGeometryFactory *factory,
    void *transform,
    char **options,
):
    """Transform a single geometry."""
    cdef void *src_ogr_geom = NULL
    cdef void *dst_ogr_geom = NULL

    src_ogr_geom = _geometry.OGRGeomBuilder().build(single_geom)
    dst_ogr_geom = factory.transformWithOptions(
                    <const OGRGeometry *>src_ogr_geom,
                    <OGRCoordinateTransformation *>transform,
                    options)

    if dst_ogr_geom == NULL and CPLGetConfigOption("OGR_ENABLE_PARTIAL_REPROJECTION", "OFF") != b"ON":
        raise TransformError(
            "Full reprojection failed. To enable partial reprojection set OGR_ENABLE_PARTIAL_REPROJECTION=True"
        )
    else:
        out_geom = _geometry.GeomBuilder().build(dst_ogr_geom)
        _geometry.OGR_G_DestroyGeometry(dst_ogr_geom)

    if src_ogr_geom != NULL:
        _geometry.OGR_G_DestroyGeometry(src_ogr_geom)

    return out_geom


def _transform_geom(src_crs, dst_crs, geom, antimeridian_cutting, antimeridian_offset, precision):
    """Return transformed geometries.

    """
    cdef char *proj_c = NULL
    cdef char *key_c = NULL
    cdef char *val_c = NULL
    cdef char **options = NULL
    cdef OGRSpatialReferenceH src = NULL
    cdef OGRSpatialReferenceH dst = NULL
    cdef void *transform = NULL
    cdef OGRGeometryFactory *factory = NULL

    if not all([src_crs, dst_crs]):
        raise RuntimeError("Must provide a source and destination CRS.")

    src = _crs_from_crs(src_crs)
    dst = _crs_from_crs(dst_crs)
    transform = OCTNewCoordinateTransformation(src, dst)

    # Transform options.
    options = _csl.CSLSetNameValue(
        options,
        "DATELINEOFFSET",
        str(antimeridian_offset).encode('utf-8')
    )

    if antimeridian_cutting:
        options = _csl.CSLSetNameValue(options, "WRAPDATELINE", "YES")

    factory = new OGRGeometryFactory()

    if isinstance(geom, Geometry):
        out_geom = recursive_round(
            _transform_single_geom(geom, factory, transform, options), precision)
    else:
        out_geom = [
            recursive_round(
                _transform_single_geom(single_geom, factory, transform, options),
                precision,
            )
            for single_geom in geom
        ]

    OCTDestroyCoordinateTransformation(transform)

    if options != NULL:
        _csl.CSLDestroy(options)

    OSRRelease(src)
    OSRRelease(dst)

    return out_geom


def recursive_round(obj, precision):
    """Recursively round coordinates."""
    if precision < 0:
        return obj
    if getattr(obj, 'geometries', None):
        return Geometry(geometries=[recursive_round(part, precision) for part in obj.geometries])
    elif getattr(obj, 'coordinates', None):
        return Geometry(coordinates=[recursive_round(part, precision) for part in obj.coordinates])
    if isinstance(obj, (int, float)):
        return round(obj, precision)
    else:
        return [recursive_round(part, precision) for part in obj]
