#define PY_SSIZE_T_CLEAN

#include <Python.h>

#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL shapely_ARRAY_API

#include <numpy/ndarraytypes.h>
#include <numpy/npy_math.h>

#include "geos.h"


/* Check simple geometries (Point, LineString, LinearRing; backed by a single CoordSequence).
Returns 1 on true, 0 on false, 2 on exception */
char equals_identical_simple(GEOSContextHandle_t ctx, const GEOSGeometry* geom1,
                             const GEOSGeometry* geom2) {
  char hasZ1, hasZ2;
  const GEOSCoordSequence *coord_seq1 = NULL;
  const GEOSCoordSequence *coord_seq2 = NULL;
  unsigned int n1, n2, dims1, dims2;
  double* buf1 = NULL;
  double* buf2 = NULL;
  npy_intp i;
  char ret;

  hasZ1 = GEOSHasZ_r(ctx, geom1);
  if (hasZ1 == 2) {
    return 2;
  }
  hasZ2 = GEOSHasZ_r(ctx, geom2);
  if (hasZ2 == 2) {
    return 2;
  }
  if (hasZ1 != hasZ2) {
    return 0;
  }

  coord_seq1 = GEOSGeom_getCoordSeq_r(ctx, geom1);
  if (coord_seq1 == NULL) {
    return 2;
  }
  coord_seq2 = GEOSGeom_getCoordSeq_r(ctx, geom2);
  if (coord_seq2 == NULL) {
    return 2;
  }

  if (GEOSCoordSeq_getSize_r(ctx, coord_seq1, &n1) == 0) {
    return 2;
  }
  if (GEOSCoordSeq_getSize_r(ctx, coord_seq2, &n2) == 0) {
    return 2;
  }
  if (n1 != n2) {
    return 0;
  }

  if (GEOSCoordSeq_getDimensions_r(ctx, coord_seq1, &dims1) == 0) {
    return 2;
  }
  if (GEOSCoordSeq_getDimensions_r(ctx, coord_seq2, &dims2) == 0) {
    return 2;
  }
  if (dims1 != dims2) {
    return 0;
  }

#if !GEOS_SINCE_3_10_0
  // Correction to hasZ for GEOS<3.10 to avoid false detection of hasM
  hasZ1 = dims1 == 3;
  hasZ2 = dims2 == 3;
#endif // !GEOS_SINCE_3_10_0

  int hasM = 0;
  if ((dims1 == 3) & (hasZ1 == 0)) {
    hasM = 1;
  }
  if (dims1 == 4) {
    hasM = 1;
  }

  buf1 = malloc(sizeof(double) * n1 * dims1);
  if (coordseq_to_buffer(ctx, coord_seq1, buf1, n1, hasZ1, hasM) == 0) {
    free(buf1);
    return 2;
  }
  buf2 = malloc(sizeof(double) * n1 * dims1);
  if (coordseq_to_buffer(ctx, coord_seq2, buf2, n1, hasZ1, hasM) == 0) {
    free(buf1);
    free(buf2);
    return 2;
  }

  ret = 1;
  for (i = 0; i < n1 * dims1; i++) {
    const double a = buf1[i];
    const double b = buf2[i];
    // NaN coordinates in same position are considered equal
    if (a != b && !(npy_isnan(a) && npy_isnan(b))) {
      ret = 0;
      break;
    }
  }

  free(buf1);
  free(buf2);
  return ret;
}

/* Check the exterior/interior rings of Polygons.
Returns 1 on true, 0 on false, 2 on exception */
char equals_identical_polygon(GEOSContextHandle_t ctx, const GEOSGeometry* geom1,
                              const GEOSGeometry* geom2) {
  int i, n1, n2;
  const GEOSGeometry *ring1 = NULL;
  const GEOSGeometry *ring2 = NULL;
  char ret;

  n1 = GEOSGetNumInteriorRings_r(ctx, geom1);
  if (n1 == -1) {
    return 2;
  }
  n2 = GEOSGetNumInteriorRings_r(ctx, geom2);
  if (n2 == -1) {
    return 2;
  }
  if (n1 != n2) {
    return 0;
  }

  /* validate the exterior ring */
  ring1 = GEOSGetExteriorRing_r(ctx, geom1);
  if (ring1 == NULL) {
    return 2;
  }
  ring2 = GEOSGetExteriorRing_r(ctx, geom2);
  if (ring2 == NULL) {
    return 2;
  }
  ret = equals_identical_simple(ctx, ring1, ring2);
  if (ret != 1) {
    return ret;
  }
  // we don't own the shells, so don't need to destroy them

  /* validate the interior rings */
  for (i = 0; i < n1; i++) {
    ring1 = GEOSGetInteriorRingN_r(ctx, geom1, i);
    if (ring1 == NULL) {
      return 2;
    }
    ring2 = GEOSGetInteriorRingN_r(ctx, geom2, i);
    if (ring2 == NULL) {
      return 2;
    }
    ret = equals_identical_simple(ctx, ring1, ring2);
    if (ret != 1) {
      return ret;
    }
  }
  return 1;
}

/* Note: forward declaration of the final function that is defined further below,
so that it can be used in equals_identical_collection just below (due to the
recursive nature of the implementation). */
char equals_identical(GEOSContextHandle_t ctx, const GEOSGeometry* geom1,
                      const GEOSGeometry* geom2);

/* Check the sub-geometries of Multi-part geometries.
Returns 1 on true, 0 on false, 2 on exception */
char equals_identical_collection(GEOSContextHandle_t ctx, const GEOSGeometry* geom1,
                                 const GEOSGeometry* geom2) {
  int i, n1, n2;
  const GEOSGeometry *sub_geom1 = NULL;
  const GEOSGeometry *sub_geom2 = NULL;
  char ret;

  n1 = GEOSGetNumGeometries_r(ctx, geom1);
  if (n1 == -1) {
    return 2;
  }
  n2 = GEOSGetNumGeometries_r(ctx, geom2);
  if (n2 == -1) {
    return 2;
  }
  if (n1 != n2) {
    return 0;
  }

  /* validate the sub geometries */
  for (i = 0; i < n1; i++) {
    sub_geom1 = GEOSGetGeometryN_r(ctx, geom1, i);
    if (sub_geom1 == NULL) {
      return 2;
    }
    sub_geom2 = GEOSGetGeometryN_r(ctx, geom2, i);
    if (sub_geom2 == NULL) {
      return 2;
    }
    ret = equals_identical(ctx, sub_geom1, sub_geom2);
    if (ret != 1) {
      return ret;
    }
  }
  return 1;
}

/* Check geometries for being identical.
Returns 1 on true, 0 on false, 2 on exception */
char equals_identical(GEOSContextHandle_t ctx, const GEOSGeometry* geom1,
                      const GEOSGeometry* geom2) {
  int type1, type2;

  type1 = GEOSGeomTypeId_r(ctx, geom1);
  if (type1 == -1) {
    return 2;
  }
  type2 = GEOSGeomTypeId_r(ctx, geom2);
  if (type2 == -1) {
    return 2;
  }
  if (type1 != type2) {
    return 0;
  }

  switch (type1) {
    case GEOS_POINT:
    case GEOS_LINESTRING:
    case GEOS_LINEARRING:
      return equals_identical_simple(ctx, geom1, geom2);
    case GEOS_POLYGON:
      return equals_identical_polygon(ctx, geom1, geom2);
    case GEOS_MULTIPOINT:
    case GEOS_MULTILINESTRING:
    case GEOS_MULTIPOLYGON:
    case GEOS_GEOMETRYCOLLECTION:
      return equals_identical_collection(ctx, geom1, geom2);
    default:
      return 2;
  }
}

/*
 * Determine pointwise equivalence of two geometries by checking
 * that the structure, ordering, and values of all vertices are
 * identical in all dimensions. NaN values are considered to be
 * equal to other NaN values in the same position within coordinate sequences.
 * \returns 1 on true, 0 on false, 2 on exception
 */
char PyGEOSEqualsIdentical(GEOSContextHandle_t ctx, const GEOSGeometry* geom1,
                           const GEOSGeometry* geom2) {
#if GEOS_SINCE_3_12_0
  return GEOSEqualsIdentical_r(ctx, geom1, geom2);
#else
  return equals_identical(ctx, geom1, geom2);
#endif
}
