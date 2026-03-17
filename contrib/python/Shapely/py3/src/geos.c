#define PY_SSIZE_T_CLEAN

#include "geos.h"

#include <Python.h>
#include <numpy/ndarraytypes.h>
#include <numpy/npy_math.h>
#include <structmember.h>

/* This initializes a globally accessible GEOS Context, only to be used when holding the
 * GIL */
void* geos_context[1] = {NULL};

/* This initializes a globally accessible GEOSException object */
PyObject* geos_exception[1] = {NULL};

int init_geos(PyObject* m) {
  PyObject* base_class = PyErr_NewException("shapely.errors.ShapelyError", NULL, NULL);
  PyModule_AddObject(m, "ShapelyError", base_class);
  geos_exception[0] =
      PyErr_NewException("shapely.errors.GEOSException", base_class, NULL);
  PyModule_AddObject(m, "GEOSException", geos_exception[0]);

  void* context_handle = GEOS_init_r();
  // TODO: the error handling is not yet set up for the global context (it is right now
  // only used where error handling is not used)
  // GEOSContext_setErrorMessageHandler_r(context_handle, geos_error_handler, last_error);
  geos_context[0] = context_handle;

  return 0;
}

void destroy_geom_arr(void* context, GEOSGeometry** array, int length) {
  int i;
  for (i = 0; i < length; i++) {
    if (array[i] != NULL) {
      GEOSGeom_destroy_r(context, array[i]);
    }
  }
}

/* These functions are used to workaround issues in GeoJSON writer for GEOS 3.10.0:
 * - POINT EMPTY was not handled correctly (we do it ourselves)
 * - MULTIPOINT (EMPTY) resulted in segfault (we check for it and raise)
 */

/* Returns 1 if a multipoint has an empty point, 0 otherwise, 2 on error.
 */
char multipoint_has_point_empty(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int n, i;
  char is_empty;
  const GEOSGeometry* sub_geom;

  n = GEOSGetNumGeometries_r(ctx, geom);
  if (n == -1) {
    return 2;
  }
  for (i = 0; i < n; i++) {
    sub_geom = GEOSGetGeometryN_r(ctx, geom, i);
    if (sub_geom == NULL) {
      return 2;
    }
    is_empty = GEOSisEmpty_r(ctx, sub_geom);
    if (is_empty != 0) {
      // If empty encountered, or on exception, return:
      return is_empty;
    }
  }
  return 0;
}

/* Returns 1 if geometry is an empty point, 0 otherwise, 2 on error.
 */
char is_point_empty(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int geom_type;

  geom_type = GEOSGeomTypeId_r(ctx, geom);
  if (geom_type == GEOS_POINT) {
    return GEOSisEmpty_r(ctx, geom);
  } else if (geom_type == -1) {
    return 2;  // GEOS exception
  } else {
    return 0;  // No empty point
  }
}

/* Returns 1 if a geometrycollection has an empty point, 0 otherwise, 2 on error.
Checks recursively (geometrycollections may contain multipoints / geometrycollections)
*/
char geometrycollection_has_point_empty(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int n, i;
  char has_empty;
  const GEOSGeometry* sub_geom;

  n = GEOSGetNumGeometries_r(ctx, geom);
  if (n == -1) {
    return 2;
  }
  for (i = 0; i < n; i++) {
    sub_geom = GEOSGetGeometryN_r(ctx, geom, i);
    if (sub_geom == NULL) {
      return 2;
    }
    has_empty = has_point_empty(ctx, (GEOSGeometry*)sub_geom);
    if (has_empty != 0) {
      // If empty encountered, or on exception, return:
      return has_empty;
    }
  }
  return 0;
}

/* Returns 1 if geometry is / has an empty point, 0 otherwise, 2 on error.
 */
char has_point_empty(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int geom_type;

  geom_type = GEOSGeomTypeId_r(ctx, geom);
  if (geom_type == GEOS_POINT) {
    return GEOSisEmpty_r(ctx, geom);
  } else if (geom_type == GEOS_MULTIPOINT) {
    return multipoint_has_point_empty(ctx, geom);
  } else if (geom_type == GEOS_GEOMETRYCOLLECTION) {
    return geometrycollection_has_point_empty(ctx, geom);
  } else if (geom_type == -1) {
    return 2;  // GEOS exception
  } else {
    return 0;  // No empty point
  }
}

/* Creates a POINT (nan, nan[, nan)] from a POINT EMPTY template

   Returns NULL on error
*/
GEOSGeometry* point_empty_to_nan(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int j, ndim;
  GEOSCoordSequence* coord_seq;
  GEOSGeometry* result;

  ndim = GEOSGeom_getCoordinateDimension_r(ctx, geom);
  if (ndim == 0) {
    return NULL;
  }

  coord_seq = GEOSCoordSeq_create_r(ctx, 1, ndim);
  if (coord_seq == NULL) {
    return NULL;
  }
  for (j = 0; j < ndim; j++) {
    if (!GEOSCoordSeq_setOrdinate_r(ctx, coord_seq, 0, j, Py_NAN)) {
      GEOSCoordSeq_destroy_r(ctx, coord_seq);
      return NULL;
    }
  }
  result = GEOSGeom_createPoint_r(ctx, coord_seq);
  if (result == NULL) {
    GEOSCoordSeq_destroy_r(ctx, coord_seq);
    return NULL;
  }
  GEOSSetSRID_r(ctx, result, GEOSGetSRID_r(ctx, geom));
  return result;
}

/* Creates a new multipoint, replacing empty points with POINT (nan, nan[, nan)]

   Returns NULL on error
*/
GEOSGeometry* multipoint_empty_to_nan(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int n, i;
  GEOSGeometry* result;
  const GEOSGeometry* sub_geom;

  n = GEOSGetNumGeometries_r(ctx, geom);
  if (n == -1) {
    return NULL;
  }

  GEOSGeometry** geoms = malloc(sizeof(void*) * n);
  for (i = 0; i < n; i++) {
    sub_geom = GEOSGetGeometryN_r(ctx, geom, i);
    if (GEOSisEmpty_r(ctx, sub_geom)) {
      geoms[i] = point_empty_to_nan(ctx, (GEOSGeometry*)sub_geom);
    } else {
      geoms[i] = GEOSGeom_clone_r(ctx, (GEOSGeometry*)sub_geom);
    }
    // If the function errored: cleanup and return
    if (geoms[i] == NULL) {
      destroy_geom_arr(ctx, geoms, i);
      free(geoms);
      return NULL;
    }
  }

  result = GEOSGeom_createCollection_r(ctx, GEOS_MULTIPOINT, geoms, n);
  // If the function errored: cleanup and return
  if (result == NULL) {
    destroy_geom_arr(ctx, geoms, i);
    free(geoms);
    return NULL;
  }

  free(geoms);
  GEOSSetSRID_r(ctx, result, GEOSGetSRID_r(ctx, geom));
  return result;
}

/* Creates a new geometrycollection, replacing all empty points with POINT (nan, nan[,
   nan)]

   Returns NULL on error
*/
GEOSGeometry* geometrycollection_empty_to_nan(GEOSContextHandle_t ctx,
                                              GEOSGeometry* geom) {
  int n, i;
  GEOSGeometry* result = NULL;
  const GEOSGeometry* sub_geom;

  n = GEOSGetNumGeometries_r(ctx, geom);
  if (n == -1) {
    return NULL;
  }

  GEOSGeometry** geoms = malloc(sizeof(void*) * n);
  for (i = 0; i < n; i++) {
    sub_geom = GEOSGetGeometryN_r(ctx, geom, i);
    geoms[i] = point_empty_to_nan_all_geoms(ctx, (GEOSGeometry*)sub_geom);
    // If the function errored: cleanup and return
    if (geoms[i] == NULL) {
      goto finish;
    }
  }

  result = GEOSGeom_createCollection_r(ctx, GEOS_GEOMETRYCOLLECTION, geoms, n);

finish:

  // If the function errored: cleanup, else set SRID
  if (result == NULL) {
    destroy_geom_arr(ctx, geoms, i);
  } else {
    GEOSSetSRID_r(ctx, result, GEOSGetSRID_r(ctx, geom));
  }
  free(geoms);
  return result;
}

/* Creates a new geometry, replacing empty points with POINT (nan, nan[, nan)]

   Returns NULL on error.
*/
GEOSGeometry* point_empty_to_nan_all_geoms(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  int geom_type;
  GEOSGeometry* result;

  geom_type = GEOSGeomTypeId_r(ctx, geom);
  if (geom_type == -1) {
    result = NULL;
  } else if (is_point_empty(ctx, geom)) {
    result = point_empty_to_nan(ctx, geom);
  } else if (geom_type == GEOS_MULTIPOINT) {
    result = multipoint_empty_to_nan(ctx, geom);
  } else if (geom_type == GEOS_GEOMETRYCOLLECTION) {
    result = geometrycollection_empty_to_nan(ctx, geom);
  } else {
    result = GEOSGeom_clone_r(ctx, geom);
  }

  GEOSSetSRID_r(ctx, result, GEOSGetSRID_r(ctx, geom));
  return result;
}

/* Checks whether the geometry contains a coordinate greater than 1E+100
 *
 * See also:
 *
 * https://github.com/shapely/shapely/issues/1903
 */
char get_zmax(GEOSContextHandle_t, const GEOSGeometry*, double*);
char get_zmax_simple(GEOSContextHandle_t, const GEOSGeometry*, double*);
char get_zmax_polygon(GEOSContextHandle_t, const GEOSGeometry*, double*);
char get_zmax_collection(GEOSContextHandle_t, const GEOSGeometry*, double*);

char get_zmax(GEOSContextHandle_t ctx, const GEOSGeometry* geom, double* zmax) {
  int type = GEOSGeomTypeId_r(ctx, geom);
  if ((type == 0) || (type == 1) || (type == 2)) {
    return get_zmax_simple(ctx, geom, zmax);
  } else if (type == 3) {
    return get_zmax_polygon(ctx, geom, zmax);
  } else if ((type >= 4) && (type <= 7)) {
    return get_zmax_collection(ctx, geom, zmax);
  } else {
    return 0;
  }
}

char get_zmax_simple(GEOSContextHandle_t ctx, const GEOSGeometry* geom, double* zmax) {
  const GEOSCoordSequence* seq;
  unsigned int n, i;
  double coord;

  seq = GEOSGeom_getCoordSeq_r(ctx, geom);
  if (seq == NULL) {
    return 0;
  }
  if (GEOSCoordSeq_getSize_r(ctx, seq, &n) == 0) {
    return 0;
  }

  for (i = 0; i < n; i++) {
    if (!GEOSCoordSeq_getZ_r(ctx, seq, i, &coord)) {
      return 0;
    }
    if (npy_isfinite(coord) && (coord > *zmax)) {
      *zmax = coord;
    }
  }
  return 1;
}

char get_zmax_polygon(GEOSContextHandle_t ctx, const GEOSGeometry* geom, double* zmax) {
  const GEOSGeometry* ring;
  int n, i;

  ring = GEOSGetExteriorRing_r(ctx, geom);
  if (ring == NULL) {
    return 0;
  }
  if (!get_zmax_simple(ctx, ring, zmax)) {
    return 0;
  }
  n = GEOSGetNumInteriorRings_r(ctx, geom);
  if (n == -1) {
    return 0;
  }

  for (i = 0; i < n; i++) {
    ring = GEOSGetInteriorRingN_r(ctx, geom, i);
    if (ring == NULL) {
      return 0;
    }
    if (!get_zmax_simple(ctx, ring, zmax)) {
      return 0;
    }
  }
  return 1;
}

char get_zmax_collection(GEOSContextHandle_t ctx, const GEOSGeometry* geom,
                         double* zmax) {
  const GEOSGeometry* elem;
  int n, i;

  n = GEOSGetNumGeometries_r(ctx, geom);
  if (n == -1) {
    return 0;
  }

  for (i = 0; i < n; i++) {
    elem = GEOSGetGeometryN_r(ctx, geom, i);
    if (elem == NULL) {
      return 0;
    }
    if (!get_zmax(ctx, elem, zmax)) {
      return 0;
    }
  }
  return 1;
}

#if !GEOS_SINCE_3_13_0
char check_to_wkt_trim_compatible(GEOSContextHandle_t ctx, const GEOSGeometry* geom,
                                  int dimension) {
  double xmax = 0.0;
  double ymax = 0.0;
  double zmax = 0.0;

  if (GEOSisEmpty_r(ctx, geom)) {
    return PGERR_SUCCESS;
  }

  // use max coordinates to check if any coordinate is too large
  if (!(GEOSGeom_getXMax_r(ctx, geom, &xmax) && GEOSGeom_getYMax_r(ctx, geom, &ymax))) {
    return PGERR_GEOS_EXCEPTION;
  }

  if ((dimension > 2) && GEOSHasZ_r(ctx, geom)) {
    if (!get_zmax(ctx, geom, &zmax)) {
      return PGERR_GEOS_EXCEPTION;
    }
  }

  if ((npy_isfinite(xmax) && (xmax > 1E100)) || (npy_isfinite(ymax) && (ymax > 1E100)) ||
      (npy_isfinite(zmax) && (zmax > 1E100))) {
    return PGERR_COORD_OUT_OF_BOUNDS;
  }

  return PGERR_SUCCESS;
}
#endif  // !GEOS_SINCE_3_13_0

#if !GEOS_SINCE_3_12_0

/* Checks whether the geometry is a 3D empty geometry and, if so, create the WKT string
 *
 * GEOS 3.9.* is able to distinguish 2D and 3D simple geometries (non-collections). But
 * the WKT serialization never writes a 3D empty geometry. This function fixes that.
 * It only makes sense to use this for GEOS versions >= 3.9 && < 3.12.
 *
 * Pending GEOS ticket: https://trac.osgeo.org/geos/ticket/1129
 *
 * If the geometry is a 3D empty, then the (char**) wkt argument is filled with the
 * correct WKT string. Else, wkt becomes NULL and the GEOS WKT writer should be used.
 *
 * The return value is one of:
 * - PGERR_SUCCESS
 * - PGERR_GEOS_EXCEPTION
 */
char wkt_empty_3d_geometry(GEOSContextHandle_t ctx, GEOSGeometry* geom, char** wkt) {
  char is_empty;
  int geom_type;

  is_empty = GEOSisEmpty_r(ctx, geom);
  if (is_empty == 2) {
    return PGERR_GEOS_EXCEPTION;
  } else if (is_empty == 0) {
    *wkt = NULL;
    return PGERR_SUCCESS;
  }
  if (GEOSGeom_getCoordinateDimension_r(ctx, geom) == 2) {
    *wkt = NULL;
    return PGERR_SUCCESS;
  }
  geom_type = GEOSGeomTypeId_r(ctx, geom);
  switch (geom_type) {
    case GEOS_POINT:
      *wkt = "POINT Z EMPTY";
      return PGERR_SUCCESS;
    case GEOS_LINESTRING:
      *wkt = "LINESTRING Z EMPTY";
      break;
    case GEOS_LINEARRING:
      *wkt = "LINEARRING Z EMPTY";
      break;
    case GEOS_POLYGON:
      *wkt = "POLYGON Z EMPTY";
      break;
    // Note: Empty collections cannot be 3D in GEOS.
    // We do include the options in case of future support.
    case GEOS_MULTIPOINT:
      *wkt = "MULTIPOINT Z EMPTY";
      break;
    case GEOS_MULTILINESTRING:
      *wkt = "MULTILINESTRING Z EMPTY";
      break;
    case GEOS_MULTIPOLYGON:
      *wkt = "MULTIPOLYGON Z EMPTY";
      break;
    case GEOS_GEOMETRYCOLLECTION:
      *wkt = "GEOMETRYCOLLECTION Z EMPTY";
      break;
    default:
      return PGERR_GEOS_EXCEPTION;
  }
  return PGERR_SUCCESS;
}

#endif  // !GEOS_SINCE_3_12_0

/* GEOSInterpolate_r and GEOSInterpolateNormalized_r segfault on empty
 * geometries and also on collections with the first geometry empty.
 *
 * This function returns:
 * - PGERR_GEOMETRY_TYPE on non-linear geometries
 * - PGERR_EMPTY_GEOMETRY on empty linear geometries
 * - PGERR_EXCEPTIONS on GEOS exceptions
 * - PGERR_SUCCESS on a non-empty and linear geometry
 */
char geos_interpolate_checker(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  char type;
  char is_empty;
  const GEOSGeometry* sub_geom;

  // Check if the geometry is linear
  type = GEOSGeomTypeId_r(ctx, geom);
  if (type == -1) {
    return PGERR_GEOS_EXCEPTION;
  } else if ((type == GEOS_POINT) || (type == GEOS_POLYGON) ||
             (type == GEOS_MULTIPOINT) || (type == GEOS_MULTIPOLYGON)) {
    return PGERR_GEOMETRY_TYPE;
  }

  // Check if the geometry is empty
  is_empty = GEOSisEmpty_r(ctx, geom);
  if (is_empty == 1) {
    return PGERR_EMPTY_GEOMETRY;
  } else if (is_empty == 2) {
    return PGERR_GEOS_EXCEPTION;
  }

  // For collections: also check the type and emptyness of the first geometry
  if ((type == GEOS_MULTILINESTRING) || (type == GEOS_GEOMETRYCOLLECTION)) {
    sub_geom = GEOSGetGeometryN_r(ctx, geom, 0);
    if (sub_geom == NULL) {
      return PGERR_GEOS_EXCEPTION;  // GEOSException
    }
    type = GEOSGeomTypeId_r(ctx, sub_geom);
    if (type == -1) {
      return PGERR_GEOS_EXCEPTION;
    } else if ((type != GEOS_LINESTRING) && (type != GEOS_LINEARRING)) {
      return PGERR_GEOMETRY_TYPE;
    }
    is_empty = GEOSisEmpty_r(ctx, sub_geom);
    if (is_empty == 1) {
      return PGERR_EMPTY_GEOMETRY;
    } else if (is_empty == 2) {
      return PGERR_GEOS_EXCEPTION;
    }
  }
  return PGERR_SUCCESS;
}

/* Define the GEOS error handler. See GEOS_INIT / GEOS_FINISH macros in geos.h*/
void geos_error_handler(const char* message, void* userdata) {
  snprintf(userdata, 1024, "%s", message);
}

/* Extract bounds from geometry.
 *
 * Bounds coordinates will be set to NPY_NAN if geom is NULL, empty, or does not have an
 * envelope.
 *
 * Parameters
 * ----------
 * ctx: GEOS context handle
 * geom: pointer to GEOSGeometry; can be NULL
 * xmin: pointer to xmin value
 * ymin: pointer to ymin value
 * xmax: pointer to xmax value
 * ymax: pointer to ymax value
 *
 * Must be called from within a GEOS_INIT_THREADS / GEOS_FINISH_THREADS
 * or GEOS_INIT / GEOS_FINISH block.
 *
 * Returns
 * -------
 * 1 on success; 0 on error
 */
int get_bounds(GEOSContextHandle_t ctx, GEOSGeometry* geom, double* xmin, double* ymin,
               double* xmax, double* ymax) {
  int retval = 1;

  if (geom == NULL || GEOSisEmpty_r(ctx, geom)) {
    *xmin = *ymin = *xmax = *ymax = NPY_NAN;
    return 1;
  }

  // use min / max coordinates
  if (!(GEOSGeom_getXMin_r(ctx, geom, xmin) && GEOSGeom_getYMin_r(ctx, geom, ymin) &&
        GEOSGeom_getXMax_r(ctx, geom, xmax) && GEOSGeom_getYMax_r(ctx, geom, ymax))) {
    return 0;
  }

  return retval;
}

/* Create a Polygon from bounding coordinates.
 *
 * Must be called from within a GEOS_INIT_THREADS / GEOS_FINISH_THREADS
 * or GEOS_INIT / GEOS_FINISH block.
 *
 * Parameters
 * ----------
 * ctx: GEOS context handle
 * xmin: minimum X value
 * ymin: minimum Y value
 * xmax: maximum X value
 * ymax: maximum Y value
 * ccw: if 1, box will be created in counterclockwise direction from bottom right;
 *  otherwise will be created in clockwise direction from bottom left.
 *
 * Returns
 * -------
 * GEOSGeometry* on success (owned by caller) or
 * NULL on failure or NPY_NAN coordinates
 */
GEOSGeometry* create_box(GEOSContextHandle_t ctx, double xmin, double ymin, double xmax,
                         double ymax, char ccw) {
  if (npy_isnan(xmin) || npy_isnan(ymin) || npy_isnan(xmax) || npy_isnan(ymax)) {
    return NULL;
  }

  GEOSCoordSequence* coords = NULL;
  GEOSGeometry* geom = NULL;
  GEOSGeometry* ring = NULL;

  // Construct coordinate sequence and set vertices
  coords = GEOSCoordSeq_create_r(ctx, 5, 2);
  if (coords == NULL) {
    return NULL;
  }

  if (ccw) {
    // Start from bottom right (xmax, ymin) to match shapely
    if (!(GEOSCoordSeq_setX_r(ctx, coords, 0, xmax) &&
          GEOSCoordSeq_setY_r(ctx, coords, 0, ymin) &&
          GEOSCoordSeq_setX_r(ctx, coords, 1, xmax) &&
          GEOSCoordSeq_setY_r(ctx, coords, 1, ymax) &&
          GEOSCoordSeq_setX_r(ctx, coords, 2, xmin) &&
          GEOSCoordSeq_setY_r(ctx, coords, 2, ymax) &&
          GEOSCoordSeq_setX_r(ctx, coords, 3, xmin) &&
          GEOSCoordSeq_setY_r(ctx, coords, 3, ymin) &&
          GEOSCoordSeq_setX_r(ctx, coords, 4, xmax) &&
          GEOSCoordSeq_setY_r(ctx, coords, 4, ymin))) {
      if (coords != NULL) {
        GEOSCoordSeq_destroy_r(ctx, coords);
      }

      return NULL;
    }
  } else {
    // Start from bottom left (min, ymin) to match shapely
    if (!(GEOSCoordSeq_setX_r(ctx, coords, 0, xmin) &&
          GEOSCoordSeq_setY_r(ctx, coords, 0, ymin) &&
          GEOSCoordSeq_setX_r(ctx, coords, 1, xmin) &&
          GEOSCoordSeq_setY_r(ctx, coords, 1, ymax) &&
          GEOSCoordSeq_setX_r(ctx, coords, 2, xmax) &&
          GEOSCoordSeq_setY_r(ctx, coords, 2, ymax) &&
          GEOSCoordSeq_setX_r(ctx, coords, 3, xmax) &&
          GEOSCoordSeq_setY_r(ctx, coords, 3, ymin) &&
          GEOSCoordSeq_setX_r(ctx, coords, 4, xmin) &&
          GEOSCoordSeq_setY_r(ctx, coords, 4, ymin))) {
      if (coords != NULL) {
        GEOSCoordSeq_destroy_r(ctx, coords);
      }

      return NULL;
    }
  }

  // Construct linear ring then use to construct polygon
  // Note: coords are owned by ring; if ring fails to construct, it will
  // automatically clean up the coords
  ring = GEOSGeom_createLinearRing_r(ctx, coords);
  if (ring == NULL) {
    return NULL;
  }

  // Note: ring is owned by polygon; if polygon fails to construct, it will
  // automatically clean up the ring
  geom = GEOSGeom_createPolygon_r(ctx, ring, NULL, 0);
  if (geom == NULL) {
    return NULL;
  }

  return geom;
}

/* Create a 3D empty Point
 *
 * Works around a limitation of the GEOS < 3.8 C API by constructing the point
 * from its WKT representation (POINT Z EMPTY).
 * (the limitation is that we can't have length-0 coord seqs)
 *
 * Returns
 * -------
 * GEOSGeometry* on success (owned by caller) or NULL on failure
 */
GEOSGeometry* PyGEOS_create3DEmptyPoint(GEOSContextHandle_t ctx) {
  GEOSGeometry* geom;
  GEOSCoordSequence* coord_seq = GEOSCoordSeq_create_r(ctx, 0, 3);
  if (coord_seq == NULL) {
    return NULL;
  }
  geom = GEOSGeom_createPoint_r(ctx, coord_seq);
  return geom;
}

/* Create a non-empty Point from x, y and optionally z coordinates.
 *
 * Parameters
 * ----------
 * ctx: GEOS context handle
 * x: X value
 * y: Y value
 * z: Z value pointer (point will be 2D if this is NULL)
 *
 * Returns
 * -------
 * GEOSGeometry* on success (owned by caller) or NULL on failure
 */
GEOSGeometry* PyGEOS_createPoint(GEOSContextHandle_t ctx, double x, double y, double* z) {
  if (z == NULL) {
    // There is no 3D equivalent for GEOSGeom_createPointFromXY_r
    // instead, it is constructed from a coord seq.
    return GEOSGeom_createPointFromXY_r(ctx, x, y);
  }

  // Fallback point construction (3D)
  GEOSCoordSequence* coord_seq = NULL;

  coord_seq = GEOSCoordSeq_create_r(ctx, 1, z == NULL ? 2 : 3);
  if (coord_seq == NULL) {
    return NULL;
  }
  if (!GEOSCoordSeq_setX_r(ctx, coord_seq, 0, x)) {
    GEOSCoordSeq_destroy_r(ctx, coord_seq);
    return NULL;
  }
  if (!GEOSCoordSeq_setY_r(ctx, coord_seq, 0, y)) {
    GEOSCoordSeq_destroy_r(ctx, coord_seq);
    return NULL;
  }

  if (z != NULL) {
    if (!GEOSCoordSeq_setZ_r(ctx, coord_seq, 0, *z)) {
      GEOSCoordSeq_destroy_r(ctx, coord_seq);
      return NULL;
    }
  }
  // Note: coordinate sequence is owned by point; if point fails to construct, it will
  // automatically clean up the coordinate sequence
  return GEOSGeom_createPoint_r(ctx, coord_seq);
}

/* Create a Point from x, y and optionally z coordinates, optionally handling NaN.
 *
 * Parameters
 * ----------
 * ctx: GEOS context handle
 * x: X value
 * y: Y value
 * z: Z value pointer (point will be 2D if this is NULL)
 * handle_nan: 0 means 'allow', 1 means 'skip', 2 means 'error'
 * out: pointer to the resulting point geometry
 *
 * Returns
 * -------
 * Returns an error state (PGERR_SUCCESS / PGERR_GEOS_EXCEPTION / PGERR_NAN_COORD)
 */
enum ShapelyErrorCode create_point(GEOSContextHandle_t ctx, double x, double y, double* z,
                                   int handle_nan, GEOSGeometry** out) {
  if (handle_nan != SHAPELY_HANDLE_NAN_ALLOW) {
    // Check here whether any of (x, y, z) is nonfinite
    if ((!npy_isfinite(x)) || (!npy_isfinite(y)) || (z != NULL ? !npy_isfinite(*z) : 0)) {
      if (handle_nan == SHAPELY_HANDLE_NAN_SKIP) {
        // Construct an empty point
        if (z == NULL) {
          *out = GEOSGeom_createEmptyPoint_r(ctx);
        } else {
          *out = PyGEOS_create3DEmptyPoint(ctx);
        }
        return (*out != NULL) ? PGERR_SUCCESS : PGERR_GEOS_EXCEPTION;
      } else {
        // Raise exception
        return PGERR_NAN_COORD;
      }
    }
  }
  *out = PyGEOS_createPoint(ctx, x, y, z);
  return (*out != NULL) ? PGERR_SUCCESS : PGERR_GEOS_EXCEPTION;
}

/* Force the coordinate dimensionality (2D / 3D) of any geometry
 *
 * Parameters
 * ----------
 * ctx: GEOS context handle
 * geom: geometry
 * dims: dimensions to force (2 or 3)
 * z: Z coordinate (ignored if dims==2)
 *
 * Returns
 * -------
 * GEOSGeometry* on success (owned by caller) or NULL on failure
 */
GEOSGeometry* force_dims(GEOSContextHandle_t, GEOSGeometry*, unsigned int, double);

GEOSGeometry* force_dims_simple(GEOSContextHandle_t ctx, GEOSGeometry* geom, int type,
                                unsigned int dims, double z) {
  unsigned int actual_dims, n, i, j;
  double coord;
  const GEOSCoordSequence* seq = GEOSGeom_getCoordSeq_r(ctx, geom);

  /* Investigate the coordinate sequence, return when already of correct dimensionality */
  if (GEOSCoordSeq_getDimensions_r(ctx, seq, &actual_dims) == 0) {
    return NULL;
  }
  if (actual_dims == dims) {
    return GEOSGeom_clone_r(ctx, geom);
  }
  if (GEOSCoordSeq_getSize_r(ctx, seq, &n) == 0) {
    return NULL;
  }

  /* Create a new one to fill with the new coordinates */
  GEOSCoordSequence* seq_new = GEOSCoordSeq_create_r(ctx, n, dims);
  if (seq_new == NULL) {
    return NULL;
  }

  for (i = 0; i < n; i++) {
    for (j = 0; j < 2; j++) {
      if (!GEOSCoordSeq_getOrdinate_r(ctx, seq, i, j, &coord)) {
        GEOSCoordSeq_destroy_r(ctx, seq_new);
        return NULL;
      }
      if (!GEOSCoordSeq_setOrdinate_r(ctx, seq_new, i, j, coord)) {
        GEOSCoordSeq_destroy_r(ctx, seq_new);
        return NULL;
      }
    }
    if (dims == 3) {
      if (!GEOSCoordSeq_setZ_r(ctx, seq_new, i, z)) {
        GEOSCoordSeq_destroy_r(ctx, seq_new);
        return NULL;
      }
    }
  }

  /* Construct a new geometry */
  if (type == 0) {
    return GEOSGeom_createPoint_r(ctx, seq_new);
  } else if (type == 1) {
    return GEOSGeom_createLineString_r(ctx, seq_new);
  } else if (type == 2) {
    return GEOSGeom_createLinearRing_r(ctx, seq_new);
  } else {
    return NULL;
  }
}

GEOSGeometry* force_dims_polygon(GEOSContextHandle_t ctx, GEOSGeometry* geom,
                                 unsigned int dims, double z) {
  int i, n;
  const GEOSGeometry *shell, *hole;
  GEOSGeometry *new_shell, *new_hole, *result = NULL;
  GEOSGeometry** new_holes;

  n = GEOSGetNumInteriorRings_r(ctx, geom);
  if (n == -1) {
    return NULL;
  }

  /* create the exterior ring */
  shell = GEOSGetExteriorRing_r(ctx, geom);
  if (shell == NULL) {
    return NULL;
  }
  new_shell = force_dims_simple(ctx, (GEOSGeometry*)shell, 2, dims, z);
  if (new_shell == NULL) {
    return NULL;
  }

  new_holes = malloc(sizeof(void*) * n);
  if (new_holes == NULL) {
    GEOSGeom_destroy_r(ctx, new_shell);
    return NULL;
  }

  for (i = 0; i < n; i++) {
    hole = GEOSGetInteriorRingN_r(ctx, geom, i);
    if (hole == NULL) {
      GEOSGeom_destroy_r(ctx, new_shell);
      destroy_geom_arr(ctx, new_holes, i - 1);
      goto finish;
    }
    new_hole = force_dims_simple(ctx, (GEOSGeometry*)hole, 2, dims, z);
    if (hole == NULL) {
      GEOSGeom_destroy_r(ctx, new_shell);
      destroy_geom_arr(ctx, new_holes, i - 1);
      goto finish;
    }
    new_holes[i] = new_hole;
  }

  result = GEOSGeom_createPolygon_r(ctx, new_shell, new_holes, n);

finish:
  if (new_holes != NULL) {
    free(new_holes);
  }
  return result;
}

GEOSGeometry* force_dims_collection(GEOSContextHandle_t ctx, GEOSGeometry* geom, int type,
                                    unsigned int dims, double z) {
  int i, n;
  const GEOSGeometry* sub_geom;
  GEOSGeometry *new_sub_geom, *result = NULL;
  GEOSGeometry** geoms;

  n = GEOSGetNumGeometries_r(ctx, geom);
  if (n == -1) {
    return NULL;
  }

  geoms = malloc(sizeof(void*) * n);
  if (geoms == NULL) {
    return NULL;
  }

  for (i = 0; i < n; i++) {
    sub_geom = GEOSGetGeometryN_r(ctx, geom, i);
    if (sub_geom == NULL) {
      destroy_geom_arr(ctx, geoms, i - i);
      goto finish;
    }
    new_sub_geom = force_dims(ctx, (GEOSGeometry*)sub_geom, dims, z);
    if (new_sub_geom == NULL) {
      destroy_geom_arr(ctx, geoms, i - i);
      goto finish;
    }
    geoms[i] = new_sub_geom;
  }

  result = GEOSGeom_createCollection_r(ctx, type, geoms, n);
finish:
  if (geoms != NULL) {
    free(geoms);
  }
  return result;
}

GEOSGeometry* force_dims(GEOSContextHandle_t ctx, GEOSGeometry* geom, unsigned int dims,
                         double z) {
  int type = GEOSGeomTypeId_r(ctx, geom);
  if ((type == 0) || (type == 1) || (type == 2)) {
    return force_dims_simple(ctx, geom, type, dims, z);
  } else if (type == 3) {
    return force_dims_polygon(ctx, geom, dims, z);
  } else if ((type >= 4) && (type <= 7)) {
    return force_dims_collection(ctx, geom, type, dims, z);
  } else {
    return NULL;
  }
}

GEOSGeometry* PyGEOSForce2D(GEOSContextHandle_t ctx, GEOSGeometry* geom) {
  return force_dims(ctx, geom, 2, 0.0);
}

GEOSGeometry* PyGEOSForce3D(GEOSContextHandle_t ctx, GEOSGeometry* geom, double z) {
  return force_dims(ctx, geom, 3, z);
}

/* Count the number of finite coordinates in a buffer
 *
 * A coordinate is finite if x, y and optionally z are all not NaN or Inf.
 *
 * The first and last finite coordinate indices are stored in the 'first_i'
 * and 'last_i' arguments.
 */
unsigned int count_finite(const double* buf, unsigned int size, unsigned int dims,
                          npy_intp cs1, npy_intp cs2, unsigned int* first_i,
                          unsigned int* last_i) {
  char *cp1, *cp2;
  char this_coord_is_finite;
  unsigned int i, j, actual_size = 0;

  *first_i = size;
  *last_i = size;
  cp1 = (char*)buf;
  for (i = 0; i < size; i++, cp1 += cs1) {
    cp2 = cp1;
    this_coord_is_finite = 1;
    for (j = 0; j < dims; j++, cp2 += cs2) {
      if (!(npy_isfinite(*(double*)cp2))) {
        this_coord_is_finite = 0;
        break;
      }
    }
    if (this_coord_is_finite) {
      actual_size++;
      if (*first_i == size) {
        *first_i = i;
      }
      *last_i = i;
    }
  }
  return actual_size;
}

char check_coordinates_equal(const double* buf, unsigned int dims, npy_intp cs1,
                             npy_intp cs2, unsigned int first_i, unsigned int last_i) {
  unsigned int j;
  char* cp_first = (char*)buf + cs1 * first_i;
  char* cp_last = (char*)buf + cs1 * last_i;

  /* Compare the coordinates */
  for (j = 0; j < dims; j++, cp_first += cs2, cp_last += cs2) {
    if (*(double*)cp_first != *(double*)cp_last) {
      return 0;
    }
  }

  return 1;
}

char fill_coord_seq(GEOSContextHandle_t ctx, GEOSCoordSequence* coord_seq,
                    const double* buf, unsigned int size, unsigned int dims, npy_intp cs1,
                    npy_intp cs2) {
  unsigned int i, j = 0;
  char *cp1, *cp2;

  cp1 = (char*)buf;
  for (i = 0; i < size; i++, cp1 += cs1) {
    cp2 = cp1;
    for (j = 0; j < dims; j++, cp2 += cs2) {
      if (!GEOSCoordSeq_setOrdinate_r(ctx, coord_seq, i, j, *(double*)cp2)) {
        return PGERR_GEOS_EXCEPTION;
      }
    }
  }
  return PGERR_SUCCESS;
}

char fill_coord_seq_skip_nan(GEOSContextHandle_t ctx, GEOSCoordSequence* coord_seq,
                             const double* buf, unsigned int dims, npy_intp cs1,
                             npy_intp cs2, unsigned int first_i, unsigned int last_i) {
  unsigned int i, j, current = 0;
  char *cp1, *cp2;
  char this_coord_is_finite;
  double coord;

  cp1 = (char*)buf + cs1 * first_i;
  for (i = first_i; i <= last_i; i++, cp1 += cs1) {
    cp2 = cp1;
    this_coord_is_finite = 1;
    for (j = 0; j < dims; j++, cp2 += cs2) {
      coord = *(double*)cp2;
      if (!npy_isfinite(coord)) {
        this_coord_is_finite = 0;
        break;
      }
      if (!GEOSCoordSeq_setOrdinate_r(ctx, coord_seq, current, j, coord)) {
        return PGERR_GEOS_EXCEPTION;
      }
    }
    if (this_coord_is_finite) {
      current++;
    }
  }
  return PGERR_SUCCESS;
}

/* Create a GEOSCoordSequence from an array
 *
 * Note: this function assumes that the dimension of the buffer is already
 * checked before calling this function, so the buffer and the dims argument
 * is only 2D or 3D.
 *
 * handle_nan: 0 means 'allow', 1 means 'skip', 2 means 'error'
 *
 * Returns an error state (PGERR_SUCCESS / PGERR_GEOS_EXCEPTION / PGERR_NAN_COORD).
 */
enum ShapelyErrorCode coordseq_from_buffer(GEOSContextHandle_t ctx, const double* buf,
                                           unsigned int size, unsigned int dims,
                                           char is_ring, int handle_nan, npy_intp cs1,
                                           npy_intp cs2, GEOSCoordSequence** coord_seq) {
  unsigned int first_i, last_i, actual_size;
  double coord;
  char errstate;
  char ring_closure = 0;

  switch (handle_nan) {
    case SHAPELY_HANDLE_NAN_ALLOW:
      actual_size = size;
      first_i = 0;
      last_i = size - 1;
      break;
    case SHAPELY_HANDLE_NAN_SKIP:
      actual_size = count_finite(buf, size, dims, cs1, cs2, &first_i, &last_i);
      break;
    case SHAPELY_HANDLE_NANS_ERROR:
      actual_size = count_finite(buf, size, dims, cs1, cs2, &first_i, &last_i);
      if (actual_size != size) {
        return PGERR_NAN_COORD;
      }
      break;
    default:
      return PGERR_NAN_COORD;
  }

  if (actual_size == 0) {
    *coord_seq = GEOSCoordSeq_create_r(ctx, 0, dims);
    return (*coord_seq != NULL) ? PGERR_SUCCESS : PGERR_GEOS_EXCEPTION;
  }

  /* Rings automatically get an extra (closing) coordinate if they have
     only 3 or if the first and last are not equal. */
  if (is_ring) {
    if (actual_size == 3) {
      ring_closure = 1;
    } else {
      ring_closure = !check_coordinates_equal(buf, dims, cs1, cs2, first_i, last_i);
    }
  }

#if GEOS_SINCE_3_10_0
  if ((!ring_closure) && ((last_i - first_i + 1) == actual_size)) {
    /* Initialize cp1 so that it points to the first coordinate (possibly skipping NaN)*/
    char* cp1 = (char*)buf + cs1 * first_i;
    if ((cs1 == dims * 8) && (cs2 == 8)) {
      /* C-contiguous memory */
      int hasZ = dims == 3;
      *coord_seq = GEOSCoordSeq_copyFromBuffer_r(ctx, (double*)cp1, actual_size, hasZ, 0);
      return (*coord_seq != NULL) ? PGERR_SUCCESS : PGERR_GEOS_EXCEPTION;
    } else if ((cs1 == 8) && (cs2 == size * 8)) {
      /* F-contiguous memory (note: this for the subset, so we don't necessarily
      end up here if the full array is F-contiguous) */
      const double* x = (double*)cp1;
      const double* y = (double*)(cp1 + cs2);
      const double* z = (dims == 3) ? (double*)(cp1 + 2 * cs2) : NULL;
      *coord_seq = GEOSCoordSeq_copyFromArrays_r(ctx, x, y, z, NULL, actual_size);
      return (*coord_seq != NULL) ? PGERR_SUCCESS : PGERR_GEOS_EXCEPTION;
    }
  }

#endif

  *coord_seq = GEOSCoordSeq_create_r(ctx, actual_size + ring_closure, dims);
  if (*coord_seq == NULL) {
    return PGERR_GEOS_EXCEPTION;
  }
  if (handle_nan == SHAPELY_HANDLE_NAN_SKIP) {
    errstate =
        fill_coord_seq_skip_nan(ctx, *coord_seq, buf, dims, cs1, cs2, first_i, last_i);
  } else {
    errstate = fill_coord_seq(ctx, *coord_seq, buf, size, dims, cs1, cs2);
  }
  if (errstate != PGERR_SUCCESS) {
    GEOSCoordSeq_destroy_r(ctx, *coord_seq);
    return errstate;
  }
  /* add the closing coordinate if necessary */
  if (ring_closure) {
    for (unsigned int j = 0; j < dims; j++) {
      coord = *(double*)((char*)buf + first_i * cs1 + j * cs2);
      if (!GEOSCoordSeq_setOrdinate_r(ctx, *coord_seq, actual_size, j, coord)) {
        GEOSCoordSeq_destroy_r(ctx, *coord_seq);
        return PGERR_GEOS_EXCEPTION;
      }
    }
  }
  return PGERR_SUCCESS;
}

/* Copy coordinates of a GEOSCoordSequence to an array
 *
 * Note: this function assumes that the buffer is from a C-contiguous array,
 * and that the dimension of the buffer can be 2, 3 or 4.
 *
 * Returns 0 on error, 1 on success.
 */
int coordseq_to_buffer(GEOSContextHandle_t ctx, const GEOSCoordSequence* coord_seq,
                       double* buf, unsigned int size, int has_z, int has_m) {

#if GEOS_SINCE_3_10_0

  return GEOSCoordSeq_copyToBuffer_r(ctx, coord_seq, buf, has_z, has_m);

#else

  char *cp1, *cp2;
  unsigned int i, j;
  unsigned int dims = 2 + has_z + has_m;

  cp1 = (char*)buf;
  for (i = 0; i < size; i++, cp1 += 8 * dims) {
    cp2 = cp1;
    for (j = 0; j < dims; j++, cp2 += 8) {
      if (j == 2 && !has_z && has_m) j = 3;  /* jump to last ordinate, fill with Nan */
      if (!GEOSCoordSeq_getOrdinate_r(ctx, coord_seq, i, j, (double*)cp2)) {
        return 0;
      }
    }
  }
  return 1;

#endif
}
