#ifndef _GEOS_H
#define _GEOS_H

#include <Python.h>
#include <numpy/ndarraytypes.h>

/* To avoid accidental use of non reentrant GEOS API. */
#ifndef GEOS_USE_ONLY_R_API
#define GEOS_USE_ONLY_R_API
#endif

// wrap geos.h import to silence geos gcc warnings
#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-prototypes"
#endif

#include <geos_c.h>

#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

/* Macros to setup GEOS Context and error handlers

Typical PyGEOS pattern in a function that uses GEOS:


// GEOS_INIT will do three things:
// 1. Make the GEOS context available in the variable ``ctx``
// 2. Initialize a variable ``errstate`` to PGERR_SUCCESS.
// 3. Set up GEOS error and warning buffers, respectively ``last_error`` and
``last_warning``

GEOS_INIT;   // or GEOS_INIT_THREADS if you use no CPython calls


// call a GEOS function using the context 'ctx'
result = SomeGEOSFunc(ctx, ...);

// handle an error state
if (result == NULL) { errstate = PGERR_GEOS_EXCEPTION; goto finish; }


// GEOS_FINISH will remove the GEOS context and set python errors in case
// errstate != PGERR_SUCCESS.

finish:
  GEOS_FINISH;  //  or GEOS_FINISH_THREADS if you use no CPython calls

*/

// Define the error states
enum ShapelyErrorCode {
  PGERR_SUCCESS,
  PGERR_NOT_A_GEOMETRY,
  PGERR_GEOS_EXCEPTION,
  PGERR_NO_MALLOC,
  PGERR_GEOMETRY_TYPE,
  PGERR_MULTIPOINT_WITH_POINT_EMPTY,
  PGERR_COORD_OUT_OF_BOUNDS,
  PGERR_EMPTY_GEOMETRY,
  PGERR_GEOJSON_EMPTY_POINT,
  PGERR_LINEARRING_NCOORDS,
  PGERR_NAN_COORD,
  PGWARN_INVALID_WKB,  // raise the GEOS WKB error as a warning instead of exception
  PGWARN_INVALID_WKT,  // raise the GEOS WKT error as a warning instead of exception
  PGWARN_INVALID_GEOJSON,
  PGERR_PYSIGNAL
};

// Define how the states are handled by CPython
#define GEOS_HANDLE_ERR                                                                  \
  if (last_warning[0] != 0) {                                                            \
    PyErr_WarnEx(PyExc_Warning, last_warning, 0);                                        \
  }                                                                                      \
  switch (errstate) {                                                                    \
    case PGERR_SUCCESS:                                                                  \
      break;                                                                             \
    case PGERR_NOT_A_GEOMETRY:                                                           \
      PyErr_SetString(PyExc_TypeError,                                                   \
                      "One of the arguments is of incorrect type. Please provide only "  \
                      "Geometry objects.");                                              \
      break;                                                                             \
    case PGERR_GEOS_EXCEPTION:                                                           \
      PyErr_SetString(geos_exception[0], last_error);                                    \
      break;                                                                             \
    case PGERR_NO_MALLOC:                                                                \
      PyErr_SetString(PyExc_MemoryError, "Could not allocate memory");                   \
      break;                                                                             \
    case PGERR_GEOMETRY_TYPE:                                                            \
      PyErr_SetString(PyExc_TypeError,                                                   \
                      "One of the Geometry inputs is of incorrect geometry type.");      \
      break;                                                                             \
    case PGERR_MULTIPOINT_WITH_POINT_EMPTY:                                              \
      PyErr_SetString(PyExc_ValueError,                                                  \
                      "WKT output of multipoints with an empty point is unsupported on " \
                      "this version of GEOS.");                                          \
      break;                                                                             \
    case PGERR_COORD_OUT_OF_BOUNDS:  /* applies to GEOS <3.13.0 with trim enabled  */    \
      PyErr_SetString(PyExc_ValueError,                                                  \
                      "WKT output of coordinates greater than 1E+100 is unsupported on " \
                      "this version of GEOS.");                                          \
      break;                                                                             \
    case PGERR_EMPTY_GEOMETRY:                                                           \
      PyErr_SetString(PyExc_ValueError, "One of the Geometry inputs is empty.");         \
      break;                                                                             \
    case PGERR_GEOJSON_EMPTY_POINT:                                                      \
      PyErr_SetString(PyExc_ValueError,                                                  \
                      "GeoJSON output of empty points is currently unsupported.");       \
      break;                                                                             \
    case PGERR_LINEARRING_NCOORDS:                                                       \
      PyErr_SetString(PyExc_ValueError,                                                  \
                      "A linearring requires at least 4 coordinates.");                  \
      break;                                                                             \
    case PGERR_NAN_COORD:                                                                \
      PyErr_SetString(PyExc_ValueError,                                                  \
                      "A NaN, Inf or -Inf coordinate was supplied. Remove the "          \
                      "coordinate or adapt the 'handle_nan' parameter.");               \
      break;                                                                             \
    case PGWARN_INVALID_WKB:                                                             \
      PyErr_WarnFormat(PyExc_Warning, 0,                                                 \
                       "Invalid WKB: geometry is returned as None. %s", last_error);     \
      break;                                                                             \
    case PGWARN_INVALID_WKT:                                                             \
      PyErr_WarnFormat(PyExc_Warning, 0,                                                 \
                       "Invalid WKT: geometry is returned as None. %s", last_error);     \
      break;                                                                             \
    case PGWARN_INVALID_GEOJSON:                                                         \
      PyErr_WarnFormat(PyExc_Warning, 0,                                                 \
                       "Invalid GeoJSON: geometry is returned as None. %s", last_error); \
      break;                                                                             \
    case PGERR_PYSIGNAL:                                                                 \
      break;                                                                             \
    default:                                                                             \
      PyErr_Format(PyExc_RuntimeError,                                                   \
                   "Pygeos ufunc returned with unknown error state code %d.", errstate); \
      break;                                                                             \
  }

// Define initialization / finalization macros
#define _GEOS_INIT_DEF           \
  char errstate = PGERR_SUCCESS; \
  char last_error[1024] = "";    \
  char last_warning[1024] = "";  \
  GEOSContextHandle_t ctx

#define _GEOS_INIT     \
  ctx = GEOS_init_r(); \
  GEOSContext_setErrorMessageHandler_r(ctx, geos_error_handler, last_error)

#define GEOS_INIT \
  _GEOS_INIT_DEF; \
  _GEOS_INIT

#define GEOS_INIT_THREADS \
  _GEOS_INIT_DEF;         \
  Py_BEGIN_ALLOW_THREADS _GEOS_INIT

#define GEOS_FINISH   \
  GEOS_finish_r(ctx); \
  GEOS_HANDLE_ERR

#define GEOS_FINISH_THREADS \
  GEOS_finish_r(ctx);       \
  Py_END_ALLOW_THREADS GEOS_HANDLE_ERR

#define GEOS_SINCE_3_10_0 ((GEOS_VERSION_MAJOR >= 3) && (GEOS_VERSION_MINOR >= 10))
#define GEOS_SINCE_3_11_0 ((GEOS_VERSION_MAJOR >= 3) && (GEOS_VERSION_MINOR >= 11))
#define GEOS_SINCE_3_12_0 ((GEOS_VERSION_MAJOR >= 3) && (GEOS_VERSION_MINOR >= 12))
#define GEOS_SINCE_3_13_0 ((GEOS_VERSION_MAJOR >= 3) && (GEOS_VERSION_MINOR >= 13))

extern void* geos_context[1];
extern PyObject* geos_exception[1];

extern void geos_error_handler(const char* message, void* userdata);
extern void destroy_geom_arr(void* context, GEOSGeometry** array, int length);
extern char has_point_empty(GEOSContextHandle_t ctx, GEOSGeometry* geom);
extern GEOSGeometry* point_empty_to_nan_all_geoms(GEOSContextHandle_t ctx,
                                                  GEOSGeometry* geom);
#if !GEOS_SINCE_3_13_0
extern char check_to_wkt_trim_compatible(GEOSContextHandle_t ctx, const GEOSGeometry* geom, int dimension);
#endif  // !GEOS_SINCE_3_13_0
#if !GEOS_SINCE_3_12_0
extern char wkt_empty_3d_geometry(GEOSContextHandle_t ctx, GEOSGeometry* geom,
                                  char** wkt);
#endif  // !GEOS_SINCE_3_12_0
extern char geos_interpolate_checker(GEOSContextHandle_t ctx, GEOSGeometry* geom);

extern int init_geos(PyObject* m);

int get_bounds(GEOSContextHandle_t ctx, GEOSGeometry* geom, double* xmin, double* ymin,
               double* xmax, double* ymax);
GEOSGeometry* create_box(GEOSContextHandle_t ctx, double xmin, double ymin, double xmax,
                         double ymax, char ccw);
extern enum ShapelyErrorCode create_point(GEOSContextHandle_t ctx, double x, double y,
                                          double* z, int handle_nan, GEOSGeometry** out);
GEOSGeometry* PyGEOSForce2D(GEOSContextHandle_t ctx, GEOSGeometry* geom);
GEOSGeometry* PyGEOSForce3D(GEOSContextHandle_t ctx, GEOSGeometry* geom, double z);

enum ShapelyHandleNan { SHAPELY_HANDLE_NAN_ALLOW, SHAPELY_HANDLE_NAN_SKIP, SHAPELY_HANDLE_NANS_ERROR };

extern enum ShapelyErrorCode coordseq_from_buffer(GEOSContextHandle_t ctx,
                                                  const double* buf, unsigned int size,
                                                  unsigned int dims, char is_ring,
                                                  int handle_nan, npy_intp cs1,
                                                  npy_intp cs2,
                                GEOSCoordSequence** coord_seq);
extern int coordseq_to_buffer(GEOSContextHandle_t ctx, const GEOSCoordSequence* coord_seq,
                              double* buf, unsigned int size, int has_z, int has_m);

#endif  // _GEOS_H
