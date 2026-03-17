/************************************************************************
 * PyGEOS C API
 *
 * This file wraps internal PyGEOS C extension functions for use in other
 * extensions.  These are specifically wrapped to enable dynamic loading
 * after Python initialization (see c_api.h and lib.c).
 *
 ***********************************************************************/
#define PY_SSIZE_T_CLEAN

#include <Python.h>
#define PyGEOS_API_Module

#include "c_api.h"
#include "geos.h"
#include "pygeom.h"

extern PyObject* PyGEOS_CreateGeometry(GEOSGeometry* ptr, GEOSContextHandle_t ctx) {
  return GeometryObject_FromGEOS(ptr, ctx);
}

extern char PyGEOS_GetGEOSGeometry(PyObject* obj, GEOSGeometry** out) {
  return get_geom((GeometryObject*)obj, out);
}

extern int PyGEOS_CoordSeq_FromBuffer(GEOSContextHandle_t ctx, const double* buf,
                                      unsigned int size, unsigned int dims, char is_ring,
                                      int handle_nan, GEOSCoordSequence** coord_seq) {
  return coordseq_from_buffer(ctx, buf, size, dims, is_ring, handle_nan, dims * 8, 8,
                              coord_seq);
}
