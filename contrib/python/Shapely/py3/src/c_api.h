/************************************************************************
 * PyGEOS C API
 *
 * This file wraps internal PyGEOS C extension functions for use in other
 * extensions.  These are specifically wrapped to enable dynamic loading
 * after Python initialization.
 *
 * Each function must provide 3 defines for use in the dynamic loading process:
 * NUM: the index in function pointer array
 * RETURN: the return type
 * PROTO: function prototype
 *
 * IMPORTANT: each function must provide 2 sets of defines below and
 * provide an entry into PyGEOS_API in lib.c module declaration block.
 *
 ***********************************************************************/

#ifndef _PYGEOS_API_H
#define _PYGEOS_API_H

#include <Python.h>
#include "geos.h"
#include "pygeom.h"

/* PyObject* PyGEOS_CreateGeometry(GEOSGeometry *ptr, GEOSContextHandle_t ctx) */
#define PyGEOS_CreateGeometry_NUM 0
#define PyGEOS_CreateGeometry_RETURN PyObject *
#define PyGEOS_CreateGeometry_PROTO (GEOSGeometry * ptr, GEOSContextHandle_t ctx)

/* char PyGEOS_GetGEOSGeometry(GeometryObject *obj, GEOSGeometry **out) */
#define PyGEOS_GetGEOSGeometry_NUM 1
#define PyGEOS_GetGEOSGeometry_RETURN char
#define PyGEOS_GetGEOSGeometry_PROTO (PyObject * obj, GEOSGeometry * *out)

/* extern int PyGEOS_CoordSeq_FromBuffer(GEOSContextHandle_t ctx, const double* buf,
                                      unsigned int size, unsigned int dims, char is_ring,
                                      int handle_nan, GEOSCoordSequence** coord_seq)*/
#define PyGEOS_CoordSeq_FromBuffer_NUM 2
#define PyGEOS_CoordSeq_FromBuffer_RETURN int
#define PyGEOS_CoordSeq_FromBuffer_PROTO                                             \
  (GEOSContextHandle_t ctx, const double* buf, unsigned int size, unsigned int dims, \
   char is_ring, int handle_nan, GEOSCoordSequence** coord_seq)

/* Total number of C API pointers */
#define PyGEOS_API_num_pointers 3

#ifdef PyGEOS_API_Module
/* This section is used when compiling shapely.lib C extension.
 * Each API function needs to provide a corresponding *_PROTO here.
 */

extern PyGEOS_CreateGeometry_RETURN PyGEOS_CreateGeometry PyGEOS_CreateGeometry_PROTO;
extern PyGEOS_GetGEOSGeometry_RETURN PyGEOS_GetGEOSGeometry PyGEOS_GetGEOSGeometry_PROTO;
extern PyGEOS_CoordSeq_FromBuffer_RETURN PyGEOS_CoordSeq_FromBuffer PyGEOS_CoordSeq_FromBuffer_PROTO;

#else
/* This section is used in modules that use the PyGEOS C API
 * Each API function needs to provide the lookup into PyGEOS_API as a
 * define statement.
 */

static void **PyGEOS_API;

#define PyGEOS_CreateGeometry \
    (*(PyGEOS_CreateGeometry_RETURN(*) PyGEOS_CreateGeometry_PROTO)PyGEOS_API[PyGEOS_CreateGeometry_NUM])

#define PyGEOS_GetGEOSGeometry \
    (*(PyGEOS_GetGEOSGeometry_RETURN(*) PyGEOS_GetGEOSGeometry_PROTO)PyGEOS_API[PyGEOS_GetGEOSGeometry_NUM])

#define PyGEOS_CoordSeq_FromBuffer \
    (*(PyGEOS_CoordSeq_FromBuffer_RETURN(*) PyGEOS_CoordSeq_FromBuffer_PROTO)PyGEOS_API[PyGEOS_CoordSeq_FromBuffer_NUM])

/* Dynamically load C API from PyCapsule.
 * This MUST be called prior to using C API functions in other modules; otherwise
 * segfaults will occur when the PyGEOS C API functions are called.
 *
 * Returns 0 on success, -1 if error.
 * PyCapsule_Import will set an exception on error.
 */
static int
import_shapely_c_api(void)
{
    PyGEOS_API = (void **)PyCapsule_Import("shapely.lib._C_API", 0);
  return (PyGEOS_API == NULL) ? -1 : 0;
}

#endif

#endif /* !defined(_PYGEOS_API_H) */
