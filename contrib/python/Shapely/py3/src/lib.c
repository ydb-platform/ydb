#define PY_SSIZE_T_CLEAN

#include <Python.h>

#define PyGEOS_API_Module

#define PY_ARRAY_UNIQUE_SYMBOL shapely_ARRAY_API
#define PY_UFUNC_UNIQUE_SYMBOL shapely_UFUNC_API
#include <numpy/ndarraytypes.h>
#include <numpy/npy_3kcompat.h>
#include <numpy/ufuncobject.h>

#include "c_api.h"
#include "coords.h"
#include "geos.h"
#include "pygeom.h"
#include "strtree.h"
#include "ufuncs.h"

/* This tells Python what methods this module has. */
static PyMethodDef GeosModule[] = {
    {"count_coordinates", PyCountCoords, METH_VARARGS,
     "Counts the total amount of coordinates in a array with geometry objects"},
    {"get_coordinates", PyGetCoords, METH_VARARGS,
     "Gets the coordinates as an (N, 2), (N, 3), or (N, 4) shaped ndarray of floats"},
    {"set_coordinates", PySetCoords, METH_VARARGS,
     "Sets coordinates to a geometry array"},
    {"_setup_signal_checks", PySetupSignalChecks, METH_VARARGS,
     "Sets the thread id and interval for signal checks"},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "lib", NULL, -1, GeosModule, NULL, NULL, NULL, NULL};

PyMODINIT_FUNC PyInit_lib(void) {
  PyObject *m, *d;

  static void* PyGEOS_API[PyGEOS_API_num_pointers];
  PyObject* c_api_object;

  m = PyModule_Create(&moduledef);
  if (!m) {
    return NULL;
  }

  /* Work with freethreaded Python */
  #ifdef Py_GIL_DISABLED
    PyUnstable_Module_SetGIL(m, Py_MOD_GIL_NOT_USED);
  #endif

  if (init_geos(m) < 0) {
    return NULL;
  };

  if (init_geom_type(m) < 0) {
    return NULL;
  };

  if (init_strtree_type(m) < 0) {
    return NULL;
  };

  d = PyModule_GetDict(m);

  import_array();
  import_umath();

  /* GEOS_VERSION_PATCH may contain non-integer characters, e.g., 0beta1
     add quotes using https://gcc.gnu.org/onlinedocs/cpp/Stringizing.html
     then take the first digit */
#define Q(x) #x
#define QUOTE(x) Q(x)

#define GEOS_VERSION_PATCH_STR QUOTE(GEOS_VERSION_PATCH)
  int geos_version_patch_int = GEOS_VERSION_PATCH_STR[0] - '0';

  /* export the GEOS versions as python tuple and string */
  PyModule_AddObject(m, "geos_version",
                     PyTuple_Pack(3, PyLong_FromLong((long)GEOS_VERSION_MAJOR),
                                  PyLong_FromLong((long)GEOS_VERSION_MINOR),
                                  PyLong_FromLong((long)geos_version_patch_int)));
  PyModule_AddObject(m, "geos_capi_version",
                     PyTuple_Pack(3, PyLong_FromLong((long)GEOS_CAPI_VERSION_MAJOR),
                                  PyLong_FromLong((long)GEOS_CAPI_VERSION_MINOR),
                                  PyLong_FromLong((long)GEOS_CAPI_VERSION_PATCH)));

  PyModule_AddObject(m, "geos_version_string", PyUnicode_FromString(GEOS_VERSION));
  PyModule_AddObject(m, "geos_capi_version_string",
                     PyUnicode_FromString(GEOS_CAPI_VERSION));

  if (init_ufuncs(m, d) < 0) {
    return NULL;
  };

  /* Initialize the C API pointer array */
  PyGEOS_API[PyGEOS_CreateGeometry_NUM] = (void*)PyGEOS_CreateGeometry;
  PyGEOS_API[PyGEOS_GetGEOSGeometry_NUM] = (void*)PyGEOS_GetGEOSGeometry;
  PyGEOS_API[PyGEOS_CoordSeq_FromBuffer_NUM] = (void*)PyGEOS_CoordSeq_FromBuffer;

  /* Create a Capsule containing the API pointer array's address */
  c_api_object = PyCapsule_New((void*)PyGEOS_API, "shapely.lib._C_API", NULL);
  if (c_api_object != NULL) {
    PyModule_AddObject(m, "_C_API", c_api_object);
  }

  return m;
}
