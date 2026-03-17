#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <structmember.h>

#define NO_IMPORT_ARRAY
#define PY_ARRAY_UNIQUE_SYMBOL shapely_ARRAY_API

#include <numpy/arrayobject.h>
#include <numpy/ndarraytypes.h>
#include <numpy/npy_3kcompat.h>

#include "kvec.h"
#include "vector.h"

/* Copy values from arr of indexes to a new numpy integer array.
 *
 * Parameters
 * ----------
 * arr: dynamic vector array to convert to ndarray
 */

PyArrayObject* index_vec_to_npy_arr(index_vec_t* arr) {
  Py_ssize_t i;
  npy_intp size = kv_size(*arr);

  npy_intp dims[1] = {size};
  // the following raises a compiler warning based on how the macro is defined
  // in numpy.  There doesn't appear to be anything we can do to avoid it.
  PyArrayObject* result = (PyArrayObject*)PyArray_SimpleNew(1, dims, NPY_INTP);
  if (result == NULL) {
    PyErr_SetString(PyExc_RuntimeError, "could not allocate numpy array");
    return NULL;
  }

  for (i = 0; i < size; i++) {
    // assign value into numpy array
    *(npy_intp*)PyArray_GETPTR1(result, i) = kv_A(*arr, i);
  }

  return (PyArrayObject*)result;
}
