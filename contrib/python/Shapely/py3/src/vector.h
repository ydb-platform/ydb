#ifndef _VECTOR_H
#define _VECTOR_H

#include <numpy/ndarraytypes.h>

#include "pygeom.h"

/* A resizable vector with numpy indices.
 * Wraps the vector implementation in kvec.h as a type.
 */
typedef struct {
  size_t n, m;
  npy_intp* a;
} index_vec_t;

/* Copy values from arr to a new numpy integer array.
 *
 * Parameters
 * ----------
 * arr: dynamic vector array to convert to ndarray
 */
extern PyArrayObject* index_vec_to_npy_arr(index_vec_t* arr);

#endif
