#pragma once

#include "Python.h"

#ifdef __cplusplus
extern "C" {
#endif

// Declare functions which are to be backported
// (see details about need for backports in ya.make)

int _PySlice_Unpack(PyObject *slice,
                    Py_ssize_t *start, Py_ssize_t *stop, Py_ssize_t *step);

Py_ssize_t _PySlice_AdjustIndices(Py_ssize_t length,
                                  Py_ssize_t *start, Py_ssize_t *stop,
                                  Py_ssize_t step);

// Declare py23 compatible names

#define PySlice_Unpack _PySlice_Unpack
#define PySlice_AdjustIndices _PySlice_AdjustIndices

#ifdef __cplusplus
}
#endif
