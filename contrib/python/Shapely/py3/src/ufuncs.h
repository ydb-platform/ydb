#ifndef _UFUNCS_H
#define _UFUNCS_H

#include <Python.h>

extern int init_ufuncs(PyObject* m, PyObject* d);
extern PyObject* PySetupSignalChecks(PyObject* self, PyObject* args);

#endif
