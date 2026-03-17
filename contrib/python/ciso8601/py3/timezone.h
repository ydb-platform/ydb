#ifndef CISO_TZINFO_H
#define CISO_TZINFO_H

#include <Python.h>

PyObject *
new_fixed_offset(int offset);

int
initialize_timezone_code(PyObject *module);

#endif
