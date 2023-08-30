#pragma once

#include <Python.h>

#include <stdint.h>

namespace NCYson {
    extern PyTypeObject PyUnsignedLong_Type;

    PyObject* PreparePyUIntType(PyObject* repr = nullptr);
    PyObject* ConstructPyUIntFromPyLong(PyLongObject*);
    PyObject* ConstructPyUIntFromUint(uint64_t);

    inline int IsExactPyUInt(PyObject* obj) {
        return Py_TYPE(obj) == &PyUnsignedLong_Type;
    }

#if PY_MAJOR_VERSION >= 3
    inline PyObject* ConstructPyNumberFromUint(uint64_t n) {
        return ConstructPyUIntFromUint(n);
    }
#else
    inline PyObject* ConstructPyNumberFromUint(uint64_t n) {
        return PyLong_FromUnsignedLong(n);
    }
#endif
}
