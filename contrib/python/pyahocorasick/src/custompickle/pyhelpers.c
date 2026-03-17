#include "pyhelpers.h"

bool 
automaton_save_load_parse_args(KeysStore store, PyObject* args, SaveLoadParameters* result) {

    PyObject* string;

    if (store == STORE_ANY) {
        if (PyTuple_GET_SIZE(args) != 2) {
            PyErr_SetString(PyExc_ValueError, "expected exactly two arguments");
            return false;
        }
    } else {
        if (PyTuple_GET_SIZE(args) != 1) {
            PyErr_SetString(PyExc_ValueError, "expected exactly one argument");
            return false;
        }
    }

    string = F(PyTuple_GetItem)(args, 0);
    if (UNLIKELY(string == NULL)) {
        return false;
    }

#if defined(PY3K)
    if (UNLIKELY(!F(PyUnicode_Check)(string))) {
        PyErr_SetString(PyExc_TypeError, "the first argument must be a string");
        return false;
    }
#else
    if (UNLIKELY(!F(PyString_Check)(string))) {
        PyErr_SetString(PyExc_TypeError, "the first argument must be a string");
        return false;
    }
#endif

    if (store == STORE_ANY) {
        result->callback = F(PyTuple_GetItem)(args, 1);
        if (UNLIKELY(result->callback == NULL)) {
            return false;
        }

        if (UNLIKELY(!F(PyCallable_Check)(result->callback))) {
            PyErr_SetString(PyExc_TypeError, "the second argument must be a callable object");
            return false;
        }
    }

#if defined(PY3K)
    result->path = F(PyUnicode_AsUTF8String)(string);
#else
    result->path = string;
    Py_INCREF(string);
#endif
    if (UNLIKELY(result->path == NULL)) {
        return false;
    }

    return true;
}

