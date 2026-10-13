/* Stub for the Yandex Arcadia `__res` module.
 *
 * The CPython source in this tree carries a Yandex patch in Python/import.c
 * (init_importlib_external) that hard-requires `import __res` to succeed,
 * otherwise core initialization fails before main() ever sees control.
 *
 * The real implementation (library/python/runtime_py3/__res.cpp) wires up
 * the binary's embedded resources (library/cpp/resource) and installs an
 * import hook serving Python sources out of those resources. Our hermetic
 * python3 build ships no embedded resources and resolves every import
 * through frozen modules, so we provide a minimal stub: just enough surface
 * for the patched init path and the stdlib call sites (gettext, linecache,
 * ssl) to fall back to their default code paths.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

static PyObject *
res_count(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return PyLong_FromLong(0);
}

static PyObject *
res_find(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
{
    if (nargs != 1) {
        PyErr_Format(PyExc_TypeError,
                     "find() takes 1 positional argument but %zd were given",
                     nargs);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
res_has(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
{
    if (nargs != 1) {
        PyErr_Format(PyExc_TypeError,
                     "has() takes 1 positional argument but %zd were given",
                     nargs);
        return NULL;
    }
    Py_RETURN_FALSE;
}

static PyObject *
res_key_by_index(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
{
    if (nargs != 1) {
        PyErr_Format(PyExc_TypeError,
                     "key_by_index() takes 1 positional argument but %zd were given",
                     nargs);
        return NULL;
    }
    PyErr_SetString(PyExc_IndexError, "no resources embedded");
    return NULL;
}

static PyObject *
res_resfs_read(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
{
    if (nargs != 1) {
        PyErr_Format(PyExc_TypeError,
                     "resfs_read() takes 1 positional argument but %zd were given",
                     nargs);
        return NULL;
    }
    Py_RETURN_NONE;
}

static PyObject *
res_resfs_src(PyObject *self, PyObject *args, PyObject *kwds)
{
    Py_RETURN_NONE;
}

static PyObject *
res_py_src_key(PyObject *self, PyObject *arg)
{
    return PyBytes_FromStringAndSize(NULL, 0);
}

static PyMethodDef res_methods[] = {
    {"count",        res_count,                                METH_NOARGS,                  NULL},
    {"find",         _PyCFunction_CAST(res_find),              METH_FASTCALL,                NULL},
    {"has",          _PyCFunction_CAST(res_has),               METH_FASTCALL,                NULL},
    {"key_by_index", _PyCFunction_CAST(res_key_by_index),      METH_FASTCALL,                NULL},
    {"resfs_read",   _PyCFunction_CAST(res_resfs_read),        METH_FASTCALL,                NULL},
    {"resfs_src",    _PyCFunction_CAST(res_resfs_src),         METH_VARARGS | METH_KEYWORDS, NULL},
    {"py_src_key",   res_py_src_key,                           METH_O,                       NULL},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef res_module = {
    PyModuleDef_HEAD_INIT,
    "__res",
    NULL,
    -1,
    res_methods,
    NULL,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC
PyInit___res(void)
{
    return PyModule_Create(&res_module);
}

/* Registered the way library/python/runtime_py3/runtime_reg_py3.cpp does it
 * in ya: a global constructor extends the inittab before Py_Initialize, so
 * Modules/config.c stays as in upstream. */
__attribute__((constructor))
static void
res_stub_register(void)
{
    PyImport_AppendInittab("__res", PyInit___res);
}
