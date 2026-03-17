/* This is a set of functions used to test C++ exceptions are not
 * broken during greenlet switches
 */

#include "../greenlet.h"

struct exception_t {
    int depth;
    exception_t(int depth) : depth(depth) {}
};

/* Functions are called via pointers to prevent inlining */
static void (*p_test_exception_throw)(int depth);
static PyObject* (*p_test_exception_switch_recurse)(int depth, int left);

static void
test_exception_throw(int depth)
{
    throw exception_t(depth);
}

static PyObject*
test_exception_switch_recurse(int depth, int left)
{
    if (left > 0) {
        return p_test_exception_switch_recurse(depth, left - 1);
    }

    PyObject* result = NULL;
    PyGreenlet* self = PyGreenlet_GetCurrent();
    if (self == NULL)
        return NULL;

    try {
        PyGreenlet_Switch(self->parent, NULL, NULL);
        p_test_exception_throw(depth);
        PyErr_SetString(PyExc_RuntimeError,
                        "throwing C++ exception didn't work");
    }
    catch (exception_t& e) {
        if (e.depth != depth)
            PyErr_SetString(PyExc_AssertionError, "depth mismatch");
        else
            result = PyLong_FromLong(depth);
    }
    catch (...) {
        PyErr_SetString(PyExc_RuntimeError, "unexpected C++ exception");
    }

    Py_DECREF(self);
    return result;
}

/* test_exception_switch(int depth)
 * - recurses depth times
 * - switches to parent inside try/catch block
 * - throws an exception that (expected to be caught in the same function)
 * - verifies depth matches (exceptions shouldn't be caught in other greenlets)
 */
static PyObject*
test_exception_switch(PyObject* self, PyObject* args)
{
    int depth;
    if (!PyArg_ParseTuple(args, "i", &depth))
        return NULL;
    return p_test_exception_switch_recurse(depth, depth);
}

static PyMethodDef test_methods[] = {
    {"test_exception_switch",
     (PyCFunction)&test_exception_switch,
     METH_VARARGS,
     "Switches to parent twice, to test exception handling and greenlet "
     "switching."},
    {NULL, NULL, 0, NULL}};

#if PY_MAJOR_VERSION >= 3
#    define INITERROR return NULL

static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT,
                                       "greenlet.tests._test_extension_cpp",
                                       NULL,
                                       0,
                                       test_methods,
                                       NULL,
                                       NULL,
                                       NULL,
                                       NULL};

PyMODINIT_FUNC
PyInit__test_extension_cpp(void)
#else
#    define INITERROR return
PyMODINIT_FUNC
init_test_extension_cpp(void)
#endif
{
    PyObject* module = NULL;

#if PY_MAJOR_VERSION >= 3
    module = PyModule_Create(&moduledef);
#else
    module = Py_InitModule("__tests__._test_extension_cpp", test_methods);
#endif

    if (module == NULL) {
        INITERROR;
    }

    PyGreenlet_Import();
    if (_PyGreenlet_API == NULL) {
        INITERROR;
    }

    p_test_exception_throw = test_exception_throw;
    p_test_exception_switch_recurse = test_exception_switch_recurse;

#if PY_MAJOR_VERSION >= 3
    return module;
#endif
}
