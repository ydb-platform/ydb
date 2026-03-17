/* Licensed under the Apache License: http://www.apache.org/licenses/LICENSE-2.0 */
/* For details: https://github.com/coveragepy/coveragepy/blob/main/NOTICE.txt */

#include "util.h"
#include "tracer.h"
#include "filedisp.h"

/* Module definition */

#define MODULE_DOC PyDoc_STR("Fast coverage tracer.")

static BOOL module_inited = FALSE;

static int
tracer_exec(PyObject *mod)
{
    if (module_inited) {
        return 0;
    }

    if (CTracer_intern_strings() < 0) {
        return -1;
    }

    /* Initialize CTracer */
    CTracerType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&CTracerType) < 0) {
        return -1;
    }

    Py_INCREF(&CTracerType);
    if (PyModule_AddObject(mod, "CTracer", (PyObject *)&CTracerType) < 0) {
        Py_DECREF(&CTracerType);
        return -1;
    }

    /* Initialize CFileDisposition */
    CFileDispositionType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&CFileDispositionType) < 0) {
        Py_DECREF(&CTracerType);
        return -1;
    }

    Py_INCREF(&CFileDispositionType);
    if (PyModule_AddObject(mod, "CFileDisposition", (PyObject *)&CFileDispositionType) < 0) {
        Py_DECREF(&CTracerType);
        Py_DECREF(&CFileDispositionType);
        return -1;
    }

    module_inited = TRUE;
    return 0;
}

static PyModuleDef_Slot tracer_slots[] = {
    {Py_mod_exec, tracer_exec},
#if PY_VERSION_HEX >= 0x030c00f0  // Python 3.12+
    {Py_mod_multiple_interpreters, Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED},
#endif
#if PY_VERSION_HEX >= 0x030d00f0  // Python 3.13+
    // signal that this module supports running without an active GIL
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL}
};

static PyModuleDef moduledef = {
    .m_base = PyModuleDef_HEAD_INIT,
    .m_name = "coverage.tracer",
    .m_doc = MODULE_DOC,
    .m_size = 0,
    .m_slots = tracer_slots,
};

PyMODINIT_FUNC
PyInit_tracer(void)
{
    return PyModuleDef_Init(&moduledef);
}
