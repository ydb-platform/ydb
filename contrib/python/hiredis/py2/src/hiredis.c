#include "hiredis.h"
#include "reader.h"

#if IS_PY3K
static int hiredis_ModuleTraverse(PyObject *m, visitproc visit, void *arg) {
    Py_VISIT(GET_STATE(m)->HiErr_Base);
    Py_VISIT(GET_STATE(m)->HiErr_ProtocolError);
    Py_VISIT(GET_STATE(m)->HiErr_ReplyError);
    return 0;
}

static int hiredis_ModuleClear(PyObject *m) {
    Py_CLEAR(GET_STATE(m)->HiErr_Base);
    Py_CLEAR(GET_STATE(m)->HiErr_ProtocolError);
    Py_CLEAR(GET_STATE(m)->HiErr_ReplyError);
    return 0;
}

static struct PyModuleDef hiredis_ModuleDef = {
    PyModuleDef_HEAD_INIT,
    MOD_HIREDIS,
    NULL,
    sizeof(struct hiredis_ModuleState), /* m_size */
    NULL, /* m_methods */
    NULL, /* m_reload */
    hiredis_ModuleTraverse, /* m_traverse */
    hiredis_ModuleClear, /* m_clear */
    NULL /* m_free */
};
#else
struct hiredis_ModuleState hiredis_py_module_state;
#endif

/* Keep pointer around for other classes to access the module state. */
PyObject *mod_hiredis;

#if IS_PY3K
PyMODINIT_FUNC PyInit_hiredis(void)
#else
PyMODINIT_FUNC inithiredis(void)
#endif

{
    if (PyType_Ready(&hiredis_ReaderType) < 0) {
#if IS_PY3K
        return NULL;
#else
        return;
#endif
    }

#if IS_PY3K
    mod_hiredis = PyModule_Create(&hiredis_ModuleDef);
#else
    mod_hiredis = Py_InitModule(MOD_HIREDIS, NULL);
#endif

    /* Setup custom exceptions */
    HIREDIS_STATE->HiErr_Base =
        PyErr_NewException(MOD_HIREDIS ".HiredisError", PyExc_Exception, NULL);
    HIREDIS_STATE->HiErr_ProtocolError =
        PyErr_NewException(MOD_HIREDIS ".ProtocolError", HIREDIS_STATE->HiErr_Base, NULL);
    HIREDIS_STATE->HiErr_ReplyError =
        PyErr_NewException(MOD_HIREDIS ".ReplyError", HIREDIS_STATE->HiErr_Base, NULL);

    Py_INCREF(HIREDIS_STATE->HiErr_Base);
    PyModule_AddObject(mod_hiredis, "HiredisError", HIREDIS_STATE->HiErr_Base);
    Py_INCREF(HIREDIS_STATE->HiErr_ProtocolError);
    PyModule_AddObject(mod_hiredis, "ProtocolError", HIREDIS_STATE->HiErr_ProtocolError);
    Py_INCREF(HIREDIS_STATE->HiErr_ReplyError);
    PyModule_AddObject(mod_hiredis, "ReplyError", HIREDIS_STATE->HiErr_ReplyError);

    Py_INCREF(&hiredis_ReaderType);
    PyModule_AddObject(mod_hiredis, "Reader", (PyObject *)&hiredis_ReaderType);

#if IS_PY3K
    return mod_hiredis;
#endif
}
