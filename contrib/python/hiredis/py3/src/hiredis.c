#include "hiredis.h"
#include "reader.h"
#include "pack.h"

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

static PyObject*
py_pack_command(PyObject* self, PyObject* cmd)
{
    return pack_command(cmd);
}

PyDoc_STRVAR(pack_command_doc, "Pack a series of arguments into the Redis protocol");

PyMethodDef pack_command_method = {
    "pack_command",                 /* The name as a C string. */
    (PyCFunction) py_pack_command,  /* The C function to invoke. */
    METH_O,                         /* Flags telling Python how to invoke */
    pack_command_doc,               /* The docstring as a C string. */
};


PyMethodDef methods[] = {
    {"pack_command", (PyCFunction) py_pack_command, METH_O, pack_command_doc},
    {NULL},
};

static struct PyModuleDef hiredis_ModuleDef = {
    PyModuleDef_HEAD_INIT,
    MOD_HIREDIS,
    NULL,
    sizeof(struct hiredis_ModuleState), /* m_size */
    methods, /* m_methods */
    NULL, /* m_reload */
    hiredis_ModuleTraverse, /* m_traverse */
    hiredis_ModuleClear, /* m_clear */
    NULL /* m_free */
};

/* Keep pointer around for other classes to access the module state. */
PyObject *mod_hiredis;

PyMODINIT_FUNC PyInit_hiredis(void)

{
    if (PyType_Ready(&hiredis_ReaderType) < 0) {
        return NULL;
    }

    mod_hiredis = PyModule_Create(&hiredis_ModuleDef);

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

    return mod_hiredis;
}
