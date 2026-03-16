#ifndef __HIREDIS_PY_H
#define __HIREDIS_PY_H

#include <Python.h>
#include <read.h>

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K 1
#endif

#ifndef MOD_HIREDIS
#define MOD_HIREDIS "hiredis"
#endif

struct hiredis_ModuleState {
    PyObject *HiErr_Base;
    PyObject *HiErr_ProtocolError;
    PyObject *HiErr_ReplyError;
};

#if IS_PY3K
#define GET_STATE(__s) ((struct hiredis_ModuleState*)PyModule_GetState(__s))
#else
extern struct hiredis_ModuleState hiredis_py_module_state;
#define GET_STATE(__s) (&hiredis_py_module_state)
#endif

/* Keep pointer around for other classes to access the module state. */
extern PyObject *mod_hiredis;
#define HIREDIS_STATE (GET_STATE(mod_hiredis))

#ifdef IS_PY3K
PyMODINIT_FUNC PyInit_hiredis(void);
#else
PyMODINIT_FUNC inithiredis(void);
#endif

#endif
