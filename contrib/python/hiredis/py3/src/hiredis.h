#ifndef __HIREDIS_PY_H
#define __HIREDIS_PY_H

#include <Python.h>
#include <read.h>

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#ifndef MOD_HIREDIS
#define MOD_HIREDIS "hiredis"
#endif

struct hiredis_ModuleState {
    PyObject *HiErr_Base;
    PyObject *HiErr_ProtocolError;
    PyObject *HiErr_ReplyError;
};

#define GET_STATE(__s) ((struct hiredis_ModuleState*)PyModule_GetState(__s))

/* Keep pointer around for other classes to access the module state. */
extern PyObject *mod_hiredis;
#define HIREDIS_STATE (GET_STATE(mod_hiredis))

PyMODINIT_FUNC PyInit_hiredis(void);

#endif
