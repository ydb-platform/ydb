/* Copyright (c) 2011-2018 Adam Jakubek, Rafał Gałczyński
 * Released under the MIT license (see attached LICENSE file).
 */

#include <Python.h>

#include "sllist.h"
#include "dllist.h"

static PyMethodDef llist_methods[] =
{
    { NULL }    /* sentinel */
};

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif

#if PY_MAJOR_VERSION >= 3

static struct PyModuleDef llist_moduledef = {
    PyModuleDef_HEAD_INIT,
    "llist",                            /* m_name */
    "Singly and doubly linked lists.",  /* m_doc */
    -1,                                 /* m_size */
    llist_methods,                      /* m_methods */
    NULL,                               /* m_reload */
    NULL,                               /* m_traverse */
    NULL,                               /* m_clear */
    NULL,                               /* m_free */
};

PyMODINIT_FUNC
PyInit_llist(void)
{
    PyObject* m;

    if (!sllist_init_type())
        return NULL;
    if (!dllist_init_type())
        return NULL;

    m = PyModule_Create(&llist_moduledef);

    sllist_register(m);
    dllist_register(m);

    return m;
}

#else

PyMODINIT_FUNC
initllist(void)
{
    PyObject* m;

    if (!sllist_init_type())
        return;
    if (!dllist_init_type())
        return;

    m = Py_InitModule3("llist", llist_methods,
                       "Singly and doubly linked lists.");

    sllist_register(m);
    dllist_register(m);
}

#endif /* PY_MAJOR_VERSION >= 3 */
