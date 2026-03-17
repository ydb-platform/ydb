/* See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "constants.h"
#include "functions.h"
#include "ldapcontrol.h"

#include "LDAPObject.h"

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC PyInit__ldap(void);
#else
PyMODINIT_FUNC init_ldap(void);
#endif

#define _STR(x)        #x
#define STR(x) _STR(x)

static char version_str[] = STR(LDAPMODULE_VERSION);
static char author_str[] = STR(LDAPMODULE_AUTHOR);
static char license_str[] = STR(LDAPMODULE_LICENSE);

static void
init_pkginfo(PyObject *m)
{
    PyModule_AddStringConstant(m, "__version__", version_str);
    PyModule_AddStringConstant(m, "__author__", author_str);
    PyModule_AddStringConstant(m, "__license__", license_str);
}

/* dummy module methods */
static PyMethodDef methods[] = {
    {NULL, NULL}
};

/* module initialisation */

/* Common initialization code */
PyObject *
init_ldap_module(void)
{
    PyObject *m, *d;

    /* Create the module and add the functions */
#if PY_MAJOR_VERSION >= 3
    static struct PyModuleDef ldap_moduledef = {
        PyModuleDef_HEAD_INIT,
        "_ldap",        /* m_name */
        "",             /* m_doc */
        -1,             /* m_size */
        methods,        /* m_methods */
    };
    m = PyModule_Create(&ldap_moduledef);
#else
    m = Py_InitModule("_ldap", methods);
#endif
    /* Initialize LDAP class */
    if (PyType_Ready(&LDAP_Type) < 0) {
        Py_DECREF(m);
        return NULL;
    }

    /* Add some symbolic constants to the module */
    d = PyModule_GetDict(m);

    init_pkginfo(m);

    if (LDAPinit_constants(m) == -1) {
        return NULL;
    }

    LDAPinit_functions(d);
    LDAPinit_control(d);

    /* Check for errors */
    if (PyErr_Occurred())
        Py_FatalError("can't initialize module _ldap");

    return m;
}

#if PY_MAJOR_VERSION < 3
PyMODINIT_FUNC
init_ldap()
{
    init_ldap_module();
}
#else
PyMODINIT_FUNC
PyInit__ldap()
{
    return init_ldap_module();
}
#endif
