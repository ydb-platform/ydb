/* constants defined for LDAP
 * See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "constants.h"
#include "ldapcontrol.h"

/* the base exception class */

PyObject *LDAPexception_class;

/* list of exception classes */

#define LDAP_ERROR_MIN          LDAP_REFERRAL_LIMIT_EXCEEDED

#ifdef LDAP_PROXIED_AUTHORIZATION_DENIED
#define LDAP_ERROR_MAX          LDAP_PROXIED_AUTHORIZATION_DENIED
#else
#ifdef LDAP_ASSERTION_FAILED
#define LDAP_ERROR_MAX          LDAP_ASSERTION_FAILED
#else
#define LDAP_ERROR_MAX          LDAP_OTHER
#endif
#endif

#define LDAP_ERROR_OFFSET       -LDAP_ERROR_MIN

static PyObject *errobjects[LDAP_ERROR_MAX - LDAP_ERROR_MIN + 1];

/* Convert a bare LDAP error number into an exception */
PyObject *
LDAPerr(int errnum)
{
    if (errnum >= LDAP_ERROR_MIN && errnum <= LDAP_ERROR_MAX &&
            errobjects[errnum + LDAP_ERROR_OFFSET] != NULL) {
        PyErr_SetNone(errobjects[errnum + LDAP_ERROR_OFFSET]);
    }
    else {
        PyObject *args = Py_BuildValue("{s:i}", "errnum", errnum);

        if (args == NULL)
            return NULL;
        PyErr_SetObject(LDAPexception_class, args);
        Py_DECREF(args);
    }
    return NULL;
}

/* Convert an LDAP error into an informative python exception */
PyObject *
LDAPraise_for_message(LDAP *l, LDAPMessage *m)
{
    if (l == NULL) {
        PyErr_SetFromErrno(LDAPexception_class);
        ldap_msgfree(m);
        return NULL;
    }
    else {
        int myerrno, errnum, opt_errnum, msgid = -1, msgtype = 0;
        PyObject *errobj;
        PyObject *info;
        PyObject *str;
        PyObject *pyerrno;
        PyObject *pyresult;
        PyObject *pyctrls = NULL;
        char *matched = NULL, *error = NULL, **refs = NULL;
        LDAPControl **serverctrls = NULL;

        /* at first save errno for later use before it gets overwritten by another call */
        myerrno = errno;

        if (m != NULL) {
            msgid = ldap_msgid(m);
            msgtype = ldap_msgtype(m);
            ldap_parse_result(l, m, &errnum, &matched, &error, &refs,
                              &serverctrls, 1);
        }

        if (msgtype <= 0) {
            opt_errnum = ldap_get_option(l, LDAP_OPT_ERROR_NUMBER, &errnum);
            if (opt_errnum != LDAP_OPT_SUCCESS)
                errnum = opt_errnum;

            if (errnum == LDAP_NO_MEMORY) {
                return PyErr_NoMemory();
            }

            ldap_get_option(l, LDAP_OPT_MATCHED_DN, &matched);
            ldap_get_option(l, LDAP_OPT_ERROR_STRING, &error);
        }

        if (errnum >= LDAP_ERROR_MIN && errnum <= LDAP_ERROR_MAX &&
                errobjects[errnum + LDAP_ERROR_OFFSET] != NULL) {
            errobj = errobjects[errnum + LDAP_ERROR_OFFSET];
        }
        else {
            errobj = LDAPexception_class;
        }

        info = PyDict_New();
        if (info == NULL) {
            ldap_memfree(matched);
            ldap_memfree(error);
            ldap_memvfree((void **)refs);
            ldap_controls_free(serverctrls);
            return NULL;
        }

        if (msgtype > 0) {
            pyresult = PyInt_FromLong(msgtype);
            if (pyresult)
                PyDict_SetItemString(info, "msgtype", pyresult);
            Py_XDECREF(pyresult);
        }

        if (msgid >= 0) {
            pyresult = PyInt_FromLong(msgid);
            if (pyresult)
                PyDict_SetItemString(info, "msgid", pyresult);
            Py_XDECREF(pyresult);
        }

        pyresult = PyInt_FromLong(errnum);
        if (pyresult)
            PyDict_SetItemString(info, "result", pyresult);
        Py_XDECREF(pyresult);

        str = PyUnicode_FromString(ldap_err2string(errnum));
        if (str)
            PyDict_SetItemString(info, "desc", str);
        Py_XDECREF(str);

        if (myerrno != 0) {
            pyerrno = PyInt_FromLong(myerrno);
            if (pyerrno)
                PyDict_SetItemString(info, "errno", pyerrno);
            Py_XDECREF(pyerrno);
        }

        if (!(pyctrls = LDAPControls_to_List(serverctrls))) {
            int err = LDAP_NO_MEMORY;

            ldap_set_option(l, LDAP_OPT_ERROR_NUMBER, &err);
            ldap_memfree(matched);
            ldap_memfree(error);
            ldap_memvfree((void **)refs);
            ldap_controls_free(serverctrls);
            return PyErr_NoMemory();
        }
        ldap_controls_free(serverctrls);
        PyDict_SetItemString(info, "ctrls", pyctrls);
        Py_XDECREF(pyctrls);

        if (matched != NULL) {
            if (*matched != '\0') {
                str = PyUnicode_FromString(matched);
                if (str)
                    PyDict_SetItemString(info, "matched", str);
                Py_XDECREF(str);
            }
            ldap_memfree(matched);
        }

        if (errnum == LDAP_REFERRAL && refs != NULL && refs[0] != NULL) {
            /* Keep old behaviour, overshadow error message */
            char err[1024];

            snprintf(err, sizeof(err), "Referral:\n%s", refs[0]);
            str = PyUnicode_FromString(err);
            PyDict_SetItemString(info, "info", str);
            Py_XDECREF(str);
        }
        else if (error != NULL && *error != '\0') {
            str = PyUnicode_FromString(error);
            if (str)
                PyDict_SetItemString(info, "info", str);
            Py_XDECREF(str);
        }

        PyErr_SetObject(errobj, info);
        Py_DECREF(info);
        ldap_memvfree((void **)refs);
        ldap_memfree(error);
        return NULL;
    }
}

PyObject *
LDAPerror(LDAP *l)
{
    return LDAPraise_for_message(l, NULL);
}

/* initialise the module constants */

int
LDAPinit_constants(PyObject *m)
{
    PyObject *exc, *nobj;
    struct ldap_apifeature_info info = { 1, "X_OPENLDAP_THREAD_SAFE", 0 };
    int thread_safe = 0;

    /* simple constants */

    if (PyModule_AddIntConstant(m, "OPT_ON", 1) != 0)
        return -1;
    if (PyModule_AddIntConstant(m, "OPT_OFF", 0) != 0)
        return -1;

    /* exceptions */

    LDAPexception_class = PyErr_NewException("ldap.LDAPError", NULL, NULL);
    if (LDAPexception_class == NULL) {
        return -1;
    }

    if (PyModule_AddObject(m, "LDAPError", LDAPexception_class) != 0)
        return -1;
    Py_INCREF(LDAPexception_class);

    /* XXX - backward compatibility with pre-1.8 */
    if (PyModule_AddObject(m, "error", LDAPexception_class) != 0)
        return -1;
    Py_INCREF(LDAPexception_class);

#ifdef LDAP_API_FEATURE_X_OPENLDAP_THREAD_SAFE
    if (ldap_get_option(NULL, LDAP_OPT_API_FEATURE_INFO, &info) == LDAP_SUCCESS) {
        thread_safe = (info.ldapaif_version == 1);
    }
#endif
    if (PyModule_AddIntConstant(m, "LIBLDAP_R", thread_safe) != 0)
        return -1;

    /* Generated constants -- see Lib/ldap/constants.py */

#define add_err(n) do {  \
    exc = PyErr_NewException("ldap." #n, LDAPexception_class, NULL);  \
    if (exc == NULL) return -1;  \
    nobj = PyLong_FromLong(LDAP_##n); \
    if (nobj == NULL) return -1; \
    if (PyObject_SetAttrString(exc, "errnum", nobj) != 0) return -1; \
    Py_DECREF(nobj); \
    errobjects[LDAP_##n+LDAP_ERROR_OFFSET] = exc;  \
    if (PyModule_AddObject(m, #n, exc) != 0) return -1;  \
    Py_INCREF(exc);  \
} while (0)

#define add_int(n) do {  \
    if (PyModule_AddIntConstant(m, #n, LDAP_##n) != 0) return -1;  \
} while (0)

#define add_string(n) do {  \
    if (PyModule_AddStringConstant(m, #n, LDAP_##n) != 0) return -1;  \
} while (0)

#include "constants_generated.h"

    return 0;
}
