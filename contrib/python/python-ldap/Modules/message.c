/* See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "message.h"
#include "berval.h"
#include "ldapcontrol.h"
#include "constants.h"

/*
 * Converts an LDAP message into a Python structure.
 *
 * On success, returns a list of dictionaries.
 * On failure, returns NULL, and sets an error.
 *
 * The message m is always freed, regardless of return value.
 *
 * If add_ctrls is non-zero, per-entry/referral/partial/intermediate
 * controls will be added as a third item to each entry tuple
 *
 * If add_intermediates is non-zero, intermediate/partial results will
 * be returned
 */
PyObject *
LDAPmessage_to_python(LDAP *ld, LDAPMessage *m, int add_ctrls,
                      int add_intermediates)
{
    /* we convert an LDAP message into a python structure.
     * It is always a list of dictionaries.
     * We always free m.
     */

    PyObject *result, *pyctrls = 0;
    LDAPMessage *entry;
    LDAPControl **serverctrls = 0;
    int rc;

    result = PyList_New(0);
    if (result == NULL) {
        ldap_msgfree(m);
        return NULL;
    }

    for (entry = ldap_first_entry(ld, m);
         entry != NULL; entry = ldap_next_entry(ld, entry)) {
        char *dn;
        char *attr;
        BerElement *ber = NULL;
        PyObject *entrytuple;
        PyObject *attrdict;
        PyObject *pydn;

        dn = ldap_get_dn(ld, entry);
        if (dn == NULL) {
            Py_DECREF(result);
            ldap_msgfree(m);
            return LDAPerror(ld);
        }

        attrdict = PyDict_New();
        if (attrdict == NULL) {
            Py_DECREF(result);
            ldap_msgfree(m);
            ldap_memfree(dn);
            return NULL;
        }

        rc = ldap_get_entry_controls(ld, entry, &serverctrls);
        if (rc) {
            Py_DECREF(result);
            ldap_msgfree(m);
            ldap_memfree(dn);
            return LDAPerror(ld);
        }

        /* convert serverctrls to list of tuples */
        if (!(pyctrls = LDAPControls_to_List(serverctrls))) {
            int err = LDAP_NO_MEMORY;

            ldap_set_option(ld, LDAP_OPT_ERROR_NUMBER, &err);
            Py_DECREF(result);
            ldap_msgfree(m);
            ldap_memfree(dn);
            ldap_controls_free(serverctrls);
            return LDAPerror(ld);
        }
        ldap_controls_free(serverctrls);

        /* Fill attrdict with lists */
        for (attr = ldap_first_attribute(ld, entry, &ber);
             attr != NULL; attr = ldap_next_attribute(ld, entry, ber)
            ) {
            PyObject *valuelist;
            PyObject *pyattr;
            struct berval **bvals;

            pyattr = PyUnicode_FromString(attr);

            bvals = ldap_get_values_len(ld, entry, attr);

            /* Find which list to append to */
            if (PyDict_Contains(attrdict, pyattr)) {
                /* Multiple attribute entries with same name. This code path
                 * is rarely used and cannot be exhausted with OpenLDAP
                 * tests. 389-DS sometimes triggeres it, see
                 * https://github.com/python-ldap/python-ldap/issues/218
                 */
                valuelist = PyDict_GetItem(attrdict, pyattr);
                /* Turn borrowed reference into owned reference */
                if (valuelist != NULL)
                    Py_INCREF(valuelist);
            }
            else {
                valuelist = PyList_New(0);
                if (valuelist != NULL && PyDict_SetItem(attrdict,
                                                        pyattr,
                                                        valuelist) == -1) {
                    Py_DECREF(valuelist);
                    valuelist = NULL;   /* catch error later */
                }
            }

            if (valuelist == NULL) {
                Py_DECREF(pyattr);
                Py_DECREF(attrdict);
                Py_DECREF(result);
                if (ber != NULL)
                    ber_free(ber, 0);
                ldap_msgfree(m);
                ldap_memfree(attr);
                ldap_memfree(dn);
                Py_XDECREF(pyctrls);
                return NULL;
            }

            if (bvals != NULL) {
                Py_ssize_t i;

                for (i = 0; bvals[i]; i++) {
                    PyObject *valuestr;

                    valuestr = LDAPberval_to_object(bvals[i]);
                    if (PyList_Append(valuelist, valuestr) == -1) {
                        Py_DECREF(pyattr);
                        Py_DECREF(attrdict);
                        Py_DECREF(result);
                        Py_DECREF(valuestr);
                        Py_DECREF(valuelist);
                        if (ber != NULL)
                            ber_free(ber, 0);
                        ldap_msgfree(m);
                        ldap_memfree(attr);
                        ldap_memfree(dn);
                        Py_XDECREF(pyctrls);
                        return NULL;
                    }
                    Py_DECREF(valuestr);
                }
                ldap_value_free_len(bvals);
            }
            Py_DECREF(pyattr);
            Py_DECREF(valuelist);
            ldap_memfree(attr);
        }

        pydn = PyUnicode_FromString(dn);
        if (pydn == NULL) {
            Py_DECREF(result);
            ldap_msgfree(m);
            ldap_memfree(dn);
            return NULL;
        }

        if (add_ctrls) {
            entrytuple = Py_BuildValue("(OOO)", pydn, attrdict, pyctrls);
        }
        else {
            entrytuple = Py_BuildValue("(OO)", pydn, attrdict);
        }
        Py_DECREF(pydn);
        ldap_memfree(dn);
        Py_DECREF(attrdict);
        Py_XDECREF(pyctrls);
        PyList_Append(result, entrytuple);
        Py_DECREF(entrytuple);
        if (ber != NULL)
            ber_free(ber, 0);
    }
    for (entry = ldap_first_reference(ld, m);
         entry != NULL; entry = ldap_next_reference(ld, entry)) {
        char **refs = NULL;
        PyObject *entrytuple;
        PyObject *reflist = PyList_New(0);

        if (reflist == NULL) {
            Py_DECREF(result);
            ldap_msgfree(m);
            return NULL;
        }
        if (ldap_parse_reference(ld, entry, &refs, &serverctrls, 0) !=
            LDAP_SUCCESS) {
            Py_DECREF(reflist);
            Py_DECREF(result);
            ldap_msgfree(m);
            return LDAPerror(ld);
        }
        /* convert serverctrls to list of tuples */
        if (!(pyctrls = LDAPControls_to_List(serverctrls))) {
            int err = LDAP_NO_MEMORY;

            ldap_set_option(ld, LDAP_OPT_ERROR_NUMBER, &err);
            Py_DECREF(reflist);
            Py_DECREF(result);
            ldap_msgfree(m);
            ldap_controls_free(serverctrls);
            return LDAPerror(ld);
        }
        ldap_controls_free(serverctrls);
        if (refs) {
            Py_ssize_t i;

            for (i = 0; refs[i] != NULL; i++) {
                /* A referal is a distinguishedName => unicode */
                PyObject *refstr = PyUnicode_FromString(refs[i]);

                PyList_Append(reflist, refstr);
                Py_DECREF(refstr);
            }
            ber_memvfree((void **)refs);
        }
        if (add_ctrls) {
            entrytuple = Py_BuildValue("(sOO)", NULL, reflist, pyctrls);
        }
        else {
            entrytuple = Py_BuildValue("(sO)", NULL, reflist);
        }
        Py_DECREF(reflist);
        Py_XDECREF(pyctrls);
        PyList_Append(result, entrytuple);
        Py_DECREF(entrytuple);
    }
    if (add_intermediates) {
        for (entry = ldap_first_message(ld, m);
             entry != NULL; entry = ldap_next_message(ld, entry)) {
            /* list of tuples */
            /* each tuple is OID, Berval, controllist */
            if (LDAP_RES_INTERMEDIATE == ldap_msgtype(entry)) {
                PyObject *valtuple;
                PyObject *valuestr;
                char *retoid = 0;
                PyObject *pyoid;
                struct berval *retdata = 0;

                if (ldap_parse_intermediate
                    (ld, entry, &retoid, &retdata, &serverctrls,
                     0) != LDAP_SUCCESS) {
                    Py_DECREF(result);
                    ldap_msgfree(m);
                    return LDAPerror(ld);
                }
                /* convert serverctrls to list of tuples */
                if (!(pyctrls = LDAPControls_to_List(serverctrls))) {
                    int err = LDAP_NO_MEMORY;

                    ldap_set_option(ld, LDAP_OPT_ERROR_NUMBER, &err);
                    Py_DECREF(result);
                    ldap_msgfree(m);
                    ldap_controls_free(serverctrls);
                    ldap_memfree(retoid);
                    ber_bvfree(retdata);
                    return LDAPerror(ld);
                }
                ldap_controls_free(serverctrls);

                valuestr = LDAPberval_to_object(retdata);
                ber_bvfree(retdata);
                if (valuestr == NULL) {
                    ldap_memfree(retoid);
                    Py_DECREF(result);
                    ldap_msgfree(m);
                    return NULL;
                }

                pyoid = PyUnicode_FromString(retoid);
                ldap_memfree(retoid);
                if (pyoid == NULL) {
                    Py_DECREF(valuestr);
                    Py_DECREF(result);
                    ldap_msgfree(m);
                    return NULL;
                }

                valtuple = Py_BuildValue("(NNN)", pyoid, valuestr, pyctrls);
                if (valtuple == NULL) {
                    Py_DECREF(result);
                    ldap_msgfree(m);
                    return NULL;
                }

                if (PyList_Append(result, valtuple) == -1) {
                    Py_DECREF(valtuple);
                    Py_DECREF(result);
                    ldap_msgfree(m);
                    return NULL;
                }
                Py_DECREF(valtuple);
            }
        }
    }
    ldap_msgfree(m);
    return result;
}
