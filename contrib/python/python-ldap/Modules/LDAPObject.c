/* See https://www.python-ldap.org/ for details. */

#include "common.h"
#include "patchlevel.h"

#include <math.h>
#include <limits.h>
#include "constants.h"
#include "LDAPObject.h"
#include "ldapcontrol.h"
#include "message.h"
#include "berval.h"
#include "options.h"

#ifdef HAVE_SASL
#include <sasl/sasl.h>
#endif

static void free_attrs(char ***);

/* constructor */

LDAPObject *
newLDAPObject(LDAP *l)
{
    LDAPObject *self = (LDAPObject *)PyObject_NEW(LDAPObject, &LDAP_Type);

    if (self == NULL)
        return NULL;
    self->ldap = l;
    self->_save = NULL;
    self->valid = 1;
    return self;
}

/* destructor */

static void
dealloc(LDAPObject *self)
{
    if (self->ldap) {
        if (self->valid) {
            LDAP_BEGIN_ALLOW_THREADS(self);
            ldap_unbind_ext(self->ldap, NULL, NULL);
            LDAP_END_ALLOW_THREADS(self);
            self->valid = 0;
        }
        self->ldap = NULL;
    }
    PyObject_DEL(self);
}

/*------------------------------------------------------------
 * utility functions
 */

/*
 * check to see if the LDAPObject is valid,
 * ie has been opened, and not closed. An exception is set if not valid.
 */

static int
not_valid(LDAPObject *l)
{
    if (l->valid) {
        return 0;
    }
    else {
        PyErr_SetString(LDAPexception_class, "LDAP connection invalid");
        return 1;
    }
}

/* free a LDAPMod (complete or partially) allocated in Tuple_to_LDAPMod() */

static void
LDAPMod_DEL(LDAPMod *lm)
{
    Py_ssize_t i;

    if (lm->mod_type)
        PyMem_DEL(lm->mod_type);
    if (lm->mod_bvalues) {
        for (i = 0; lm->mod_bvalues[i]; i++) {
            PyMem_DEL(lm->mod_bvalues[i]);
        }
        PyMem_DEL(lm->mod_bvalues);
    }
    PyMem_DEL(lm);
}

/*
 * convert a tuple of the form (int,str,[str,...])
 * or (str, [str,...]) if no_op is true, into an LDAPMod structure.
 * See ldap_modify(3) for details.
 *
 * NOTE: the resulting LDAPMod structure has pointers directly into
 *       the Python string storage, so LDAPMod structures MUST have a
 *       shorter lifetime than the tuple passed in.
 */

/* XXX - there is no way to pass complex-structured BER objects in here! */

static LDAPMod *
Tuple_to_LDAPMod(PyObject *tup, int no_op)
{
    int op;
    char *type;
    PyObject *list, *item;
    LDAPMod *lm = NULL;
    Py_ssize_t i, len, nstrs;

    if (!PyTuple_Check(tup)) {
        LDAPerror_TypeError("Tuple_to_LDAPMod(): expected a tuple", tup);
        return NULL;
    }

    if (no_op) {
        if (!PyArg_ParseTuple(tup, "sO:Tuple_to_LDAPMod", &type, &list))
            return NULL;
        op = 0;
    }
    else {
        if (!PyArg_ParseTuple(tup, "isO:Tuple_to_LDAPMod", &op, &type, &list))
            return NULL;
    }

    lm = PyMem_NEW(LDAPMod, 1);

    if (lm == NULL)
        goto nomem;

    lm->mod_op = op | LDAP_MOD_BVALUES;
    lm->mod_bvalues = NULL;

    len = strlen(type);
    lm->mod_type = PyMem_NEW(char, len + 1);

    if (lm->mod_type == NULL)
        goto nomem;
    memcpy(lm->mod_type, type, len + 1);

    if (list == Py_None) {
        /* None indicates a NULL mod_bvals */
    }
    else if (PyBytes_Check(list)) {
        /* Single string is a singleton list */
        lm->mod_bvalues = PyMem_NEW(struct berval *, 2);

        if (lm->mod_bvalues == NULL)
            goto nomem;
        lm->mod_bvalues[0] = PyMem_NEW(struct berval, 1);

        if (lm->mod_bvalues[0] == NULL)
            goto nomem;
        lm->mod_bvalues[1] = NULL;
        lm->mod_bvalues[0]->bv_len = PyBytes_Size(list);
        lm->mod_bvalues[0]->bv_val = PyBytes_AsString(list);
    }
    else if (PySequence_Check(list)) {
        nstrs = PySequence_Length(list);
        lm->mod_bvalues = PyMem_NEW(struct berval *, nstrs + 1);

        if (lm->mod_bvalues == NULL)
            goto nomem;
        for (i = 0; i < nstrs; i++) {
            lm->mod_bvalues[i] = PyMem_NEW(struct berval, 1);

            if (lm->mod_bvalues[i] == NULL)
                goto nomem;
            lm->mod_bvalues[i + 1] = NULL;
            item = PySequence_GetItem(list, i);
            if (item == NULL)
                goto error;
            if (!PyBytes_Check(item)) {
                LDAPerror_TypeError
                    ("Tuple_to_LDAPMod(): expected a byte string in the list",
                     item);
                goto error;
            }
            lm->mod_bvalues[i]->bv_len = PyBytes_Size(item);
            lm->mod_bvalues[i]->bv_val = PyBytes_AsString(item);
            Py_DECREF(item);
        }
        if (nstrs == 0)
            lm->mod_bvalues[0] = NULL;
    }

    return lm;

  nomem:
    PyErr_NoMemory();
  error:
    if (lm)
        LDAPMod_DEL(lm);

    return NULL;
}

/* free the structure allocated in List_to_LDAPMods() */

static void
LDAPMods_DEL(LDAPMod **lms)
{
    LDAPMod **lmp;

    for (lmp = lms; *lmp; lmp++)
        LDAPMod_DEL(*lmp);
    PyMem_DEL(lms);
}

/*
 * convert a list of tuples into a LDAPMod*[] array structure
 * NOTE: list of tuples must live longer than the LDAPMods
 */

static LDAPMod **
List_to_LDAPMods(PyObject *list, int no_op)
{

    Py_ssize_t i, len;
    LDAPMod **lms;
    PyObject *item;

    if (!PySequence_Check(list)) {
        LDAPerror_TypeError("List_to_LDAPMods(): expected list of tuples",
                            list);
        return NULL;
    }

    len = PySequence_Length(list);

    if (len < 0) {
        LDAPerror_TypeError("List_to_LDAPMods(): expected list of tuples",
                            list);
        return NULL;
    }

    lms = PyMem_NEW(LDAPMod *, len + 1);

    if (lms == NULL)
        goto nomem;

    for (i = 0; i < len; i++) {
        lms[i] = NULL;
        item = PySequence_GetItem(list, i);
        if (item == NULL)
            goto error;
        lms[i] = Tuple_to_LDAPMod(item, no_op);
        Py_DECREF(item);
        if (lms[i] == NULL)
            goto error;
    }
    lms[len] = NULL;
    return lms;

  nomem:
    PyErr_NoMemory();
  error:
    if (lms)
        LDAPMods_DEL(lms);
    return NULL;
}

/*
 * convert a python list of strings into an attr list (char*[]).
 * returns 1 if successful, 0 if not (with exception set)
 */

int
attrs_from_List(PyObject *attrlist, char ***attrsp)
{

    char **attrs = NULL;
    PyObject *seq = NULL;

    if (attrlist == Py_None) {
        /* None means a NULL attrlist */
#if PY_MAJOR_VERSION == 2
    }
    else if (PyBytes_Check(attrlist)) {
#else
    }
    else if (PyUnicode_Check(attrlist)) {
#endif
        /* caught by John Benninghoff <johnb@netscape.com> */
        LDAPerror_TypeError
            ("attrs_from_List(): expected *list* of strings, not a string",
             attrlist);
        goto error;
    }
    else {
        PyObject *item = NULL;
        Py_ssize_t i, len, strlen;

#if PY_MAJOR_VERSION >= 3
        const char *str;
#else
        char *str;
#endif

        seq = PySequence_Fast(attrlist, "expected list of strings or None");
        if (seq == NULL)
            goto error;

        len = PySequence_Length(attrlist);

        attrs = PyMem_NEW(char *, len + 1);

        if (attrs == NULL)
            goto nomem;

        for (i = 0; i < len; i++) {
            attrs[i] = NULL;
            item = PySequence_Fast_GET_ITEM(seq, i);
            if (item == NULL)
                goto error;
#if PY_MAJOR_VERSION == 2
            /* Encoded in Python to UTF-8 */
            if (!PyBytes_Check(item)) {
                LDAPerror_TypeError
                    ("attrs_from_List(): expected bytes in list", item);
                goto error;
            }
            if (PyBytes_AsStringAndSize(item, &str, &strlen) == -1) {
                goto error;
            }
#else
            if (!PyUnicode_Check(item)) {
                LDAPerror_TypeError
                    ("attrs_from_List(): expected string in list", item);
                goto error;
            }
            str = PyUnicode_AsUTF8AndSize(item, &strlen);
#endif
            /* Make a copy. PyBytes_AsString* / PyUnicode_AsUTF8* return
             * internal values that must be treated like const char. Python
             * 3.7 actually returns a const char.
             */
            attrs[i] = (char *)PyMem_NEW(char, strlen + 1);

            if (attrs[i] == NULL)
                goto nomem;
            memcpy(attrs[i], str, strlen + 1);
        }
        attrs[len] = NULL;
        Py_DECREF(seq);
    }

    *attrsp = attrs;
    return 1;

  nomem:
    PyErr_NoMemory();
  error:
    Py_XDECREF(seq);
    free_attrs(&attrs);
    return 0;
}

/* free memory allocated from above routine */

static void
free_attrs(char ***attrsp)
{
    char **attrs = *attrsp;
    char **p;

    if (attrs == NULL)
        return;

    *attrsp = NULL;
    for (p = attrs; *p != NULL; p++) {
        PyMem_DEL(*p);
    }
    PyMem_DEL(attrs);
}

/*------------------------------------------------------------
 * methods
 */

/* ldap_unbind_ext */

static PyObject *
l_ldap_unbind_ext(LDAPObject *self, PyObject *args)
{
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int ldaperror;

    if (!PyArg_ParseTuple(args, "|OO:unbind_ext", &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_unbind_ext(self->ldap, server_ldcs, client_ldcs);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    self->valid = 0;
    Py_INCREF(Py_None);
    return Py_None;
}

/* ldap_abandon_ext */

static PyObject *
l_ldap_abandon_ext(LDAPObject *self, PyObject *args)
{
    int msgid;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "i|OO:abandon_ext", &msgid, &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_abandon_ext(self->ldap, msgid, server_ldcs, client_ldcs);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    Py_INCREF(Py_None);
    return Py_None;
}

/* ldap_add_ext */

static PyObject *
l_ldap_add_ext(LDAPObject *self, PyObject *args)
{
    char *dn;
    PyObject *modlist;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;
    LDAPMod **mods;

    if (!PyArg_ParseTuple
        (args, "sO|OO:add_ext", &dn, &modlist, &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    mods = List_to_LDAPMods(modlist, 1);
    if (mods == NULL)
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs)) {
            LDAPMods_DEL(mods);
            return NULL;
        }
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPMods_DEL(mods);
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_add_ext(self->ldap, dn, mods, server_ldcs, client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);
    LDAPMods_DEL(mods);
    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_simple_bind */

static PyObject *
l_ldap_simple_bind(LDAPObject *self, PyObject *args)
{
    char *who;
    int msgid;
    int ldaperror;
    Py_ssize_t cred_len;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;
    struct berval cred;

    if (!PyArg_ParseTuple
        (args, "zz#|OO:simple_bind", &who, &cred.bv_val, &cred_len,
         &serverctrls, &clientctrls))
        return NULL;
    cred.bv_len = (ber_len_t) cred_len;

    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_sasl_bind(self->ldap, who, LDAP_SASL_SIMPLE, &cred, server_ldcs,
                       client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

#ifdef HAVE_SASL
/* The following functions implement SASL binds. A new method
   sasl_interactive_bind_s(bind_dn, sasl_mechanism) has been introduced.

   * The bind_dn argument will be passed to the c library; however,
     normally it is not needed and should be an empty string.

   * The sasl_mechanism argument is an instance of a class that
     implements a callback interface. For convenience, it should be
     derived from the sasl class (which lives in the ldap.sasl module).
     See the module documentation for more information.

     Check your /usr/lib/sasl/ directory for locally installed SASL
     auth modules ("mechanisms"), or try

       ldapsearch   -b "" -s base -LLL -x  supportedSASLMechanisms

     (perhaps with an additional -h and -p argument for ldap host and
     port). The latter will show you which SASL mechanisms are known
     to the LDAP server. If you do not want to set up Kerberos, you
     can still use SASL binds. Your authentication data should then be
     stored in /etc/sasldb (see saslpasswd(8)). If the LDAP server
     does not find the sasldb, it wont allow for DIGEST-MD5 and
     CRAM-MD5. One important thing to get started with sasldb: you
     should first add a dummy user (saslpasswd -c dummy), and this
     will give you some strange error messages. Then delete the dummy
     user (saslpasswd -d dummy), and now you can start adding users to
     your sasldb (again, use the -c switch). Strange, eh?

   * The sasl_mechanism object must implement a method, which will be
     called by the sasl lib several times. The prototype of the
     callback looks like this: callback(id, challenge, prompt,
     defresult) has to return a string (or maybe None). The id
     argument specifies, which information should be passed back to
     the SASL lib (see SASL_CB_xxx in sasl.h)
*/
static int
interaction(unsigned flags, sasl_interact_t *interact, PyObject *SASLObject)
{
/*  const char *dflt = interact->defresult; */
    PyObject *result;
    char *c_result;

    result = PyObject_CallMethod(SASLObject, "callback", "isss", interact->id,  /* see sasl.h */
                                 interact->challenge,
                                 interact->prompt, interact->defresult);

    if (result == NULL)
        /*searching for a better error code */
        return LDAP_OPERATIONS_ERROR;
    c_result = PyBytes_AsString(result);        /*xxx Error checking?? */

    /* according to the sasl docs, we should malloc() the returned
       string only for calls where interact->id == SASL_CB_PASS, so we
       probably leak a few bytes per ldap bind. However, if I restrict
       the strdup() to this case, I get segfaults. Should probably be
       fixed sometimes.
     */
    interact->result = strdup(c_result);
    if (interact->result == NULL)
        return LDAP_OPERATIONS_ERROR;
    interact->len = strlen(c_result);
    /* We _should_ overwrite the python string buffer for security
       reasons, however we may not (api/stringObjects.html). Any ideas?
     */

    Py_DECREF(result);  /*not needed any longer */
    result = NULL;

    return LDAP_SUCCESS;
}

/*
  This function will be called by ldap_sasl_interactive_bind(). The
  "*in" is an array of sasl_interact_t's (see sasl.h for a
  reference). The last interact in the array has an interact->id of
  SASL_CB_LIST_END.

*/

int
py_ldap_sasl_interaction(LDAP *ld, unsigned flags, void *defaults, void *in)
{
    /* These are just typecasts */
    sasl_interact_t *interact = (sasl_interact_t *)in;
    PyObject *SASLObject = (PyObject *)defaults;

    /* Loop over the array of sasl_interact_t structs */
    while (interact->id != SASL_CB_LIST_END) {
        int rc = 0;

        rc = interaction(flags, interact, SASLObject);
        if (rc)
            return rc;
        interact++;
    }
    return LDAP_SUCCESS;
}

static PyObject *
l_ldap_sasl_bind_s(LDAPObject *self, PyObject *args)
{
    const char *dn;
    const char *mechanism;
    struct berval cred;
    Py_ssize_t cred_len;

    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    struct berval *servercred;
    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "zzz#OO:sasl_bind_s", &dn, &mechanism, &cred.bv_val, &cred_len,
         &serverctrls, &clientctrls))
        return NULL;

    if (not_valid(self))
        return NULL;

    cred.bv_len = cred_len;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }
    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_sasl_bind_s(self->ldap,
                                 dn,
                                 mechanism,
                                 cred.bv_val ? &cred : NULL,
                                 (LDAPControl **)server_ldcs,
                                 (LDAPControl **)client_ldcs, &servercred);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror == LDAP_SASL_BIND_IN_PROGRESS) {
        if (servercred && servercred->bv_val && *servercred->bv_val)
            return PyBytes_FromStringAndSize(servercred->bv_val,
                                             servercred->bv_len);
    }
    else if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);
    return PyInt_FromLong(ldaperror);
}

static PyObject *
l_ldap_sasl_interactive_bind_s(LDAPObject *self, PyObject *args)
{
    char *c_mechanism;
    char *who;

    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    PyObject *SASLObject = NULL;
    PyObject *mechanism = NULL;
    int msgid;

    static unsigned sasl_flags = LDAP_SASL_QUIET;

    /*
     * In Python 2.3+, a "I" format argument indicates that we're either converting
     * the Python object into a long or an unsigned int. In versions prior to that,
     * it will always convert to a long. Since the sasl_flags variable is an
     * unsigned int, we need to use the "I" flag if we're running Python 2.3+ and a
     * "i" otherwise.
     */
#if (PY_MAJOR_VERSION == 2) && (PY_MINOR_VERSION < 3)
    if (!PyArg_ParseTuple
        (args, "sOOOi:sasl_interactive_bind_s", &who, &SASLObject,
         &serverctrls, &clientctrls, &sasl_flags))
#else
    if (!PyArg_ParseTuple
        (args, "sOOOI:sasl_interactive_bind_s", &who, &SASLObject,
         &serverctrls, &clientctrls, &sasl_flags))
#endif
        return NULL;

    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    /* now we extract the sasl mechanism from the SASL Object */
    mechanism = PyObject_GetAttrString(SASLObject, "mech");
    if (mechanism == NULL)
        return NULL;
    c_mechanism = PyBytes_AsString(mechanism);
    Py_DECREF(mechanism);
    mechanism = NULL;

    /* Don't know if it is the "intended use" of the defaults
       parameter of ldap_sasl_interactive_bind_s when we pass the
       Python object SASLObject, but passing it through some
       static variable would destroy thread safety, IMHO.
     */
    msgid = ldap_sasl_interactive_bind_s(self->ldap,
                                         who,
                                         c_mechanism,
                                         (LDAPControl **)server_ldcs,
                                         (LDAPControl **)client_ldcs,
                                         sasl_flags,
                                         py_ldap_sasl_interaction, SASLObject);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (msgid != LDAP_SUCCESS)
        return LDAPerror(self->ldap);
    return PyInt_FromLong(msgid);
}
#endif

#ifdef LDAP_API_FEATURE_CANCEL

/* ldap_cancel */

static PyObject *
l_ldap_cancel(LDAPObject *self, PyObject *args)
{
    int msgid;
    int cancelid;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "i|OO:cancel", &cancelid, &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_cancel(self->ldap, cancelid, server_ldcs, client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

#endif

/* ldap_compare_ext */

static PyObject *
l_ldap_compare_ext(LDAPObject *self, PyObject *args)
{
    char *dn, *attr;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;
    Py_ssize_t value_len;
    struct berval value;

    if (!PyArg_ParseTuple
        (args, "sss#|OO:compare_ext", &dn, &attr, &value.bv_val, &value_len,
         &serverctrls, &clientctrls))
        return NULL;
    value.bv_len = (ber_len_t) value_len;

    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_compare_ext(self->ldap, dn, attr, &value, server_ldcs,
                         client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_delete_ext */

static PyObject *
l_ldap_delete_ext(LDAPObject *self, PyObject *args)
{
    char *dn;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "s|OO:delete_ext", &dn, &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_delete_ext(self->ldap, dn, server_ldcs, client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_modify_ext */

static PyObject *
l_ldap_modify_ext(LDAPObject *self, PyObject *args)
{
    char *dn;
    PyObject *modlist;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;
    LDAPMod **mods;

    if (!PyArg_ParseTuple
        (args, "sO|OO:modify_ext", &dn, &modlist, &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    mods = List_to_LDAPMods(modlist, 0);
    if (mods == NULL)
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs)) {
            LDAPMods_DEL(mods);
            return NULL;
        }
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPMods_DEL(mods);
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_modify_ext(self->ldap, dn, mods, server_ldcs, client_ldcs,
                        &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPMods_DEL(mods);
    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_rename */

static PyObject *
l_ldap_rename(LDAPObject *self, PyObject *args)
{
    char *dn, *newrdn;
    char *newSuperior = NULL;
    int delold = 1;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "ss|ziOO:rename", &dn, &newrdn, &newSuperior, &delold,
         &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_rename(self->ldap, dn, newrdn, newSuperior, delold, server_ldcs,
                    client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_result4 */

static PyObject *
l_ldap_result4(LDAPObject *self, PyObject *args)
{
    int msgid = LDAP_RES_ANY;
    int all = 1;
    double timeout = -1.0;
    int add_ctrls = 0;
    int add_intermediates = 0;
    int add_extop = 0;
    struct timeval tv;
    struct timeval *tvp;
    int res_type;
    LDAPMessage *msg = NULL;
    PyObject *retval, *pmsg, *pyctrls = 0;
    int res_msgid = 0;
    char *retoid = 0;
    PyObject *valuestr = NULL;
    int result = LDAP_SUCCESS;
    LDAPControl **serverctrls = 0;

    if (!PyArg_ParseTuple
        (args, "|iidiii:result4", &msgid, &all, &timeout, &add_ctrls,
         &add_intermediates, &add_extop))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (timeout >= 0) {
        tvp = &tv;
        set_timeval_from_double(tvp, timeout);
    }
    else {
        tvp = NULL;
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    res_type = ldap_result(self->ldap, msgid, all, tvp, &msg);
    LDAP_END_ALLOW_THREADS(self);

    if (res_type < 0)   /* LDAP or system error */
        return LDAPerror(self->ldap);

    if (res_type == 0) {
        /* Polls return (None, None, None, None); timeouts raise an exception */
        if (timeout == 0) {
            if (add_extop) {
                return Py_BuildValue("(OOOOOO)", Py_None, Py_None, Py_None,
                                     Py_None, Py_None, Py_None);
            }
            else {
                return Py_BuildValue("(OOOO)", Py_None, Py_None, Py_None,
                                     Py_None);
            }
        }
        else
            return LDAPerr(LDAP_TIMEOUT);
    }

    if (msg)
        res_msgid = ldap_msgid(msg);

    if (res_type == LDAP_RES_SEARCH_ENTRY) {
        /* LDAPmessage_to_python will parse entries and read the controls for each entry */
    }
    else if (res_type == LDAP_RES_SEARCH_REFERENCE) {
        /* LDAPmessage_to_python will parse refs and read the controls for each res */
    }
    else if (res_type == LDAP_RES_INTERMEDIATE) {
        /* LDAPmessage_to_python will parse intermediates and controls */
    }
    else {
        int rc;

        if (res_type == LDAP_RES_EXTENDED) {
            struct berval *retdata = 0;

            LDAP_BEGIN_ALLOW_THREADS(self);
            rc = ldap_parse_extended_result(self->ldap, msg, &retoid, &retdata,
                                            0);
            LDAP_END_ALLOW_THREADS(self);
            /* handle error rc!=0 here? */
            if (rc == LDAP_SUCCESS) {
                valuestr = LDAPberval_to_object(retdata);
            }
            ber_bvfree(retdata);
        }

        LDAP_BEGIN_ALLOW_THREADS(self);
        rc = ldap_parse_result(self->ldap, msg, &result, NULL, NULL, NULL,
                               &serverctrls, 0);
        LDAP_END_ALLOW_THREADS(self);
    }

    if (result != LDAP_SUCCESS) {       /* result error */
        ldap_controls_free(serverctrls);
        Py_XDECREF(valuestr);
        return LDAPraise_for_message(self->ldap, msg);
    }

    if (!(pyctrls = LDAPControls_to_List(serverctrls))) {
        int err = LDAP_NO_MEMORY;

        LDAP_BEGIN_ALLOW_THREADS(self);
        ldap_set_option(self->ldap, LDAP_OPT_ERROR_NUMBER, &err);
        LDAP_END_ALLOW_THREADS(self);
        ldap_controls_free(serverctrls);
        ldap_msgfree(msg);
        Py_XDECREF(valuestr);
        return LDAPerror(self->ldap);
    }
    ldap_controls_free(serverctrls);

    pmsg =
        LDAPmessage_to_python(self->ldap, msg, add_ctrls, add_intermediates);

    if (pmsg == NULL) {
        retval = NULL;
    }
    else {
        /* s handles NULL, but O does not */
        if (add_extop) {
            retval = Py_BuildValue("(iOiOsO)", res_type, pmsg, res_msgid,
                                   pyctrls, retoid,
                                   valuestr ? valuestr : Py_None);
        }
        else {
            retval =
                Py_BuildValue("(iOiO)", res_type, pmsg, res_msgid, pyctrls);
        }

        if (pmsg != Py_None) {
            Py_DECREF(pmsg);
        }
    }
    Py_XDECREF(valuestr);
    Py_XDECREF(pyctrls);
    return retval;
}

/* ldap_search_ext */

static PyObject *
l_ldap_search_ext(LDAPObject *self, PyObject *args)
{
    char *base;
    int scope;
    char *filter;
    PyObject *attrlist = Py_None;
    char **attrs;
    int attrsonly = 0;

    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    double timeout = -1.0;
    struct timeval tv;
    struct timeval *tvp;

    int sizelimit = 0;

    int msgid;
    int ldaperror;

    if (!PyArg_ParseTuple(args, "sis|OiOOdi:search_ext",
                          &base, &scope, &filter, &attrlist, &attrsonly,
                          &serverctrls, &clientctrls, &timeout, &sizelimit))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!attrs_from_List(attrlist, &attrs))
        return NULL;

    if (timeout >= 0) {
        tvp = &tv;
        set_timeval_from_double(tvp, timeout);
    }
    else {
        tvp = NULL;
    }

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs)) {
            free_attrs(&attrs);
            return NULL;
        }
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            free_attrs(&attrs);
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror =
        ldap_search_ext(self->ldap, base, scope, filter, attrs, attrsonly,
                        server_ldcs, client_ldcs, tvp, sizelimit, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    free_attrs(&attrs);
    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_whoami_s (available since OpenLDAP 2.1.13) */

static PyObject *
l_ldap_whoami_s(LDAPObject *self, PyObject *args)
{
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    struct berval *bvalue = NULL;

    PyObject *result;

    int ldaperror;

    if (!PyArg_ParseTuple(args, "|OO:whoami_s", &serverctrls, &clientctrls))
        return NULL;
    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_whoami_s(self->ldap, &bvalue, server_ldcs, client_ldcs);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS) {
        ber_bvfree(bvalue);
        return LDAPerror(self->ldap);
    }

    result = LDAPberval_to_unicode_object(bvalue);
    ber_bvfree(bvalue);

    return result;
}

#ifdef HAVE_TLS
/* ldap_start_tls_s */

static PyObject *
l_ldap_start_tls_s(LDAPObject *self, PyObject *args)
{
    int ldaperror;

    if (!PyArg_ParseTuple(args, ":start_tls_s"))
        return NULL;
    if (not_valid(self))
        return NULL;

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_start_tls_s(self->ldap, NULL, NULL);
    LDAP_END_ALLOW_THREADS(self);
    if (ldaperror != LDAP_SUCCESS) {
        ldap_set_option(self->ldap, LDAP_OPT_ERROR_NUMBER, &ldaperror);
        return LDAPerror(self->ldap);
    }

    Py_INCREF(Py_None);
    return Py_None;
}

#endif

/* ldap_set_option */

static PyObject *
l_ldap_set_option(LDAPObject *self, PyObject *args)
{
    PyObject *value;
    int option;

    if (!PyArg_ParseTuple(args, "iO:set_option", &option, &value))
        return NULL;
    if (not_valid(self))
        return NULL;
    if (!LDAP_set_option(self, option, value))
        return NULL;
    Py_INCREF(Py_None);
    return Py_None;
}

/* ldap_get_option */

static PyObject *
l_ldap_get_option(LDAPObject *self, PyObject *args)
{
    int option;

    if (!PyArg_ParseTuple(args, "i:get_option", &option))
        return NULL;
    if (not_valid(self))
        return NULL;
    return LDAP_get_option(self, option);
}

/* ldap_passwd */

static PyObject *
l_ldap_passwd(LDAPObject *self, PyObject *args)
{
    struct berval user;
    Py_ssize_t user_len;
    struct berval oldpw;
    Py_ssize_t oldpw_len;
    struct berval newpw;
    Py_ssize_t newpw_len;
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "z#z#z#|OO:passwd", &user.bv_val, &user_len, &oldpw.bv_val,
         &oldpw_len, &newpw.bv_val, &newpw_len, &serverctrls, &clientctrls))
        return NULL;

    user.bv_len = (ber_len_t) user_len;
    oldpw.bv_len = (ber_len_t) oldpw_len;
    newpw.bv_len = (ber_len_t) newpw_len;

    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_passwd(self->ldap,
                            user.bv_val != NULL ? &user : NULL,
                            oldpw.bv_val != NULL ? &oldpw : NULL,
                            newpw.bv_val != NULL ? &newpw : NULL,
                            server_ldcs, client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* ldap_extended_operation */

static PyObject *
l_ldap_extended_operation(LDAPObject *self, PyObject *args)
{
    char *reqoid = NULL;
    struct berval reqvalue = { 0, NULL };
    PyObject *serverctrls = Py_None;
    PyObject *clientctrls = Py_None;
    LDAPControl **server_ldcs = NULL;
    LDAPControl **client_ldcs = NULL;

    int msgid;
    int ldaperror;

    if (!PyArg_ParseTuple
        (args, "sz#|OO:extended_operation", &reqoid, &reqvalue.bv_val,
         &reqvalue.bv_len, &serverctrls, &clientctrls))
        return NULL;

    if (not_valid(self))
        return NULL;

    if (!PyNone_Check(serverctrls)) {
        if (!LDAPControls_from_object(serverctrls, &server_ldcs))
            return NULL;
    }

    if (!PyNone_Check(clientctrls)) {
        if (!LDAPControls_from_object(clientctrls, &client_ldcs)) {
            LDAPControl_List_DEL(server_ldcs);
            return NULL;
        }
    }

    LDAP_BEGIN_ALLOW_THREADS(self);
    ldaperror = ldap_extended_operation(self->ldap, reqoid,
                                        reqvalue.bv_val !=
                                        NULL ? &reqvalue : NULL, server_ldcs,
                                        client_ldcs, &msgid);
    LDAP_END_ALLOW_THREADS(self);

    LDAPControl_List_DEL(server_ldcs);
    LDAPControl_List_DEL(client_ldcs);

    if (ldaperror != LDAP_SUCCESS)
        return LDAPerror(self->ldap);

    return PyInt_FromLong(msgid);
}

/* methods */

static PyMethodDef methods[] = {
    {"unbind_ext", (PyCFunction)l_ldap_unbind_ext, METH_VARARGS},
    {"abandon_ext", (PyCFunction)l_ldap_abandon_ext, METH_VARARGS},
    {"add_ext", (PyCFunction)l_ldap_add_ext, METH_VARARGS},
    {"simple_bind", (PyCFunction)l_ldap_simple_bind, METH_VARARGS},
#ifdef HAVE_SASL
    {"sasl_interactive_bind_s", (PyCFunction)l_ldap_sasl_interactive_bind_s,
     METH_VARARGS},
    {"sasl_bind_s", (PyCFunction)l_ldap_sasl_bind_s, METH_VARARGS},
#endif
    {"compare_ext", (PyCFunction)l_ldap_compare_ext, METH_VARARGS},
    {"delete_ext", (PyCFunction)l_ldap_delete_ext, METH_VARARGS},
    {"modify_ext", (PyCFunction)l_ldap_modify_ext, METH_VARARGS},
    {"rename", (PyCFunction)l_ldap_rename, METH_VARARGS},
    {"result4", (PyCFunction)l_ldap_result4, METH_VARARGS},
    {"search_ext", (PyCFunction)l_ldap_search_ext, METH_VARARGS},
#ifdef HAVE_TLS
    {"start_tls_s", (PyCFunction)l_ldap_start_tls_s, METH_VARARGS},
#endif
    {"whoami_s", (PyCFunction)l_ldap_whoami_s, METH_VARARGS},
    {"passwd", (PyCFunction)l_ldap_passwd, METH_VARARGS},
    {"set_option", (PyCFunction)l_ldap_set_option, METH_VARARGS},
    {"get_option", (PyCFunction)l_ldap_get_option, METH_VARARGS},
#ifdef LDAP_API_FEATURE_CANCEL
    {"cancel", (PyCFunction)l_ldap_cancel, METH_VARARGS},
#endif
    {"extop", (PyCFunction)l_ldap_extended_operation, METH_VARARGS},
    {NULL, NULL}
};

/* type entry */

PyTypeObject LDAP_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
        "LDAP",         /*tp_name */
    sizeof(LDAPObject), /*tp_basicsize */
    0,                  /*tp_itemsize */
    /* methods */
    (destructor) dealloc,       /*tp_dealloc */
    0,                  /*tp_print */
    0,                  /*tp_getattr */
    0,                  /*tp_setattr */
    0,                  /*tp_compare */
    0,                  /*tp_repr */
    0,                  /*tp_as_number */
    0,                  /*tp_as_sequence */
    0,                  /*tp_as_mapping */
    0,                  /*tp_hash */
    0,                  /*tp_call */
    0,                  /*tp_str */
    0,                  /*tp_getattro */
    0,                  /*tp_setattro */
    0,                  /*tp_as_buffer */
    0,                  /*tp_flags */
    0,                  /*tp_doc */
    0,                  /*tp_traverse */
    0,                  /*tp_clear */
    0,                  /*tp_richcompare */
    0,                  /*tp_weaklistoffset */
    0,                  /*tp_iter */
    0,                  /*tp_iternext */
    methods,            /*tp_methods */
    0,                  /*tp_members */
    0,                  /*tp_getset */
};
