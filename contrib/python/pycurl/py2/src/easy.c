#include "pycurl.h"
#include "docstrings.h"

/*************************************************************************
// static utility functions
**************************************************************************/


/* assert some CurlObject invariants */
PYCURL_INTERNAL void
assert_curl_state(const CurlObject *self)
{
    assert(self != NULL);
    assert(Py_TYPE(self) == p_Curl_Type);
#ifdef WITH_THREAD
    (void) pycurl_get_thread_state(self);
#endif
}


/* check state for methods */
PYCURL_INTERNAL int
check_curl_state(const CurlObject *self, int flags, const char *name)
{
    assert_curl_state(self);
    if ((flags & 1) && self->handle == NULL) {
        PyErr_Format(ErrorObject, "cannot invoke %s() - no curl handle", name);
        return -1;
    }
#ifdef WITH_THREAD
    if ((flags & 2) && pycurl_get_thread_state(self) != NULL) {
        PyErr_Format(ErrorObject, "cannot invoke %s() - perform() is currently running", name);
        return -1;
    }
#endif
    return 0;
}


/*************************************************************************
// CurlObject
**************************************************************************/

/* --------------- construct/destruct (i.e. open/close) --------------- */

/* initializer - used to intialize curl easy handles for use with pycurl */
static int
util_curl_init(CurlObject *self)
{
    int res;

    /* Set curl error buffer and zero it */
    res = curl_easy_setopt(self->handle, CURLOPT_ERRORBUFFER, self->error);
    if (res != CURLE_OK) {
        return (-1);
    }
    memset(self->error, 0, sizeof(self->error));

    /* Set backreference */
    res = curl_easy_setopt(self->handle, CURLOPT_PRIVATE, (char *) self);
    if (res != CURLE_OK) {
        return (-1);
    }

    /* Enable NOPROGRESS by default, i.e. no progress output */
    res = curl_easy_setopt(self->handle, CURLOPT_NOPROGRESS, (long)1);
    if (res != CURLE_OK) {
        return (-1);
    }

    /* Disable VERBOSE by default, i.e. no verbose output */
    res = curl_easy_setopt(self->handle, CURLOPT_VERBOSE, (long)0);
    if (res != CURLE_OK) {
        return (-1);
    }

    /* Set default USERAGENT */
    assert(g_pycurl_useragent);
    res = curl_easy_setopt(self->handle, CURLOPT_USERAGENT, g_pycurl_useragent);
    if (res != CURLE_OK) {
        return (-1);
    }
    return (0);
}

/* constructor */
PYCURL_INTERNAL CurlObject *
do_curl_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
    CurlObject *self;
    int res;
    int *ptr;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "", empty_keywords)) {
        return NULL;
    }

    /* Allocate python curl object */
    self = (CurlObject *) p_Curl_Type->tp_alloc(p_Curl_Type, 0);
    if (self == NULL)
        return NULL;

    /* tp_alloc is expected to return zeroed memory */
    for (ptr = (int *) &self->dict;
        ptr < (int *) (((char *) self) + sizeof(CurlObject));
        ++ptr)
            assert(*ptr == 0);

    /* Initialize curl handle */
    self->handle = curl_easy_init();
    if (self->handle == NULL)
        goto error;

    res = util_curl_init(self);
    if (res < 0)
            goto error;
    /* Success - return new object */
    return self;

error:
    Py_DECREF(self);    /* this also closes self->handle */
    PyErr_SetString(ErrorObject, "initializing curl failed");
    return NULL;
}


/* util function shared by close() and clear() */
PYCURL_INTERNAL void
util_curl_xdecref(CurlObject *self, int flags, CURL *handle)
{
    if (flags & PYCURL_MEMGROUP_ATTRDICT) {
        /* Decrement refcount for attributes dictionary. */
        Py_CLEAR(self->dict);
    }

    if (flags & PYCURL_MEMGROUP_MULTI) {
        /* Decrement refcount for multi_stack. */
        if (self->multi_stack != NULL) {
            CurlMultiObject *multi_stack = self->multi_stack;
            self->multi_stack = NULL;
            if (multi_stack->multi_handle != NULL && handle != NULL) {
                /* TODO this is where we could remove the easy object
                from the multi object's easy_object_dict, but this
                requires us to have a reference to the multi object
                which right now we don't. */
                (void) curl_multi_remove_handle(multi_stack->multi_handle, handle);
            }
            Py_DECREF(multi_stack);
        }
    }

    if (flags & PYCURL_MEMGROUP_CALLBACK) {
        /* Decrement refcount for python callbacks. */
        Py_CLEAR(self->w_cb);
        Py_CLEAR(self->h_cb);
        Py_CLEAR(self->r_cb);
        Py_CLEAR(self->pro_cb);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
        Py_CLEAR(self->xferinfo_cb);
#endif
        Py_CLEAR(self->debug_cb);
        Py_CLEAR(self->ioctl_cb);
        Py_CLEAR(self->seek_cb);
        Py_CLEAR(self->opensocket_cb);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
        Py_CLEAR(self->closesocket_cb);
#endif
        Py_CLEAR(self->sockopt_cb);
        Py_CLEAR(self->ssh_key_cb);
    }

    if (flags & PYCURL_MEMGROUP_FILE) {
        /* Decrement refcount for python file objects. */
        Py_CLEAR(self->readdata_fp);
        Py_CLEAR(self->writedata_fp);
        Py_CLEAR(self->writeheader_fp);
    }

    if (flags & PYCURL_MEMGROUP_POSTFIELDS) {
        /* Decrement refcount for postfields object */
        Py_CLEAR(self->postfields_obj);
    }

    if (flags & PYCURL_MEMGROUP_SHARE) {
        /* Decrement refcount for share objects. */
        if (self->share != NULL) {
            CurlShareObject *share = self->share;
            self->share = NULL;
            if (share->share_handle != NULL && handle != NULL) {
                curl_easy_setopt(handle, CURLOPT_SHARE, NULL);
            }
            Py_DECREF(share);
        }
    }

    if (flags & PYCURL_MEMGROUP_HTTPPOST) {
        /* Decrement refcounts for httppost related references. */
        Py_CLEAR(self->httppost_ref_list);
    }

    if (flags & PYCURL_MEMGROUP_CACERTS) {
        /* Decrement refcounts for ca certs related references. */
        Py_CLEAR(self->ca_certs_obj);
    }
}


static void
util_curl_close(CurlObject *self)
{
    CURL *handle;

    /* Zero handle and thread-state to disallow any operations to be run
     * from now on */
    assert(self != NULL);
    assert(Py_TYPE(self) == p_Curl_Type);
    handle = self->handle;
    self->handle = NULL;
    if (handle == NULL) {
        /* Some paranoia assertions just to make sure the object
         * deallocation problem is finally really fixed... */
#ifdef WITH_THREAD
        assert(self->state == NULL);
#endif
        assert(self->multi_stack == NULL);
        assert(self->share == NULL);
        return;             /* already closed */
    }
#ifdef WITH_THREAD
    self->state = NULL;
#endif

    /* Decref multi stuff which uses this handle */
    util_curl_xdecref(self, PYCURL_MEMGROUP_MULTI, handle);
    /* Decref share which uses this handle */
    util_curl_xdecref(self, PYCURL_MEMGROUP_SHARE, handle);

    /* Cleanup curl handle - must be done without the gil */
    Py_BEGIN_ALLOW_THREADS
    curl_easy_cleanup(handle);
    Py_END_ALLOW_THREADS
    handle = NULL;

    /* Decref easy related objects */
    util_curl_xdecref(self, PYCURL_MEMGROUP_EASY, handle);

    if (self->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject *) self);
    }

    /* Free all variables allocated by setopt */
#undef SFREE
#define SFREE(v)   if ((v) != NULL) (curl_formfree(v), (v) = NULL)
    SFREE(self->httppost);
#undef SFREE
#define SFREE(v)   if ((v) != NULL) (curl_slist_free_all(v), (v) = NULL)
    SFREE(self->httpheader);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    SFREE(self->proxyheader);
#endif
    SFREE(self->http200aliases);
    SFREE(self->quote);
    SFREE(self->postquote);
    SFREE(self->prequote);
    SFREE(self->telnetoptions);
#ifdef HAVE_CURLOPT_RESOLVE
    SFREE(self->resolve);
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    SFREE(self->mail_rcpt);
#endif
#ifdef HAVE_CURLOPT_CONNECT_TO
    SFREE(self->connect_to);
#endif
#undef SFREE
}


PYCURL_INTERNAL void
do_curl_dealloc(CurlObject *self)
{
    PyObject_GC_UnTrack(self);
    Py_TRASHCAN_SAFE_BEGIN(self);

    Py_CLEAR(self->dict);
    util_curl_close(self);

    Curl_Type.tp_free(self);
    Py_TRASHCAN_SAFE_END(self);
}


static PyObject *
do_curl_close(CurlObject *self)
{
    if (check_curl_state(self, 2, "close") != 0) {
        return NULL;
    }
    util_curl_close(self);
    Py_RETURN_NONE;
}


/* --------------- GC support --------------- */

/* Drop references that may have created reference cycles. */
PYCURL_INTERNAL int
do_curl_clear(CurlObject *self)
{
#ifdef WITH_THREAD
    assert(pycurl_get_thread_state(self) == NULL);
#endif
    util_curl_xdecref(self, PYCURL_MEMGROUP_ALL, self->handle);
    return 0;
}

/* Traverse all refcounted objects. */
PYCURL_INTERNAL int
do_curl_traverse(CurlObject *self, visitproc visit, void *arg)
{
    int err;
#undef VISIT
#define VISIT(v)    if ((v) != NULL && ((err = visit(v, arg)) != 0)) return err

    VISIT(self->dict);
    VISIT((PyObject *) self->multi_stack);
    VISIT((PyObject *) self->share);

    VISIT(self->w_cb);
    VISIT(self->h_cb);
    VISIT(self->r_cb);
    VISIT(self->pro_cb);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
    VISIT(self->xferinfo_cb);
#endif
    VISIT(self->debug_cb);
    VISIT(self->ioctl_cb);
    VISIT(self->seek_cb);
    VISIT(self->opensocket_cb);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
    VISIT(self->closesocket_cb);
#endif
    VISIT(self->sockopt_cb);
    VISIT(self->ssh_key_cb);

    VISIT(self->readdata_fp);
    VISIT(self->writedata_fp);
    VISIT(self->writeheader_fp);

    VISIT(self->postfields_obj);

    VISIT(self->ca_certs_obj);

    return 0;
#undef VISIT
}


/* ------------------------ reset ------------------------ */

static PyObject*
do_curl_reset(CurlObject *self)
{
    int res;

    curl_easy_reset(self->handle);

    /* Decref easy interface related objects */
    util_curl_xdecref(self, PYCURL_MEMGROUP_EASY, self->handle);

    /* Free all variables allocated by setopt */
#undef SFREE
#define SFREE(v)   if ((v) != NULL) (curl_formfree(v), (v) = NULL)
    SFREE(self->httppost);
#undef SFREE
#define SFREE(v)   if ((v) != NULL) (curl_slist_free_all(v), (v) = NULL)
    SFREE(self->httpheader);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    SFREE(self->proxyheader);
#endif
    SFREE(self->http200aliases);
    SFREE(self->quote);
    SFREE(self->postquote);
    SFREE(self->prequote);
    SFREE(self->telnetoptions);
#ifdef HAVE_CURLOPT_RESOLVE
    SFREE(self->resolve);
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    SFREE(self->mail_rcpt);
#endif
#ifdef HAVE_CURLOPT_CONNECT_TO
    SFREE(self->connect_to);
#endif
#undef SFREE
    res = util_curl_init(self);
    if (res < 0) {
        Py_DECREF(self);    /* this also closes self->handle */
        PyErr_SetString(ErrorObject, "resetting curl failed");
        return NULL;
    }

    Py_RETURN_NONE;
}


static PyObject *do_curl_getstate(CurlObject *self)
{
    PyErr_SetString(PyExc_TypeError, "Curl objects do not support serialization");
    return NULL;
}


static PyObject *do_curl_setstate(CurlObject *self, PyObject *args)
{
    PyErr_SetString(PyExc_TypeError, "Curl objects do not support deserialization");
    return NULL;
}


/*************************************************************************
// type definitions
**************************************************************************/

/* --------------- methods --------------- */

PYCURL_INTERNAL PyMethodDef curlobject_methods[] = {
    {"close", (PyCFunction)do_curl_close, METH_NOARGS, curl_close_doc},
    {"errstr", (PyCFunction)do_curl_errstr, METH_NOARGS, curl_errstr_doc},
    {"errstr_raw", (PyCFunction)do_curl_errstr_raw, METH_NOARGS, curl_errstr_raw_doc},
    {"getinfo", (PyCFunction)do_curl_getinfo, METH_VARARGS, curl_getinfo_doc},
    {"getinfo_raw", (PyCFunction)do_curl_getinfo_raw, METH_VARARGS, curl_getinfo_raw_doc},
    {"pause", (PyCFunction)do_curl_pause, METH_VARARGS, curl_pause_doc},
    {"perform", (PyCFunction)do_curl_perform, METH_NOARGS, curl_perform_doc},
    {"perform_rb", (PyCFunction)do_curl_perform_rb, METH_NOARGS, curl_perform_rb_doc},
    {"perform_rs", (PyCFunction)do_curl_perform_rs, METH_NOARGS, curl_perform_rs_doc},
    {"setopt", (PyCFunction)do_curl_setopt, METH_VARARGS, curl_setopt_doc},
    {"setopt_string", (PyCFunction)do_curl_setopt_string, METH_VARARGS, curl_setopt_string_doc},
    {"unsetopt", (PyCFunction)do_curl_unsetopt, METH_VARARGS, curl_unsetopt_doc},
    {"reset", (PyCFunction)do_curl_reset, METH_NOARGS, curl_reset_doc},
#if defined(HAVE_CURL_OPENSSL)
    {"set_ca_certs", (PyCFunction)do_curl_set_ca_certs, METH_VARARGS, curl_set_ca_certs_doc},
#endif
    {"__getstate__", (PyCFunction)do_curl_getstate, METH_NOARGS, NULL},
    {"__setstate__", (PyCFunction)do_curl_setstate, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};


/* --------------- setattr/getattr --------------- */


#if PY_MAJOR_VERSION >= 3

PYCURL_INTERNAL PyObject *
do_curl_getattro(PyObject *o, PyObject *n)
{
    PyObject *v = PyObject_GenericGetAttr(o, n);
    if( !v && PyErr_ExceptionMatches(PyExc_AttributeError) )
    {
        PyErr_Clear();
        v = my_getattro(o, n, ((CurlObject *)o)->dict,
                        curlobject_constants, curlobject_methods);
    }
    return v;
}

PYCURL_INTERNAL int
do_curl_setattro(PyObject *o, PyObject *name, PyObject *v)
{
    assert_curl_state((CurlObject *)o);
    return my_setattro(&((CurlObject *)o)->dict, name, v);
}

#else /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL PyObject *
do_curl_getattr(CurlObject *co, char *name)
{
    assert_curl_state(co);
    return my_getattr((PyObject *)co, name, co->dict,
                      curlobject_constants, curlobject_methods);
}

PYCURL_INTERNAL int
do_curl_setattr(CurlObject *co, char *name, PyObject *v)
{
    assert_curl_state(co);
    return my_setattr(&co->dict, name, v);
}

#endif /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL PyTypeObject Curl_Type = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                          /* ob_size */
#endif
    "pycurl.Curl",              /* tp_name */
    sizeof(CurlObject),         /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)do_curl_dealloc, /* tp_dealloc */
    0,                          /* tp_print */
#if PY_MAJOR_VERSION >= 3
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
#else
    (getattrfunc)do_curl_getattr,  /* tp_getattr */
    (setattrfunc)do_curl_setattr,  /* tp_setattr */
#endif
    0,                          /* tp_reserved */
    0,                          /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash  */
    0,                          /* tp_call */
    0,                          /* tp_str */
#if PY_MAJOR_VERSION >= 3
    (getattrofunc)do_curl_getattro, /* tp_getattro */
    (setattrofunc)do_curl_setattro, /* tp_setattro */
#else
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
#endif
    0,                          /* tp_as_buffer */
    PYCURL_TYPE_FLAGS,          /* tp_flags */
    curl_doc,                   /* tp_doc */
    (traverseproc)do_curl_traverse, /* tp_traverse */
    (inquiry)do_curl_clear,     /* tp_clear */
    0,                          /* tp_richcompare */
    offsetof(CurlObject, weakreflist), /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    curlobject_methods,         /* tp_methods */
    0,                          /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    0,                          /* tp_init */
    PyType_GenericAlloc,        /* tp_alloc */
    (newfunc)do_curl_new,       /* tp_new */
    PyObject_GC_Del,            /* tp_free */
};

/* vi:ts=4:et:nowrap
 */
