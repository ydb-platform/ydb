#include "pycurl.h"
#include "docstrings.h"


/*************************************************************************
// CurlSlistObject
**************************************************************************/

PYCURL_INTERNAL void
util_curlslist_update(CurlSlistObject **old, struct curl_slist *slist)
{
    /* Decref previous object */
    Py_XDECREF(*old);
    /* Create a new object */
    *old = PyObject_New(CurlSlistObject, p_CurlSlist_Type);
    assert(*old != NULL);
    /* Store curl_slist into the new object */
    (*old)->slist = slist;
}

PYCURL_INTERNAL void
do_curlslist_dealloc(CurlSlistObject *self) {
    if (self->slist != NULL) {
        curl_slist_free_all(self->slist);
        self->slist = NULL;
    }
    CurlSlist_Type.tp_free(self);
}

PYCURL_INTERNAL PyTypeObject CurlSlist_Type = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                          /* ob_size */
#endif
    "pycurl.CurlSlist",         /* tp_name */
    sizeof(CurlSlistObject),    /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)do_curlslist_dealloc, /* tp_dealloc */
    0,                          /* tp_print / tp_vectorcall_offset */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_reserved / tp_as_async */
    0,                          /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash */
    0,                          /* tp_call */
    0,                          /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    0,                          /* tp_as_buffer */
    0,                          /* tp_flags */
    0,                          /* tp_doc */
    0,                          /* tp_traverse */
    0,                          /* tp_clear */
    0,                          /* tp_richcompare */
    0,                          /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    0,                          /* tp_methods */
    0,                          /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    0,                          /* tp_init */
    0,                          /* tp_alloc */
    0,                          /* tp_new */
    0,                          /* tp_free */
    0,                          /* tp_is_gc */
    0,                          /* tp_bases */
    0,                          /* tp_mro */
    0,                          /* tp_cache */
    0,                          /* tp_subclasses */
    0,                          /* tp_weaklist */
#if PY_MAJOR_VERSION >= 3
    0,                          /* tp_del */
    0,                          /* tp_version_tag */
    0,                          /* tp_finalize */
#if PY_VERSION_HEX >= 0x03080000
    0,                          /* tp_vectorcall */
#endif
#endif
};


/*************************************************************************
// CurlHttppostObject
**************************************************************************/

PYCURL_INTERNAL void
util_curlhttppost_update(CurlObject *obj, struct curl_httppost *httppost, PyObject *reflist)
{
    /* Decref previous object */
    Py_XDECREF(obj->httppost);
    /* Create a new object */
    obj->httppost = PyObject_New(CurlHttppostObject, p_CurlHttppost_Type);
    assert(obj->httppost != NULL);
    /* Store curl_httppost and reflist into the new object */
    obj->httppost->httppost = httppost;
    obj->httppost->reflist = reflist;
}

PYCURL_INTERNAL void
do_curlhttppost_dealloc(CurlHttppostObject *self) {
    if (self->httppost != NULL) {
        curl_formfree(self->httppost);
        self->httppost = NULL;
    }
    Py_CLEAR(self->reflist);
    CurlHttppost_Type.tp_free(self);
}

PYCURL_INTERNAL PyTypeObject CurlHttppost_Type = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                          /* ob_size */
#endif
    "pycurl.CurlHttppost",      /* tp_name */
    sizeof(CurlHttppostObject), /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)do_curlhttppost_dealloc, /* tp_dealloc */
    0,                          /* tp_print / tp_vectorcall_offset */
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
    0,                          /* tp_reserved / tp_as_async */
    0,                          /* tp_repr */
    0,                          /* tp_as_number */
    0,                          /* tp_as_sequence */
    0,                          /* tp_as_mapping */
    0,                          /* tp_hash */
    0,                          /* tp_call */
    0,                          /* tp_str */
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
    0,                          /* tp_as_buffer */
    0,                          /* tp_flags */
    0,                          /* tp_doc */
    0,                          /* tp_traverse */
    0,                          /* tp_clear */
    0,                          /* tp_richcompare */
    0,                          /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    0,                          /* tp_methods */
    0,                          /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    0,                          /* tp_init */
    0,                          /* tp_alloc */
    0,                          /* tp_new */
    0,                          /* tp_free */
    0,                          /* tp_is_gc */
    0,                          /* tp_bases */
    0,                          /* tp_mro */
    0,                          /* tp_cache */
    0,                          /* tp_subclasses */
    0,                          /* tp_weaklist */
#if PY_MAJOR_VERSION >= 3
    0,                          /* tp_del */
    0,                          /* tp_version_tag */
    0,                          /* tp_finalize */
#if PY_VERSION_HEX >= 0x03080000
    0,                          /* tp_vectorcall */
#endif
#endif
};


/*************************************************************************
// static utility functions
**************************************************************************/


/* assert some CurlObject invariants */
PYCURL_INTERNAL void
assert_curl_state(const CurlObject *self)
{
    assert(self != NULL);
    assert(PyObject_IsInstance((PyObject *) self, (PyObject *) p_Curl_Type) == 1);
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

/* initializer - used to initialize curl easy handles for use with pycurl */
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

    /* Set CAINFO or CAPATH if runtime autodetection enabled */
    #ifdef PYCURL_AUTODETECT_CA
    if (g_pycurl_autodetected_cainfo) {
        res = curl_easy_setopt(self->handle, CURLOPT_CAINFO, g_pycurl_autodetected_cainfo);
        if (res != CURLE_OK) {
            return (-1);
        }
    } else if (g_pycurl_autodetected_capath) {
        res = curl_easy_setopt(self->handle, CURLOPT_CAPATH, g_pycurl_autodetected_capath);
        if (res != CURLE_OK) {
            return (-1);
        }
    }
    #endif

    return (0);
}

/* constructor */
PYCURL_INTERNAL CurlObject *
do_curl_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
    CurlObject *self;
    int res;
    int *ptr;

    if (subtype == p_Curl_Type && !PyArg_ParseTupleAndKeywords(args, kwds, "", empty_keywords)) {
        return NULL;
    }

    /* Allocate python curl object */
    self = (CurlObject *) subtype->tp_alloc(subtype, 0);
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

/* duphandle */
PYCURL_INTERNAL CurlObject *
do_curl_duphandle(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    PyTypeObject *subtype;
    CurlObject *dup;
    int res;
    int *ptr;

    /* Allocate python curl object */
    subtype = Py_TYPE(self);
    dup = (CurlObject *) subtype->tp_alloc(subtype, 0);
    if (dup == NULL)
        return NULL;

    /* tp_alloc is expected to return zeroed memory */
    for (ptr = (int *) &dup->dict;
        ptr < (int *) (((char *) dup) + sizeof(CurlObject));
        ++ptr)
            assert(*ptr == 0);

    /* Clone the curl handle */
    dup->handle = curl_easy_duphandle(self->handle);
    if (dup->handle == NULL)
        goto error;

    /* Set curl error buffer and zero it */
    res = curl_easy_setopt(dup->handle, CURLOPT_ERRORBUFFER, dup->error);
    if (res != CURLE_OK) {
        goto error;
    }
    memset(dup->error, 0, sizeof(dup->error));

    /* Set backreference */
    res = curl_easy_setopt(dup->handle, CURLOPT_PRIVATE, (char *) dup);
    if (res != CURLE_OK) {
        goto error;
    }

    /* Copy attribute dictionary */
    if (self->dict != NULL) {
        dup->dict = PyDict_Copy(self->dict);
        if (dup->dict == NULL) {
            goto error;
        }
    }

    /* Checking for CURLE_OK is not required here.
     * All values have already been successfully setopt'ed with self->handle. */

    /* Assign and incref python callback and update data pointers */
    if (self->w_cb != NULL) {
        dup->w_cb = my_Py_NewRef(self->w_cb);
        curl_easy_setopt(dup->handle, CURLOPT_WRITEDATA, dup);
    }
    if (self->h_cb != NULL) {
        dup->h_cb = my_Py_NewRef(self->h_cb);
        curl_easy_setopt(dup->handle, CURLOPT_WRITEHEADER, dup);
    }
    if (self->r_cb != NULL) {
        dup->r_cb = my_Py_NewRef(self->r_cb);
        curl_easy_setopt(dup->handle, CURLOPT_READDATA, dup);
    }
    if (self->pro_cb != NULL) {
        dup->pro_cb = my_Py_NewRef(self->pro_cb);
        curl_easy_setopt(dup->handle, CURLOPT_PROGRESSDATA, dup);
    }
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
    if (self->xferinfo_cb != NULL) {
        dup->xferinfo_cb = my_Py_NewRef(self->xferinfo_cb);
        curl_easy_setopt(dup->handle, CURLOPT_XFERINFODATA, dup);
    }
#endif
    if (self->debug_cb != NULL) {
        dup->debug_cb = my_Py_NewRef(self->debug_cb);
        curl_easy_setopt(dup->handle, CURLOPT_DEBUGDATA, dup);
    }
    if (self->ioctl_cb != NULL) {
        dup->ioctl_cb = my_Py_NewRef(self->ioctl_cb);
        curl_easy_setopt(dup->handle, CURLOPT_IOCTLDATA, dup);
    }
    if (self->opensocket_cb != NULL) {
        dup->opensocket_cb = my_Py_NewRef(self->opensocket_cb);
        curl_easy_setopt(dup->handle, CURLOPT_OPENSOCKETDATA, dup);
    }
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
    if (self->closesocket_cb != NULL) {
        dup->closesocket_cb = my_Py_NewRef(self->closesocket_cb);
        curl_easy_setopt(dup->handle, CURLOPT_CLOSESOCKETDATA, dup);
    }
#endif
    if (self->sockopt_cb != NULL) {
        dup->sockopt_cb = my_Py_NewRef(self->sockopt_cb);
        curl_easy_setopt(dup->handle, CURLOPT_SOCKOPTDATA, dup);
    }
#ifdef HAVE_CURL_7_19_6_OPTS
    if (self->ssh_key_cb != NULL) {
        dup->ssh_key_cb = my_Py_NewRef(self->ssh_key_cb);
        curl_easy_setopt(dup->handle, CURLOPT_SSH_KEYDATA, dup);
    }
#endif
    if (self->seek_cb != NULL) {
        dup->seek_cb = my_Py_NewRef(self->seek_cb);
        curl_easy_setopt(dup->handle, CURLOPT_SEEKDATA, dup);
    }
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 80, 0)
    if (self->prereq_cb != NULL) {
        dup->prereq_cb = my_Py_NewRef(self->prereq_cb);
        curl_easy_setopt(dup->handle, CURLOPT_PREREQDATA, dup);
    }
#endif

    /* Assign and incref python file objects */
    dup->readdata_fp = my_Py_XNewRef(self->readdata_fp);
    dup->writedata_fp = my_Py_XNewRef(self->writedata_fp);
    dup->writeheader_fp = my_Py_XNewRef(self->writeheader_fp);

    /* Assign and incref postfields object */
    dup->postfields_obj = my_Py_XNewRef(self->postfields_obj);

    /* Assign and incref ca certs related references */
    dup->ca_certs_obj = my_Py_XNewRef(self->ca_certs_obj);

    /* Assign and incref every curl_slist allocated by setopt */
    dup->httpheader = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->httpheader);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    dup->proxyheader = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->proxyheader);
#endif
    dup->http200aliases = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->http200aliases);
    dup->quote = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->quote);
    dup->postquote = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->postquote);
    dup->prequote = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->prequote);
    dup->telnetoptions = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->telnetoptions);
#ifdef HAVE_CURLOPT_RESOLVE
    dup->resolve = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->resolve);
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    dup->mail_rcpt = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->mail_rcpt);
#endif
#ifdef HAVE_CURLOPT_CONNECT_TO
    dup->connect_to = (CurlSlistObject *)my_Py_XNewRef((PyObject *)self->connect_to);
#endif

    /* Assign and incref httppost */
    dup->httppost = (CurlHttppostObject *)my_Py_XNewRef((PyObject *)self->httppost);

    /* Success - return cloned object */
    return dup;

error:
    Py_CLEAR(dup->dict);
    Py_DECREF(dup);    /* this also closes dup->handle */
    PyErr_SetString(ErrorObject, "cloning curl failed");
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
            if (multi_stack->multi_handle != NULL && handle != NULL) {
                /* TODO this is where we could remove the easy object
                from the multi object's easy_object_dict, but this
                requires us to have a reference to the multi object
                which right now we don't. */
                /* Allow threads because callbacks can be invoked */
                PYCURL_BEGIN_ALLOW_THREADS_EASY
                (void) curl_multi_remove_handle(multi_stack->multi_handle, handle);
                PYCURL_END_ALLOW_THREADS_EASY
            }
            self->multi_stack = NULL;
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
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 80, 0)
        Py_CLEAR(self->prereq_cb);
#endif
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
        /* Decrement refcounts for httppost object. */
        Py_CLEAR(self->httppost);
    }

    if (flags & PYCURL_MEMGROUP_CACERTS) {
        /* Decrement refcounts for ca certs related references. */
        Py_CLEAR(self->ca_certs_obj);
    }

    if (flags & PYCURL_MEMGROUP_SLIST) {
        /* Decrement refcounts for slist objects. */
        Py_CLEAR(self->httpheader);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
        Py_CLEAR(self->proxyheader);
#endif
        Py_CLEAR(self->http200aliases);
        Py_CLEAR(self->quote);
        Py_CLEAR(self->postquote);
        Py_CLEAR(self->prequote);
        Py_CLEAR(self->telnetoptions);
#ifdef HAVE_CURLOPT_RESOLVE
        Py_CLEAR(self->resolve);
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
        Py_CLEAR(self->mail_rcpt);
#endif
#ifdef HAVE_CURLOPT_CONNECT_TO
        Py_CLEAR(self->connect_to);
#endif
    }
}


static void
util_curl_close(CurlObject *self)
{
    CURL *handle;

    /* Zero handle and thread-state to disallow any operations to be run
     * from now on */
    assert(self != NULL);
    assert(PyObject_IsInstance((PyObject *) self, (PyObject *) p_Curl_Type) == 1);
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
}


PYCURL_INTERNAL void
do_curl_dealloc(CurlObject *self)
{
    PyObject_GC_UnTrack(self);
    CPy_TRASHCAN_BEGIN(self, do_curl_dealloc);

    Py_CLEAR(self->dict);
    util_curl_close(self);

    Curl_Type.tp_free(self);
    CPy_TRASHCAN_END(self);
}


static PyObject *
do_curl_close(CurlObject *self, PyObject *Py_UNUSED(ignored))
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
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 80, 0)
    VISIT(self->prereq_cb);
#endif

    VISIT(self->readdata_fp);
    VISIT(self->writedata_fp);
    VISIT(self->writeheader_fp);

    VISIT(self->postfields_obj);

    VISIT(self->ca_certs_obj);

    VISIT((PyObject *) self->httpheader);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    VISIT((PyObject *) self->proxyheader);
#endif
    VISIT((PyObject *) self->http200aliases);
    VISIT((PyObject *) self->quote);
    VISIT((PyObject *) self->postquote);
    VISIT((PyObject *) self->prequote);
    VISIT((PyObject *) self->telnetoptions);
#ifdef HAVE_CURLOPT_RESOLVE
    VISIT((PyObject *) self->resolve);
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    VISIT((PyObject *) self->mail_rcpt);
#endif
#ifdef HAVE_CURLOPT_CONNECT_TO
    VISIT((PyObject *) self->connect_to);
#endif

    VISIT((PyObject *) self->httppost);

    return 0;
#undef VISIT
}


/* ------------------------ reset ------------------------ */

static PyObject*
do_curl_reset(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    int res;

    curl_easy_reset(self->handle);

    /* Decref easy interface related objects */
    util_curl_xdecref(self, PYCURL_MEMGROUP_EASY, self->handle);

    res = util_curl_init(self);
    if (res < 0) {
        Py_DECREF(self);    /* this also closes self->handle */
        PyErr_SetString(ErrorObject, "resetting curl failed");
        return NULL;
    }

    Py_RETURN_NONE;
}


static PyObject *do_curl_getstate(CurlObject *self, PyObject *Py_UNUSED(ignored))
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
    {"duphandle", (PyCFunction)do_curl_duphandle, METH_NOARGS, curl_duphandle_doc},
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
