#include "pycurl.h"
#include "docstrings.h"

/*************************************************************************
// static utility functions
**************************************************************************/


/* assert some CurlShareObject invariants */
static void
assert_share_state(const CurlShareObject *self)
{
    assert(self != NULL);
    assert(PyObject_IsInstance((PyObject *) self, (PyObject *) p_CurlShare_Type) == 1);
#ifdef WITH_THREAD
    assert(self->lock != NULL);
#endif
}


static int
check_share_state(const CurlShareObject *self, int flags, const char *name)
{
    assert_share_state(self);
    return 0;
}


/* constructor */
PYCURL_INTERNAL CurlShareObject *
do_share_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
    int res;
    CurlShareObject *self;
#ifdef WITH_THREAD
    const curl_lock_function lock_cb = share_lock_callback;
    const curl_unlock_function unlock_cb = share_unlock_callback;
#endif
    int *ptr;
    
    if (subtype == p_CurlShare_Type && !PyArg_ParseTupleAndKeywords(args, kwds, "", empty_keywords)) {
        return NULL;
    }

    /* Allocate python curl-share object */
    self = (CurlShareObject *) subtype->tp_alloc(subtype, 0);
    if (!self) {
        return NULL;
    }

    /* tp_alloc is expected to return zeroed memory */
    for (ptr = (int *) &self->dict;
        ptr < (int *) (((char *) self) + sizeof(CurlShareObject));
        ++ptr) {
            assert(*ptr == 0);
    }
    
#ifdef WITH_THREAD
    self->lock = share_lock_new();
    assert(self->lock != NULL);
#endif

    /* Allocate libcurl share handle */
    self->share_handle = curl_share_init();
    if (self->share_handle == NULL) {
        Py_DECREF(self);
        PyErr_SetString(ErrorObject, "initializing curl-share failed");
        return NULL;
    }

#ifdef WITH_THREAD
    /* Set locking functions and data  */
    res = curl_share_setopt(self->share_handle, CURLSHOPT_LOCKFUNC, lock_cb);
    assert(res == CURLE_OK);
    res = curl_share_setopt(self->share_handle, CURLSHOPT_USERDATA, self);
    assert(res == CURLE_OK);
    res = curl_share_setopt(self->share_handle, CURLSHOPT_UNLOCKFUNC, unlock_cb);
    assert(res == CURLE_OK);
#endif

    return self;
}


PYCURL_INTERNAL int
do_share_traverse(CurlShareObject *self, visitproc visit, void *arg)
{
    int err;
#undef VISIT
#define VISIT(v)    if ((v) != NULL && ((err = visit(v, arg)) != 0)) return err

    VISIT(self->dict);

    return 0;
#undef VISIT
}


/* Drop references that may have created reference cycles. */
PYCURL_INTERNAL int
do_share_clear(CurlShareObject *self)
{
    Py_CLEAR(self->dict);
    return 0;
}


static void
util_share_close(CurlShareObject *self){
    if (self->share_handle != NULL) {
        CURLSH *share_handle = self->share_handle;
        self->share_handle = NULL;
        curl_share_cleanup(share_handle);
    }
}


PYCURL_INTERNAL void
do_share_dealloc(CurlShareObject *self)
{
    PyObject_GC_UnTrack(self);
    CPy_TRASHCAN_BEGIN(self, do_share_dealloc);

    Py_CLEAR(self->dict);
    util_share_close(self);

#ifdef WITH_THREAD
    share_lock_destroy(self->lock);
#endif

    if (self->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject *) self);
    }
     
    CurlShare_Type.tp_free(self);
    CPy_TRASHCAN_END(self);
}


static PyObject *
do_share_close(CurlShareObject *self, PyObject *Py_UNUSED(ignored))
{
    if (check_share_state(self, 2, "close") != 0) {
        return NULL;
    }
    util_share_close(self);
    Py_RETURN_NONE;
}


/* setopt, unsetopt*/
/* --------------- unsetopt/setopt/getinfo --------------- */

static PyObject *
do_curlshare_setopt(CurlShareObject *self, PyObject *args)
{
    int option;
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "iO:setopt", &option, &obj))
        return NULL;
    if (check_share_state(self, 1 | 2, "sharesetopt") != 0)
        return NULL;

    /* early checks of option value */
    if (option <= 0)
        goto error;
    if (option >= (int)CURLOPTTYPE_OFF_T + OPTIONS_SIZE)
        goto error;
    if (option % 10000 >= OPTIONS_SIZE)
        goto error;

#if 0 /* XXX - should we ??? */
    /* Handle the case of None */
    if (obj == Py_None) {
        return util_curl_unsetopt(self, option);
    }
#endif

    /* Handle the case of integer arguments */
    if (PyInt_Check(obj)) {
        long d = PyInt_AsLong(obj);
        switch(d) {
        case CURL_LOCK_DATA_COOKIE:
        case CURL_LOCK_DATA_DNS:
        case CURL_LOCK_DATA_SSL_SESSION:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 57, 0)
        case CURL_LOCK_DATA_CONNECT:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 61, 0)
        case CURL_LOCK_DATA_PSL:
#endif
            break;
        default:
            goto error;
        }
        switch(option) {
        case CURLSHOPT_SHARE:
        case CURLSHOPT_UNSHARE:
            curl_share_setopt(self->share_handle, option, d);
            break;
        default:
            PyErr_SetString(PyExc_TypeError, "integers are not supported for this option");
            return NULL;
        }
        Py_RETURN_NONE;
    }
    /* Failed to match any of the function signatures -- return error */
error:
    PyErr_SetString(PyExc_TypeError, "invalid arguments to setopt");
    return NULL;
}


static PyObject *do_curlshare_getstate(CurlShareObject *self, PyObject *Py_UNUSED(ignored))
{
    PyErr_SetString(PyExc_TypeError, "CurlShare objects do not support serialization");
    return NULL;
}


static PyObject *do_curlshare_setstate(CurlShareObject *self, PyObject *args)
{
    PyErr_SetString(PyExc_TypeError, "CurlShare objects do not support deserialization");
    return NULL;
}


/*************************************************************************
// type definitions
**************************************************************************/

/* --------------- methods --------------- */

PYCURL_INTERNAL PyMethodDef curlshareobject_methods[] = {
    {"close", (PyCFunction)do_share_close, METH_NOARGS, share_close_doc},
    {"setopt", (PyCFunction)do_curlshare_setopt, METH_VARARGS, share_setopt_doc},
    {"__getstate__", (PyCFunction)do_curlshare_getstate, METH_NOARGS, NULL},
    {"__setstate__", (PyCFunction)do_curlshare_setstate, METH_VARARGS, NULL},
    {NULL, NULL, 0, 0}
};


/* --------------- setattr/getattr --------------- */


#if PY_MAJOR_VERSION >= 3

PYCURL_INTERNAL PyObject *
do_share_getattro(PyObject *o, PyObject *n)
{
    PyObject *v;
    assert_share_state((CurlShareObject *)o);
    v = PyObject_GenericGetAttr(o, n);
    if( !v && PyErr_ExceptionMatches(PyExc_AttributeError) )
    {
        PyErr_Clear();
        v = my_getattro(o, n, ((CurlShareObject *)o)->dict,
                        curlshareobject_constants, curlshareobject_methods);
    }
    return v;
}

PYCURL_INTERNAL int
do_share_setattro(PyObject *o, PyObject *n, PyObject *v)
{
    assert_share_state((CurlShareObject *)o);
    return my_setattro(&((CurlShareObject *)o)->dict, n, v);
}

#else /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL PyObject *
do_share_getattr(CurlShareObject *cso, char *name)
{
    assert_share_state(cso);
    return my_getattr((PyObject *)cso, name, cso->dict,
                      curlshareobject_constants, curlshareobject_methods);
}

PYCURL_INTERNAL int
do_share_setattr(CurlShareObject *so, char *name, PyObject *v)
{
    assert_share_state(so);
    return my_setattr(&so->dict, name, v);
}

#endif /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL PyTypeObject CurlShare_Type = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                          /* ob_size */
#endif
    "pycurl.CurlShare",         /* tp_name */
    sizeof(CurlShareObject),    /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)do_share_dealloc, /* tp_dealloc */
    0,                          /* tp_print */
#if PY_MAJOR_VERSION >= 3
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
#else
    (getattrfunc)do_share_getattr,  /* tp_getattr */
    (setattrfunc)do_share_setattr,  /* tp_setattr */
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
    (getattrofunc)do_share_getattro, /* tp_getattro */
    (setattrofunc)do_share_setattro, /* tp_setattro */
#else
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
#endif
    0,                          /* tp_as_buffer */
    PYCURL_TYPE_FLAGS,          /* tp_flags */
    share_doc,                  /* tp_doc */
    (traverseproc)do_share_traverse, /* tp_traverse */
    (inquiry)do_share_clear,    /* tp_clear */
    0,                          /* tp_richcompare */
    offsetof(CurlShareObject, weakreflist), /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    curlshareobject_methods,    /* tp_methods */
    0,                          /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    0,                          /* tp_init */
    PyType_GenericAlloc,        /* tp_alloc */
    (newfunc)do_share_new,      /* tp_new */
    PyObject_GC_Del,            /* tp_free */
};

/* vi:ts=4:et:nowrap
 */
