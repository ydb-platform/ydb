#include "pycurl.h"
#include "docstrings.h"

/*************************************************************************
// static utility functions
**************************************************************************/


/* assert some CurlMultiObject invariants */
static void
assert_multi_state(const CurlMultiObject *self)
{
    assert(self != NULL);
    assert(PyObject_IsInstance((PyObject *) self, (PyObject *) p_CurlMulti_Type) == 1);
#ifdef WITH_THREAD
    if (self->state != NULL) {
        assert(self->multi_handle != NULL);
    }
#endif
}


static int
check_multi_state(const CurlMultiObject *self, int flags, const char *name)
{
    assert_multi_state(self);
    if ((flags & 1) && self->multi_handle == NULL) {
        PyErr_Format(ErrorObject, "cannot invoke %s() - no multi handle", name);
        return -1;
    }
#ifdef WITH_THREAD
    if ((flags & 2) && self->state != NULL) {
        PyErr_Format(ErrorObject, "cannot invoke %s() - multi_perform() is currently running", name);
        return -1;
    }
#endif
    return 0;
}


/*************************************************************************
// CurlMultiObject
**************************************************************************/

/* --------------- construct/destruct (i.e. open/close) --------------- */

/* constructor */
PYCURL_INTERNAL CurlMultiObject *
do_multi_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
    CurlMultiObject *self;
    int *ptr;

    if (subtype == p_CurlMulti_Type && !PyArg_ParseTupleAndKeywords(args, kwds, "", empty_keywords)) {
        return NULL;
    }

    /* Allocate python curl-multi object */
    self = (CurlMultiObject *) subtype->tp_alloc(subtype, 0);
    if (!self) {
        return NULL;
    }

    /* tp_alloc is expected to return zeroed memory */
    for (ptr = (int *) &self->dict;
        ptr < (int *) (((char *) self) + sizeof(CurlMultiObject));
        ++ptr)
            assert(*ptr == 0);

    self->easy_object_dict = PyDict_New();
    if (self->easy_object_dict == NULL) {
        Py_DECREF(self);
        return NULL;
    }
    
    /* Allocate libcurl multi handle */
    self->multi_handle = curl_multi_init();
    if (self->multi_handle == NULL) {
        Py_DECREF(self);
        PyErr_SetString(ErrorObject, "initializing curl-multi failed");
        return NULL;
    }
    return self;
}

static void
util_multi_close(CurlMultiObject *self)
{
    assert(self != NULL);

#ifdef WITH_THREAD
    self->state = NULL;
#endif
    
    if (self->multi_handle != NULL) {
        CURLM *multi_handle = self->multi_handle;
        /* Allow threads because callbacks can be invoked */
        PYCURL_BEGIN_ALLOW_THREADS
        curl_multi_cleanup(multi_handle);
        PYCURL_END_ALLOW_THREADS
        self->multi_handle = NULL;
    }
}


static void
util_multi_xdecref(CurlMultiObject *self)
{
    Py_CLEAR(self->easy_object_dict);
    Py_CLEAR(self->dict);
    Py_CLEAR(self->t_cb);
    Py_CLEAR(self->s_cb);
}


PYCURL_INTERNAL void
do_multi_dealloc(CurlMultiObject *self)
{
    PyObject_GC_UnTrack(self);
    CPy_TRASHCAN_BEGIN(self, do_multi_dealloc);

    util_multi_xdecref(self);
    util_multi_close(self);

    if (self->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject *) self);
    }

    CurlMulti_Type.tp_free(self);
    CPy_TRASHCAN_END(self);
}


static PyObject *
do_multi_close(CurlMultiObject *self, PyObject *Py_UNUSED(ignored))
{
    if (check_multi_state(self, 2, "close") != 0) {
        return NULL;
    }
    util_multi_close(self);
    Py_RETURN_NONE;
}


/* --------------- GC support --------------- */

/* Drop references that may have created reference cycles. */
PYCURL_INTERNAL int
do_multi_clear(CurlMultiObject *self)
{
    util_multi_xdecref(self);
    return 0;
}

PYCURL_INTERNAL int
do_multi_traverse(CurlMultiObject *self, visitproc visit, void *arg)
{
    int err;
#undef VISIT
#define VISIT(v)    if ((v) != NULL && ((err = visit(v, arg)) != 0)) return err

    VISIT(self->dict);
    VISIT(self->easy_object_dict);

    return 0;
#undef VISIT
}


/* --------------- setopt --------------- */

static int
multi_socket_callback(CURL *easy,
                      curl_socket_t s,
                      int what,
                      void *userp,
                      void *socketp)
{
    CurlMultiObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    PYCURL_DECLARE_THREAD_STATE;

    /* acquire thread */
    self = (CurlMultiObject *)userp;
    if (!PYCURL_ACQUIRE_THREAD_MULTI()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "multi_socket_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return 0;
    }

    /* check args */
    if (self->s_cb == NULL)
        goto silent_error;

    if (socketp == NULL) {
        Py_INCREF(Py_None);
        socketp = Py_None;
    }

    /* run callback */
    arglist = Py_BuildValue("(iiOO)", what, s, userp, (PyObject *)socketp);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->s_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* return values from socket callbacks should be ignored */

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return 0;
verbose_error:
    PyErr_Print();
    goto silent_error;
    return 0;
}


static int
multi_timer_callback(CURLM *multi,
                     long timeout_ms,
                     void *userp)
{
    CurlMultiObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    int ret = 0;       /* always success */
    PYCURL_DECLARE_THREAD_STATE;

    UNUSED(multi);

    /* acquire thread */
    self = (CurlMultiObject *)userp;
    if (!PYCURL_ACQUIRE_THREAD_MULTI()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "multi_timer_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check args */
    if (self->t_cb == NULL)
        goto silent_error;

    /* run callback */
    arglist = Py_BuildValue("(i)", timeout_ms);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->t_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* return values from timer callbacks should be ignored */

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;

    return 0;
}


static PyObject *
do_multi_setopt_int(CurlMultiObject *self, int option, PyObject *obj)
{
    long d = PyInt_AsLong(obj);
    switch(option) {
    case CURLMOPT_MAXCONNECTS:
    case CURLMOPT_PIPELINING:
#ifdef HAVE_CURL_7_30_0_PIPELINE_OPTS
    case CURLMOPT_MAX_HOST_CONNECTIONS:
    case CURLMOPT_MAX_TOTAL_CONNECTIONS:
    case CURLMOPT_MAX_PIPELINE_LENGTH:
    case CURLMOPT_CONTENT_LENGTH_PENALTY_SIZE:
    case CURLMOPT_CHUNK_LENGTH_PENALTY_SIZE:
#endif
#ifdef HAVE_CURL_7_67_0_MULTI_STREAMS
    case CURLMOPT_MAX_CONCURRENT_STREAMS:
#endif
        curl_multi_setopt(self->multi_handle, option, d);
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "integers are not supported for this option");
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
do_multi_setopt_charpp(CurlMultiObject *self, int option, int which, PyObject *obj)
{
    Py_ssize_t len, i;
    int res;
    static const char *empty_list[] = { NULL };
    char **list = NULL;
    PyObject **encoded_objs = NULL;
    PyObject *encoded_obj = NULL;
    char *encoded_str;
    PyObject *rv = NULL;

    len = PyListOrTuple_Size(obj, which);
    if (len == 0) {
        res = curl_multi_setopt(self->multi_handle, option, empty_list);
        if (res != CURLE_OK) {
            CURLERROR_RETVAL_MULTI_DONE();
        }
        Py_RETURN_NONE;
    }

    /* add NULL terminator as the last list item */
    list = PyMem_New(char *, len+1);
    if (list == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    /* no need for the NULL terminator here */
    encoded_objs = PyMem_New(PyObject *, len);
    if (encoded_objs == NULL) {
        PyErr_NoMemory();
        goto done;
    }
    memset(encoded_objs, 0, sizeof(PyObject *) * len);

    for (i = 0; i < len; i++) {
        PyObject *listitem = PyListOrTuple_GetItem(obj, i, which);
        if (!PyText_Check(listitem)) {
            PyErr_SetString(ErrorObject, "list/tuple items must be strings");
            goto done;
        }
        encoded_str = PyText_AsString_NoNUL(listitem, &encoded_obj);
        if (encoded_str == NULL) {
            goto done;
        }
        list[i] = encoded_str;
        encoded_objs[i] = encoded_obj;
    }
    list[len] = NULL;

    res = curl_multi_setopt(self->multi_handle, option, list);
    if (res != CURLE_OK) {
        rv = NULL;
        CURLERROR_RETVAL_MULTI_DONE();
    }

    rv = Py_None;
done:
    if (encoded_objs) {
        for (i = 0; i < len; i++) {
            Py_XDECREF(encoded_objs[i]);
        }
        PyMem_Free(encoded_objs);
    }
    PyMem_Free(list);
    return rv;
}


static PyObject *
do_multi_setopt_list(CurlMultiObject *self, int option, int which, PyObject *obj)
{
    switch(option) {
#ifdef HAVE_CURL_7_30_0_PIPELINE_OPTS
    case CURLMOPT_PIPELINING_SITE_BL:
    case CURLMOPT_PIPELINING_SERVER_BL:
#endif
        return do_multi_setopt_charpp(self, option, which, obj);
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "lists/tuples are not supported for this option");
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
do_multi_setopt_callable(CurlMultiObject *self, int option, PyObject *obj)
{
    /* We use function types here to make sure that our callback
     * definitions exactly match the <curl/multi.h> interface.
     */
    const curl_multi_timer_callback t_cb = multi_timer_callback;
    const curl_socket_callback s_cb = multi_socket_callback;

    switch(option) {
    case CURLMOPT_SOCKETFUNCTION:
        curl_multi_setopt(self->multi_handle, CURLMOPT_SOCKETFUNCTION, s_cb);
        curl_multi_setopt(self->multi_handle, CURLMOPT_SOCKETDATA, self);
        Py_INCREF(obj);
        self->s_cb = obj;
        break;
    case CURLMOPT_TIMERFUNCTION:
        curl_multi_setopt(self->multi_handle, CURLMOPT_TIMERFUNCTION, t_cb);
        curl_multi_setopt(self->multi_handle, CURLMOPT_TIMERDATA, self);
        Py_INCREF(obj);
        self->t_cb = obj;
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "callables are not supported for this option");
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
do_multi_setopt_none(CurlMultiObject *self, int option, PyObject *obj)
{
    switch(option) {
#ifdef HAVE_CURL_7_30_0_PIPELINE_OPTS
    case CURLMOPT_PIPELINING_SITE_BL:
    case CURLMOPT_PIPELINING_SERVER_BL:
        curl_multi_setopt(self->multi_handle, option, NULL);
        break;
#endif
    case CURLMOPT_SOCKETFUNCTION:
        curl_multi_setopt(self->multi_handle, CURLMOPT_SOCKETFUNCTION, NULL);
        curl_multi_setopt(self->multi_handle, CURLMOPT_SOCKETDATA, NULL);
        Py_CLEAR(self->s_cb);
        break;
    case CURLMOPT_TIMERFUNCTION:
        curl_multi_setopt(self->multi_handle, CURLMOPT_TIMERFUNCTION, NULL);
        curl_multi_setopt(self->multi_handle, CURLMOPT_TIMERDATA, NULL);
        Py_CLEAR(self->t_cb);
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "unsetting is not supported for this option");
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
do_multi_setopt(CurlMultiObject *self, PyObject *args)
{
    int option, which;
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "iO:setopt", &option, &obj))
        return NULL;
    if (check_multi_state(self, 1 | 2, "setopt") != 0)
        return NULL;

    /* Early checks of option value */
    if (option <= 0)
        goto error;
    if (option >= (int)CURLOPTTYPE_OFF_T + MOPTIONS_SIZE)
        goto error;
    if (option % 10000 >= MOPTIONS_SIZE)
        goto error;

    /* Handle unsetting of options */
    if (obj == Py_None) {
        return do_multi_setopt_none(self, option, obj);
    }

    /* Handle the case of integer arguments */
    if (PyInt_Check(obj)) {
        return do_multi_setopt_int(self, option, obj);
    }

    /* Handle the case of list or tuple objects */
    which = PyListOrTuple_Check(obj);
    if (which) {
        return do_multi_setopt_list(self, option, which, obj);
    }

    if (PyFunction_Check(obj) || PyCFunction_Check(obj) ||
        PyCallable_Check(obj) || PyMethod_Check(obj)) {
        return do_multi_setopt_callable(self, option, obj);
    }

    /* Failed to match any of the function signatures -- return error */
error:
    PyErr_SetString(PyExc_TypeError, "invalid arguments to setopt");
    return NULL;
}


/* --------------- timeout --------------- */

static PyObject *
do_multi_timeout(CurlMultiObject *self, PyObject *Py_UNUSED(ignored))
{
    CURLMcode res;
    long timeout;

    if (check_multi_state(self, 1 | 2, "timeout") != 0) {
        return NULL;
    }

    res = curl_multi_timeout(self->multi_handle, &timeout);
    if (res != CURLM_OK) {
        CURLERROR_MSG("timeout failed");
    }

    /* Return number of millisecs until timeout */
    return Py_BuildValue("l", timeout);
}


/* --------------- assign --------------- */

static PyObject *
do_multi_assign(CurlMultiObject *self, PyObject *args)
{
    CURLMcode res;
    curl_socket_t socket;
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "iO:assign", &socket, &obj))
        return NULL;
    if (check_multi_state(self, 1 | 2, "assign") != 0) {
        return NULL;
    }
    Py_INCREF(obj);

    res = curl_multi_assign(self->multi_handle, socket, obj);
    if (res != CURLM_OK) {
        CURLERROR_MSG("assign failed");
    }

    Py_RETURN_NONE;
}


/* --------------- socket_action --------------- */
static PyObject *
do_multi_socket_action(CurlMultiObject *self, PyObject *args)
{
    CURLMcode res;
    curl_socket_t socket;
    int ev_bitmask;
    int running = -1;

    if (!PyArg_ParseTuple(args, "ii:socket_action", &socket, &ev_bitmask))
        return NULL;
    if (check_multi_state(self, 1 | 2, "socket_action") != 0) {
        return NULL;
    }

    PYCURL_BEGIN_ALLOW_THREADS
    res = curl_multi_socket_action(self->multi_handle, socket, ev_bitmask, &running);
    PYCURL_END_ALLOW_THREADS

    if (res != CURLM_OK) {
        CURLERROR_MSG("multi_socket_action failed");
    }
    /* Return a tuple with the result and the number of running handles */
    return Py_BuildValue("(ii)", (int)res, running);
}

/* --------------- socket_all --------------- */

static PyObject *
do_multi_socket_all(CurlMultiObject *self, PyObject *Py_UNUSED(ignored))
{
    CURLMcode res;
    int running = -1;

    if (check_multi_state(self, 1 | 2, "socket_all") != 0) {
        return NULL;
    }

    PYCURL_BEGIN_ALLOW_THREADS
    res = curl_multi_socket_all(self->multi_handle, &running);
    PYCURL_END_ALLOW_THREADS

    /* We assume these errors are ok, otherwise raise exception */
    if (res != CURLM_OK && res != CURLM_CALL_MULTI_PERFORM) {
        CURLERROR_MSG("perform failed");
    }

    /* Return a tuple with the result and the number of running handles */
    return Py_BuildValue("(ii)", (int)res, running);
}


/* --------------- perform --------------- */

static PyObject *
do_multi_perform(CurlMultiObject *self, PyObject *Py_UNUSED(ignored))
{
    CURLMcode res;
    int running = -1;

    if (check_multi_state(self, 1 | 2, "perform") != 0) {
        return NULL;
    }

    PYCURL_BEGIN_ALLOW_THREADS
    res = curl_multi_perform(self->multi_handle, &running);
    PYCURL_END_ALLOW_THREADS

    /* We assume these errors are ok, otherwise raise exception */
    if (res != CURLM_OK && res != CURLM_CALL_MULTI_PERFORM) {
        CURLERROR_MSG("perform failed");
    }

    /* Return a tuple with the result and the number of running handles */
    return Py_BuildValue("(ii)", (int)res, running);
}


/* --------------- add_handle/remove_handle --------------- */

/* static utility function */
static int
check_multi_add_remove(const CurlMultiObject *self, const CurlObject *obj)
{
    /* check CurlMultiObject status */
    assert_multi_state(self);
    if (self->multi_handle == NULL) {
        PyErr_SetString(ErrorObject, "cannot add/remove handle - multi-stack is closed");
        return -1;
    }
#ifdef WITH_THREAD
    if (self->state != NULL) {
        PyErr_SetString(ErrorObject, "cannot add/remove handle - multi_perform() already running");
        return -1;
    }
#endif
    /* check CurlObject status */
    assert_curl_state(obj);
#ifdef WITH_THREAD
    if (obj->state != NULL) {
        PyErr_SetString(ErrorObject, "cannot add/remove handle - perform() of curl object already running");
        return -1;
    }
#endif
    if (obj->multi_stack != NULL && obj->multi_stack != self) {
        PyErr_SetString(ErrorObject, "cannot add/remove handle - curl object already on another multi-stack");
        return -1;
    }
    return 0;
}


static PyObject *
do_multi_add_handle(CurlMultiObject *self, PyObject *args)
{
    CurlObject *obj;
    CURLMcode res;

    if (!PyArg_ParseTuple(args, "O!:add_handle", p_Curl_Type, &obj)) {
        return NULL;
    }
    if (check_multi_add_remove(self, obj) != 0) {
        return NULL;
    }
    if (obj->handle == NULL) {
        PyErr_SetString(ErrorObject, "curl object already closed");
        return NULL;
    }
    if (obj->multi_stack == self) {
        PyErr_SetString(ErrorObject, "curl object already on this multi-stack");
        return NULL;
    }
    
    PyDict_SetItem(self->easy_object_dict, (PyObject *) obj, Py_True);
    
    assert(obj->multi_stack == NULL);
    /* Allow threads because callbacks can be invoked */
    PYCURL_BEGIN_ALLOW_THREADS
    res = curl_multi_add_handle(self->multi_handle, obj->handle);
    PYCURL_END_ALLOW_THREADS
    if (res != CURLM_OK) {
        PyDict_DelItem(self->easy_object_dict, (PyObject *) obj);
        CURLERROR_MSG("curl_multi_add_handle() failed due to internal errors");
    }
    obj->multi_stack = self;
    Py_INCREF(self);
    
    Py_RETURN_NONE;
}


static PyObject *
do_multi_remove_handle(CurlMultiObject *self, PyObject *args)
{
    CurlObject *obj;
    CURLMcode res;

    if (!PyArg_ParseTuple(args, "O!:remove_handle", p_Curl_Type, &obj)) {
        return NULL;
    }
    if (check_multi_add_remove(self, obj) != 0) {
        return NULL;
    }
    if (obj->handle == NULL) {
        /* CurlObject handle already closed -- ignore */
        if (PyDict_GetItem(self->easy_object_dict, (PyObject *) obj)) {
            PyDict_DelItem(self->easy_object_dict, (PyObject *) obj);
        }
        goto done;
    }
    if (obj->multi_stack != self) {
        PyErr_SetString(ErrorObject, "curl object not on this multi-stack");
        return NULL;
    }
    /* Allow threads because callbacks can be invoked */
    PYCURL_BEGIN_ALLOW_THREADS
    res = curl_multi_remove_handle(self->multi_handle, obj->handle);
    PYCURL_END_ALLOW_THREADS
    if (res == CURLM_OK) {
        PyDict_DelItem(self->easy_object_dict, (PyObject *) obj);
        // if PyDict_DelItem fails, remove_handle call will also fail.
        // but the dictionary should always have our object in it
        // hence this failure shouldn't happen unless something unaccounted
        // for went wrong
    } else {
        CURLERROR_MSG("curl_multi_remove_handle() failed due to internal errors");
    }
    assert(obj->multi_stack == self);
    obj->multi_stack = NULL;
    Py_DECREF(self);
done:
    Py_RETURN_NONE;
}


/* --------------- fdset ---------------------- */

static PyObject *
do_multi_fdset(CurlMultiObject *self, PyObject *Py_UNUSED(ignored))
{
    CURLMcode res;
    int max_fd = -1, fd;
    PyObject *ret = NULL;
    PyObject *read_list = NULL, *write_list = NULL, *except_list = NULL;
    PyObject *py_fd = NULL;

    if (check_multi_state(self, 1 | 2, "fdset") != 0) {
        return NULL;
    }

    /* Clear file descriptor sets */
    FD_ZERO(&self->read_fd_set);
    FD_ZERO(&self->write_fd_set);
    FD_ZERO(&self->exc_fd_set);

    /* Don't bother releasing the gil as this is just a data structure operation */
    res = curl_multi_fdset(self->multi_handle, &self->read_fd_set,
                           &self->write_fd_set, &self->exc_fd_set, &max_fd);
    if (res != CURLM_OK) {
        CURLERROR_MSG("curl_multi_fdset() failed due to internal errors");
    }

    /* Allocate lists. */
    if ((read_list = PyList_New((Py_ssize_t)0)) == NULL) goto error;
    if ((write_list = PyList_New((Py_ssize_t)0)) == NULL) goto error;
    if ((except_list = PyList_New((Py_ssize_t)0)) == NULL) goto error;

    /* Populate lists */
    for (fd = 0; fd < max_fd + 1; fd++) {
        if (FD_ISSET(fd, &self->read_fd_set)) {
            if ((py_fd = PyInt_FromLong((long)fd)) == NULL) goto error;
            if (PyList_Append(read_list, py_fd) != 0) goto error;
            Py_DECREF(py_fd);
            py_fd = NULL;
        }
        if (FD_ISSET(fd, &self->write_fd_set)) {
            if ((py_fd = PyInt_FromLong((long)fd)) == NULL) goto error;
            if (PyList_Append(write_list, py_fd) != 0) goto error;
            Py_DECREF(py_fd);
            py_fd = NULL;
        }
        if (FD_ISSET(fd, &self->exc_fd_set)) {
            if ((py_fd = PyInt_FromLong((long)fd)) == NULL) goto error;
            if (PyList_Append(except_list, py_fd) != 0) goto error;
            Py_DECREF(py_fd);
            py_fd = NULL;
        }
    }

    /* Return a tuple with the 3 lists */
    ret = Py_BuildValue("(OOO)", read_list, write_list, except_list);
error:
    Py_XDECREF(py_fd);
    Py_XDECREF(except_list);
    Py_XDECREF(write_list);
    Py_XDECREF(read_list);
    return ret;
}


/* --------------- info_read --------------- */

static PyObject *
do_multi_info_read(CurlMultiObject *self, PyObject *args)
{
    PyObject *ret = NULL;
    PyObject *ok_list = NULL, *err_list = NULL;
    CURLMsg *msg;
    int in_queue = 0, num_results = INT_MAX;

    /* Sanity checks */
    if (!PyArg_ParseTuple(args, "|i:info_read", &num_results)) {
        return NULL;
    }
    if (num_results <= 0) {
        PyErr_SetString(ErrorObject, "argument to info_read must be greater than zero");
        return NULL;
    }
    if (check_multi_state(self, 1 | 2, "info_read") != 0) {
        return NULL;
    }

    if ((ok_list = PyList_New((Py_ssize_t)0)) == NULL) goto error;
    if ((err_list = PyList_New((Py_ssize_t)0)) == NULL) goto error;

    /* Loop through up to 'num_results' messages */
    while (num_results-- > 0) {
        CURLcode res;
        CurlObject *co = NULL;

        if ((msg = curl_multi_info_read(self->multi_handle, &in_queue)) == NULL) {
            break;
        }

        /* Fetch the curl object that corresponds to the curl handle in the message */
        res = curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, (char **) &co);
        if (res != CURLE_OK || co == NULL) {
            Py_DECREF(err_list);
            Py_DECREF(ok_list);
            CURLERROR_MSG("Unable to fetch curl handle from curl object");
        }
        assert(PyObject_IsInstance((PyObject *) co, (PyObject *) p_Curl_Type) == 1);
        if (msg->msg != CURLMSG_DONE) {
            /* FIXME: what does this mean ??? */
        }
        if (msg->data.result == CURLE_OK) {
            /* Append curl object to list of objects which succeeded */
            if (PyList_Append(ok_list, (PyObject *)co) != 0) {
                goto error;
            }
        }
        else {
            /* Create a result tuple that will get added to err_list. */
            PyObject *error_str = NULL;
            PyObject *v;
#if PY_MAJOR_VERSION >= 3
            error_str = PyUnicode_DecodeLocale(co->error, "surrogateescape");
            if (error_str == NULL) {
                goto error;
            }
            v = Py_BuildValue("(OiO)", (PyObject *)co, (int)msg->data.result, error_str);
#else
            v = Py_BuildValue("(Ois)", (PyObject *)co, (int)msg->data.result, co->error);
#endif
            /* Append curl object to list of objects which failed */
            if (v == NULL || PyList_Append(err_list, v) != 0) {
                Py_XDECREF(error_str);
                Py_XDECREF(v);
                goto error;
            }
            Py_DECREF(v);
        }
    }
    /* Return (number of queued messages, [ok_objects], [error_objects]) */
    ret = Py_BuildValue("(iOO)", in_queue, ok_list, err_list);
error:
    Py_XDECREF(err_list);
    Py_XDECREF(ok_list);
    return ret;
}


/* --------------- select --------------- */

static PyObject *
do_multi_select(CurlMultiObject *self, PyObject *args)
{
    int max_fd = -1, n;
    double timeout = -1.0;
    struct timeval tv, *tvp;
    CURLMcode res;

    if (!PyArg_ParseTuple(args, "d:select", &timeout)) {
        return NULL;
    }
    if (check_multi_state(self, 1 | 2, "select") != 0) {
        return NULL;
    }

    if (timeout < 0 || timeout >= 365 * 24 * 60 * 60) {
        PyErr_SetString(PyExc_OverflowError, "invalid timeout period");
        return NULL;
    } else {
        long seconds = (long)timeout;
        timeout = timeout - (double)seconds;
        assert(timeout >= 0.0); assert(timeout < 1.0);
        tv.tv_sec = seconds;
        tv.tv_usec = (long)(timeout*1000000.0);
        tvp = &tv;
    }

    FD_ZERO(&self->read_fd_set);
    FD_ZERO(&self->write_fd_set);
    FD_ZERO(&self->exc_fd_set);

    res = curl_multi_fdset(self->multi_handle, &self->read_fd_set,
                           &self->write_fd_set, &self->exc_fd_set, &max_fd);
    if (res != CURLM_OK) {
        CURLERROR_MSG("multi_fdset failed");
    }

    if (max_fd < 0) {
        n = 0;
    }
    else {
        Py_BEGIN_ALLOW_THREADS
        n = select(max_fd + 1, &self->read_fd_set, &self->write_fd_set, &self->exc_fd_set, tvp);
        Py_END_ALLOW_THREADS
        /* info: like Python's socketmodule.c we do not raise an exception
         *       if select() fails - we'll leave it to the actual libcurl
         *       socket code to report any errors.
         */
    }

    return PyInt_FromLong(n);
}


static PyObject *do_curlmulti_getstate(CurlMultiObject *self, PyObject *Py_UNUSED(ignored))
{
    PyErr_SetString(PyExc_TypeError, "CurlMulti objects do not support serialization");
    return NULL;
}


static PyObject *do_curlmulti_setstate(CurlMultiObject *self, PyObject *args)
{
    PyErr_SetString(PyExc_TypeError, "CurlMulti objects do not support deserialization");
    return NULL;
}


/*************************************************************************
// type definitions
**************************************************************************/

/* --------------- methods --------------- */

PYCURL_INTERNAL PyMethodDef curlmultiobject_methods[] = {
    {"add_handle", (PyCFunction)do_multi_add_handle, METH_VARARGS, multi_add_handle_doc},
    {"close", (PyCFunction)do_multi_close, METH_NOARGS, multi_close_doc},
    {"fdset", (PyCFunction)do_multi_fdset, METH_NOARGS, multi_fdset_doc},
    {"info_read", (PyCFunction)do_multi_info_read, METH_VARARGS, multi_info_read_doc},
    {"perform", (PyCFunction)do_multi_perform, METH_NOARGS, multi_perform_doc},
    {"socket_action", (PyCFunction)do_multi_socket_action, METH_VARARGS, multi_socket_action_doc},
    {"socket_all", (PyCFunction)do_multi_socket_all, METH_NOARGS, multi_socket_all_doc},
    {"setopt", (PyCFunction)do_multi_setopt, METH_VARARGS, multi_setopt_doc},
    {"timeout", (PyCFunction)do_multi_timeout, METH_NOARGS, multi_timeout_doc},
    {"assign", (PyCFunction)do_multi_assign, METH_VARARGS, multi_assign_doc},
    {"remove_handle", (PyCFunction)do_multi_remove_handle, METH_VARARGS, multi_remove_handle_doc},
    {"select", (PyCFunction)do_multi_select, METH_VARARGS, multi_select_doc},
    {"__getstate__", (PyCFunction)do_curlmulti_getstate, METH_NOARGS, NULL},
    {"__setstate__", (PyCFunction)do_curlmulti_setstate, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};


/* --------------- setattr/getattr --------------- */


#if PY_MAJOR_VERSION >= 3

PYCURL_INTERNAL PyObject *
do_multi_getattro(PyObject *o, PyObject *n)
{
    PyObject *v;
    assert_multi_state((CurlMultiObject *)o);
    v = PyObject_GenericGetAttr(o, n);
    if( !v && PyErr_ExceptionMatches(PyExc_AttributeError) )
    {
        PyErr_Clear();
        v = my_getattro(o, n, ((CurlMultiObject *)o)->dict,
                        curlmultiobject_constants, curlmultiobject_methods);
    }
    return v;
}

PYCURL_INTERNAL int
do_multi_setattro(PyObject *o, PyObject *n, PyObject *v)
{
    assert_multi_state((CurlMultiObject *)o);
    return my_setattro(&((CurlMultiObject *)o)->dict, n, v);
}

#else /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL PyObject *
do_multi_getattr(CurlMultiObject *co, char *name)
{
    assert_multi_state(co);
    return my_getattr((PyObject *)co, name, co->dict,
                      curlmultiobject_constants, curlmultiobject_methods);
}

PYCURL_INTERNAL int
do_multi_setattr(CurlMultiObject *co, char *name, PyObject *v)
{
    assert_multi_state(co);
    return my_setattr(&co->dict, name, v);
}

#endif /* PY_MAJOR_VERSION >= 3 */

PYCURL_INTERNAL PyTypeObject CurlMulti_Type = {
#if PY_MAJOR_VERSION >= 3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                          /* ob_size */
#endif
    "pycurl.CurlMulti",         /* tp_name */
    sizeof(CurlMultiObject),    /* tp_basicsize */
    0,                          /* tp_itemsize */
    (destructor)do_multi_dealloc, /* tp_dealloc */
    0,                          /* tp_print */
#if PY_MAJOR_VERSION >= 3
    0,                          /* tp_getattr */
    0,                          /* tp_setattr */
#else
    (getattrfunc)do_multi_getattr,  /* tp_getattr */
    (setattrfunc)do_multi_setattr,  /* tp_setattr */
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
    (getattrofunc)do_multi_getattro, /* tp_getattro */
    (setattrofunc)do_multi_setattro, /* tp_setattro */
#else
    0,                          /* tp_getattro */
    0,                          /* tp_setattro */
#endif
    0,                          /* tp_as_buffer */
    PYCURL_TYPE_FLAGS,          /* tp_flags */
    multi_doc,                   /* tp_doc */
    (traverseproc)do_multi_traverse, /* tp_traverse */
    (inquiry)do_multi_clear,    /* tp_clear */
    0,                          /* tp_richcompare */
    offsetof(CurlMultiObject, weakreflist), /* tp_weaklistoffset */
    0,                          /* tp_iter */
    0,                          /* tp_iternext */
    curlmultiobject_methods,    /* tp_methods */
    0,                          /* tp_members */
    0,                          /* tp_getset */
    0,                          /* tp_base */
    0,                          /* tp_dict */
    0,                          /* tp_descr_get */
    0,                          /* tp_descr_set */
    0,                          /* tp_dictoffset */
    0,                          /* tp_init */
    PyType_GenericAlloc,        /* tp_alloc */
    (newfunc)do_multi_new,      /* tp_new */
    PyObject_GC_Del,            /* tp_free */
};

/* vi:ts=4:et:nowrap
 */
