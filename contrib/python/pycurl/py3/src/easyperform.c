#include "pycurl.h"


/* --------------- perform --------------- */

PYCURL_INTERNAL PyObject *
do_curl_perform(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    int res;

    if (check_curl_state(self, 1 | 2, "perform") != 0) {
        return NULL;
    }

    PYCURL_BEGIN_ALLOW_THREADS
    res = curl_easy_perform(self->handle);
    PYCURL_END_ALLOW_THREADS

    if (res != CURLE_OK) {
        CURLERROR_RETVAL();
    }
    Py_RETURN_NONE;
}


PYCURL_INTERNAL PyObject *
do_curl_perform_rb(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    PyObject *v, *io;
    
    /* NOTE: this tuple is never freed. */
    static PyObject *empty_tuple = NULL;
    
    if (empty_tuple == NULL) {
        empty_tuple = PyTuple_New(0);
        if (empty_tuple == NULL) {
            return NULL;
        }
    }
    
    io = PyObject_Call(bytesio, empty_tuple, NULL);
    if (io == NULL) {
        return NULL;
    }
    
    v = do_curl_setopt_filelike(self, CURLOPT_WRITEDATA, io);
    if (v == NULL) {
        Py_DECREF(io);
        return NULL;
    }
    
    v = do_curl_perform(self, NULL);
    if (v == NULL) {
        return NULL;
    }
    
    v = PyObject_CallMethod(io, "getvalue", NULL);
    Py_DECREF(io);
    return v;
}

#if PY_MAJOR_VERSION >= 3
PYCURL_INTERNAL PyObject *
do_curl_perform_rs(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    PyObject *v, *decoded;
    
    v = do_curl_perform_rb(self, NULL);
    if (v == NULL) {
        return NULL;
    }
    
    decoded = PyUnicode_FromEncodedObject(v, NULL, NULL);
    Py_DECREF(v);
    return decoded;
}
#endif


/* --------------- pause --------------- */


/* curl_easy_pause() can be called from inside a callback or outside */
PYCURL_INTERNAL PyObject *
do_curl_pause(CurlObject *self, PyObject *args)
{
    int bitmask;
    CURLcode res;
#ifdef WITH_THREAD
    PyThreadState *saved_state;
#endif

    if (!PyArg_ParseTuple(args, "i:pause", &bitmask)) {
        return NULL;
    }
    if (check_curl_state(self, 1, "pause") != 0) {
        return NULL;
    }

#ifdef WITH_THREAD
    /* Save handle to current thread (used as context for python callbacks) */
    saved_state = self->state;
    PYCURL_BEGIN_ALLOW_THREADS_EASY

    /* We must allow threads here because unpausing a handle can cause
       some of its callbacks to be invoked immediately, from inside
       curl_easy_pause() */
#endif

    res = curl_easy_pause(self->handle, bitmask);

#ifdef WITH_THREAD
    PYCURL_END_ALLOW_THREADS_EASY

    /* Restore the thread-state to whatever it was on entry */
    self->state = saved_state;
#endif

    if (res != CURLE_OK) {
        CURLERROR_MSG("pause/unpause failed");
    } else {
        Py_INCREF(Py_None);
        return Py_None;
    }
}
