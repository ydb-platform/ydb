#include "pycurl.h"


/* IMPORTANT NOTE: due to threading issues, we cannot call _any_ Python
 * function without acquiring the thread state in the callback handlers.
 */


static size_t
util_write_callback(int flags, char *ptr, size_t size, size_t nmemb, void *stream)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    size_t ret = 0;     /* assume error */
    PyObject *cb;
    int total_size;
    PYCURL_DECLARE_THREAD_STATE;

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "util_write_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check args */
    cb = flags ? self->h_cb : self->w_cb;
    if (cb == NULL)
        goto silent_error;
    if (size <= 0 || nmemb <= 0)
        goto done;
    total_size = (int)(size * nmemb);
    if (total_size < 0 || (size_t)total_size / size != nmemb) {
        PyErr_SetString(ErrorObject, "integer overflow in write callback");
        goto verbose_error;
    }

    /* run callback */
#if PY_MAJOR_VERSION >= 3
    arglist = Py_BuildValue("(y#)", ptr, total_size);
#else
    arglist = Py_BuildValue("(s#)", ptr, total_size);
#endif
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* handle result */
    if (result == Py_None) {
        ret = total_size;           /* None means success */
    }
    else if (PyInt_Check(result) || PyLong_Check(result)) {
        /* if the cast to long fails, PyLong_AsLong() returns -1L */
        ret = (size_t) PyLong_AsLong(result);
    }
    else {
        PyErr_SetString(ErrorObject, "write callback must return int or None");
        goto verbose_error;
    }

done:
silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


PYCURL_INTERNAL size_t
write_callback(char *ptr, size_t size, size_t nmemb, void *stream)
{
    return util_write_callback(0, ptr, size, nmemb, stream);
}

PYCURL_INTERNAL size_t
header_callback(char *ptr, size_t size, size_t nmemb, void *stream)
{
    return util_write_callback(1, ptr, size, nmemb, stream);
}


/* convert protocol address from C to python, returns a tuple of protocol
   specific values */
static PyObject *
convert_protocol_address(struct sockaddr* saddr, unsigned int saddrlen)
{
    PyObject *res_obj = NULL;

    switch (saddr->sa_family)
    {
    case AF_INET:
        {
            struct sockaddr_in* sin = (struct sockaddr_in*)saddr;
            char *addr_str = PyMem_New(char, INET_ADDRSTRLEN);

            if (addr_str == NULL) {
                PyErr_NoMemory();
                goto error;
            }

            if (inet_ntop(saddr->sa_family, &sin->sin_addr, addr_str, INET_ADDRSTRLEN) == NULL) {
                PyErr_SetFromErrno(ErrorObject);
                PyMem_Free(addr_str);
                goto error;
            }
            res_obj = Py_BuildValue("(si)", addr_str, ntohs(sin->sin_port));
            PyMem_Free(addr_str);
       }
        break;
    case AF_INET6:
        {
            struct sockaddr_in6* sin6 = (struct sockaddr_in6*)saddr;
            char *addr_str = PyMem_New(char, INET6_ADDRSTRLEN);

            if (addr_str == NULL) {
                PyErr_NoMemory();
                goto error;
            }

            if (inet_ntop(saddr->sa_family, &sin6->sin6_addr, addr_str, INET6_ADDRSTRLEN) == NULL) {
                PyErr_SetFromErrno(ErrorObject);
                PyMem_Free(addr_str);
                goto error;
            }
            res_obj = Py_BuildValue("(siii)", addr_str, (int) ntohs(sin6->sin6_port),
                (int) ntohl(sin6->sin6_flowinfo), (int) ntohl(sin6->sin6_scope_id));
            PyMem_Free(addr_str);
        }
        break;
#if !defined(WIN32)
    case AF_UNIX:
        {
            struct sockaddr_un* s_un = (struct sockaddr_un*)saddr;

#if PY_MAJOR_VERSION >= 3
            res_obj = Py_BuildValue("y", s_un->sun_path);
#else
            res_obj = Py_BuildValue("s", s_un->sun_path);
#endif
        }
        break;
#endif
    default:
        /* We (currently) only support IPv4/6 addresses.  Can curl even be used
           with anything else? */
        PyErr_SetString(ErrorObject, "Unsupported address family");
    }

error:
    return res_obj;
}


/* curl_socket_t is just an int on unix/windows (with limitations that
 * are not important here) */
PYCURL_INTERNAL curl_socket_t
opensocket_callback(void *clientp, curlsocktype purpose,
                    struct curl_sockaddr *address)
{
    PyObject *arglist;
    PyObject *result = NULL;
    PyObject *fileno_result = NULL;
    CurlObject *self;
    int ret = CURL_SOCKET_BAD;
    PyObject *converted_address;
    PyObject *python_address;
    PYCURL_DECLARE_THREAD_STATE;

    self = (CurlObject *)clientp;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "opensocket_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    converted_address = convert_protocol_address(&address->addr, address->addrlen);
    if (converted_address == NULL) {
        goto verbose_error;
    }

    arglist = Py_BuildValue("(iiiN)", address->family, address->socktype, address->protocol, converted_address);
    if (arglist == NULL) {
        Py_DECREF(converted_address);
        goto verbose_error;
    }
    python_address = PyObject_Call(curl_sockaddr_type, arglist, NULL);
    Py_DECREF(arglist);
    if (python_address == NULL) {
        goto verbose_error;
    }

    arglist = Py_BuildValue("(iN)", purpose, python_address);
    if (arglist == NULL) {
        Py_DECREF(python_address);
        goto verbose_error;
    }
    result = PyObject_Call(self->opensocket_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL) {
        goto verbose_error;
    }

    if (PyInt_Check(result) && PyInt_AsLong(result) == CURL_SOCKET_BAD) {
        ret = CURL_SOCKET_BAD;
    } else if (PyObject_HasAttrString(result, "fileno")) {
        fileno_result = PyObject_CallMethod(result, "fileno", NULL);

        if (fileno_result == NULL) {
            ret = CURL_SOCKET_BAD;
            goto verbose_error;
        }
        // normal operation:
        if (PyInt_Check(fileno_result)) {
            int sock_fd = PyInt_AsLong(fileno_result);
#if defined(WIN32)
            ret = dup_winsock(sock_fd, address);
#else
            ret = dup(sock_fd);
#endif
            goto done;
        } else {
            PyErr_SetString(ErrorObject, "Open socket callback returned an object whose fileno method did not return an integer");
            ret = CURL_SOCKET_BAD;
        }
    } else {
        PyErr_SetString(ErrorObject, "Open socket callback's return value must be a socket");
        ret = CURL_SOCKET_BAD;
        goto verbose_error;
    }

silent_error:
done:
    Py_XDECREF(result);
    Py_XDECREF(fileno_result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


PYCURL_INTERNAL int
sockopt_cb(void *clientp, curl_socket_t curlfd, curlsocktype purpose)
{
    PyObject *arglist;
    CurlObject *self;
    int ret = CURL_SOCKOPT_ERROR;
    PyObject *ret_obj = NULL;
    PYCURL_DECLARE_THREAD_STATE;

    self = (CurlObject *)clientp;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "sockopt_cb failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    arglist = Py_BuildValue("(ii)", (int) curlfd, (int) purpose);
    if (arglist == NULL)
        goto verbose_error;

    ret_obj = PyObject_Call(self->sockopt_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (!PyInt_Check(ret_obj) && !PyLong_Check(ret_obj)) {
        PyObject *ret_repr = PyObject_Repr(ret_obj);
        if (ret_repr) {
            PyObject *encoded_obj;
            char *str = PyText_AsString_NoNUL(ret_repr, &encoded_obj);
            fprintf(stderr, "sockopt callback returned %s which is not an integer\n", str);
            /* PyErr_Format(PyExc_TypeError, "sockopt callback returned %s which is not an integer", str); */
            Py_XDECREF(encoded_obj);
            Py_DECREF(ret_repr);
        }
        goto silent_error;
    }
    if (PyInt_Check(ret_obj)) {
        /* long to int cast */
        ret = (int) PyInt_AsLong(ret_obj);
    } else {
        /* long to int cast */
        ret = (int) PyLong_AsLong(ret_obj);
    }
    goto done;

silent_error:
    ret = -1;
done:
    Py_XDECREF(ret_obj);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
PYCURL_INTERNAL int
closesocket_callback(void *clientp, curl_socket_t curlfd)
{
    PyObject *arglist;
    CurlObject *self;
    int ret = 1;
    PyObject *ret_obj = NULL;
    PYCURL_DECLARE_THREAD_STATE;

    self = (CurlObject *)clientp;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "closesocket_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    arglist = Py_BuildValue("(i)", (int) curlfd);
    if (arglist == NULL)
        goto verbose_error;

    ret_obj = PyObject_Call(self->closesocket_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (!ret_obj)
       goto silent_error;
    if (!PyInt_Check(ret_obj) && !PyLong_Check(ret_obj)) {
        PyObject *ret_repr = PyObject_Repr(ret_obj);
        if (ret_repr) {
            PyObject *encoded_obj;
            char *str = PyText_AsString_NoNUL(ret_repr, &encoded_obj);
            fprintf(stderr, "closesocket callback returned %s which is not an integer\n", str);
            /* PyErr_Format(PyExc_TypeError, "closesocket callback returned %s which is not an integer", str); */
            Py_XDECREF(encoded_obj);
            Py_DECREF(ret_repr);
        }
        goto silent_error;
    }
    if (PyInt_Check(ret_obj)) {
        /* long to int cast */
        ret = (int) PyInt_AsLong(ret_obj);
    } else {
        /* long to int cast */
        ret = (int) PyLong_AsLong(ret_obj);
    }
    goto done;

silent_error:
    ret = -1;
done:
    Py_XDECREF(ret_obj);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}
#endif


#ifdef HAVE_CURL_7_19_6_OPTS
static PyObject *
khkey_to_object(const struct curl_khkey *khkey)
{
    PyObject *arglist, *ret;

    if (khkey == NULL) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (khkey->len) {
#if PY_MAJOR_VERSION >= 3
        arglist = Py_BuildValue("(y#i)", khkey->key, khkey->len, khkey->keytype);
#else
        arglist = Py_BuildValue("(s#i)", khkey->key, khkey->len, khkey->keytype);
#endif
    } else {
#if PY_MAJOR_VERSION >= 3
        arglist = Py_BuildValue("(yi)", khkey->key, khkey->keytype);
#else
        arglist = Py_BuildValue("(si)", khkey->key, khkey->keytype);
#endif
    }

    if (arglist == NULL) {
        return NULL;
    }

    ret = PyObject_Call(khkey_type, arglist, NULL);
    Py_DECREF(arglist);
    return ret;
}


PYCURL_INTERNAL int
ssh_key_cb(CURL *easy, const struct curl_khkey *knownkey,
    const struct curl_khkey *foundkey, int khmatch, void *clientp)
{
    PyObject *arglist;
    CurlObject *self;
    int ret = CURLKHSTAT_REJECT;
    PyObject *knownkey_obj = NULL;
    PyObject *foundkey_obj = NULL;
    PyObject *ret_obj = NULL;
    PYCURL_DECLARE_THREAD_STATE;

    self = (CurlObject *)clientp;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "ssh_key_cb failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    knownkey_obj = khkey_to_object(knownkey);
    if (knownkey_obj == NULL) {
        goto silent_error;
    }
    foundkey_obj = khkey_to_object(foundkey);
    if (foundkey_obj == NULL) {
        goto silent_error;
    }

    arglist = Py_BuildValue("(OOi)", knownkey_obj, foundkey_obj, khmatch);
    if (arglist == NULL)
        goto verbose_error;

    ret_obj = PyObject_Call(self->ssh_key_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (!PyInt_Check(ret_obj) && !PyLong_Check(ret_obj)) {
        PyObject *ret_repr = PyObject_Repr(ret_obj);
        if (ret_repr) {
            PyObject *encoded_obj;
            char *str = PyText_AsString_NoNUL(ret_repr, &encoded_obj);
            fprintf(stderr, "ssh key callback returned %s which is not an integer\n", str);
            /* PyErr_Format(PyExc_TypeError, "ssh key callback returned %s which is not an integer", str); */
            Py_XDECREF(encoded_obj);
            Py_DECREF(ret_repr);
        }
        goto silent_error;
    }
    if (PyInt_Check(ret_obj)) {
        /* long to int cast */
        ret = (int) PyInt_AsLong(ret_obj);
    } else {
        /* long to int cast */
        ret = (int) PyLong_AsLong(ret_obj);
    }
    goto done;

silent_error:
    ret = -1;
done:
    Py_XDECREF(knownkey_obj);
    Py_XDECREF(foundkey_obj);
    Py_XDECREF(ret_obj);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}
#endif


PYCURL_INTERNAL int
seek_callback(void *stream, curl_off_t offset, int origin)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    int ret = 2;     /* assume error 2 (can't seek, libcurl free to work around). */
    PyObject *cb;
    int source = 0;     /* assume beginning */
    PYCURL_DECLARE_THREAD_STATE;

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "seek_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check arguments */
    switch (origin)
    {
      case SEEK_SET:
          source = 0;
          break;
      case SEEK_CUR:
          source = 1;
          break;
      case SEEK_END:
          source = 2;
          break;
      default:
          source = origin;
          break;
    }

    /* run callback */
    cb = self->seek_cb;
    if (cb == NULL)
        goto silent_error;
    arglist = Py_BuildValue("(L,i)", (PY_LONG_LONG) offset, source);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* handle result */
    if (result == Py_None) {
        ret = 0;           /* None means success */
    }
    else if (PyInt_Check(result)) {
        int ret_code = PyInt_AsLong(result);
        if (ret_code < 0 || ret_code > 2) {
            PyErr_Format(ErrorObject, "invalid return value for seek callback %d not in (0, 1, 2)", ret_code);
            goto verbose_error;
        }
        ret = ret_code;    /* pass the return code from the callback */
    }
    else {
        PyErr_SetString(ErrorObject, "seek callback must return 0 (CURL_SEEKFUNC_OK), 1 (CURL_SEEKFUNC_FAIL), 2 (CURL_SEEKFUNC_CANTSEEK) or None");
        goto verbose_error;
    }

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}




PYCURL_INTERNAL size_t
read_callback(char *ptr, size_t size, size_t nmemb, void *stream)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;

    size_t ret = CURL_READFUNC_ABORT;     /* assume error, this actually works */
    int total_size;

    PYCURL_DECLARE_THREAD_STATE;

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "read_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check args */
    if (self->r_cb == NULL)
        goto silent_error;
    if (size <= 0 || nmemb <= 0)
        goto done;
    total_size = (int)(size * nmemb);
    if (total_size < 0 || (size_t)total_size / size != nmemb) {
        PyErr_SetString(ErrorObject, "integer overflow in read callback");
        goto verbose_error;
    }

    /* run callback */
    arglist = Py_BuildValue("(i)", total_size);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->r_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* handle result */
    if (PyByteStr_Check(result)) {
        char *buf = NULL;
        Py_ssize_t obj_size = -1;
        Py_ssize_t r;
        r = PyByteStr_AsStringAndSize(result, &buf, &obj_size);
        if (r != 0 || obj_size < 0 || obj_size > total_size) {
            PyErr_Format(ErrorObject, "invalid return value for read callback (%ld bytes returned when at most %ld bytes were wanted)", (long)obj_size, (long)total_size);
            goto verbose_error;
        }
        memcpy(ptr, buf, obj_size);
        ret = obj_size;             /* success */
    }
    else if (PyUnicode_Check(result)) {
        char *buf = NULL;
        Py_ssize_t obj_size = -1;
        Py_ssize_t r;
        /*
        Encode with ascii codec.

        HTTP requires sending content-length for request body to the server
        before the request body is sent, therefore typically content length
        is given via POSTFIELDSIZE before read function is invoked to
        provide the data.

        However, if we encode the string using any encoding other than ascii,
        the length of encoded string may not match the length of unicode
        string we are encoding. Therefore, if client code does a simple
        len(source_string) to determine the value to supply in content-length,
        the length of bytes read may be different.

        To avoid this situation, we only accept ascii bytes in the string here.

        Encode data yourself to bytes when dealing with non-ascii data.
        */
        PyObject *encoded = PyUnicode_AsEncodedString(result, "ascii", "strict");
        if (encoded == NULL) {
            goto verbose_error;
        }
        r = PyByteStr_AsStringAndSize(encoded, &buf, &obj_size);
        if (r != 0 || obj_size < 0 || obj_size > total_size) {
            Py_DECREF(encoded);
            PyErr_Format(ErrorObject, "invalid return value for read callback (%ld bytes returned after encoding to utf-8 when at most %ld bytes were wanted)", (long)obj_size, (long)total_size);
            goto verbose_error;
        }
        memcpy(ptr, buf, obj_size);
        Py_DECREF(encoded);
        ret = obj_size;             /* success */
    }
#if PY_MAJOR_VERSION < 3
    else if (PyInt_Check(result)) {
        long r = PyInt_AsLong(result);
        if (r != CURL_READFUNC_ABORT && r != CURL_READFUNC_PAUSE)
            goto type_error;
        ret = r; /* either CURL_READFUNC_ABORT or CURL_READFUNC_PAUSE */
    }
#endif
    else if (PyLong_Check(result)) {
        long r = PyLong_AsLong(result);
        if (r != CURL_READFUNC_ABORT && r != CURL_READFUNC_PAUSE)
            goto type_error;
        ret = r; /* either CURL_READFUNC_ABORT or CURL_READFUNC_PAUSE */
    }
    else {
    type_error:
        PyErr_SetString(ErrorObject, "read callback must return a byte string or Unicode string with ASCII code points only");
        goto verbose_error;
    }

done:
silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


PYCURL_INTERNAL int
progress_callback(void *stream,
                  double dltotal, double dlnow, double ultotal, double ulnow)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    int ret = 1;       /* assume error */
    PYCURL_DECLARE_THREAD_STATE;

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "progress_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check args */
    if (self->pro_cb == NULL)
        goto silent_error;

    /* run callback */
    arglist = Py_BuildValue("(dddd)", dltotal, dlnow, ultotal, ulnow);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->pro_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* handle result */
    if (result == Py_None) {
        ret = 0;        /* None means success */
    }
    else if (PyInt_Check(result)) {
        ret = (int) PyInt_AsLong(result);
    }
    else {
        ret = PyObject_IsTrue(result);  /* FIXME ??? */
    }

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
PYCURL_INTERNAL int
xferinfo_callback(void *stream,
    curl_off_t dltotal, curl_off_t dlnow,
    curl_off_t ultotal, curl_off_t ulnow)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    int ret = 1;       /* assume error */
    PYCURL_DECLARE_THREAD_STATE;

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "xferinfo_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check args */
    if (self->xferinfo_cb == NULL)
        goto silent_error;

    /* run callback */
    arglist = Py_BuildValue("(LLLL)",
        (PY_LONG_LONG) dltotal, (PY_LONG_LONG) dlnow,
        (PY_LONG_LONG) ultotal, (PY_LONG_LONG) ulnow);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->xferinfo_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* handle result */
    if (result == Py_None) {
        ret = 0;        /* None means success */
    }
    else if (PyInt_Check(result)) {
        ret = (int) PyInt_AsLong(result);
    }
    else {
        ret = PyObject_IsTrue(result);  /* FIXME ??? */
    }

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}
#endif


PYCURL_INTERNAL int
debug_callback(CURL *curlobj, curl_infotype type,
               char *buffer, size_t total_size, void *stream)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    int ret = 0;       /* always success */
    PYCURL_DECLARE_THREAD_STATE;

    UNUSED(curlobj);

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "debug_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    /* check args */
    if (self->debug_cb == NULL)
        goto silent_error;
    if ((int)total_size < 0 || (size_t)((int)total_size) != total_size) {
        PyErr_SetString(ErrorObject, "integer overflow in debug callback");
        goto verbose_error;
    }

    /* run callback */
#if PY_MAJOR_VERSION >= 3
    arglist = Py_BuildValue("(iy#)", (int)type, buffer, (int)total_size);
#else
    arglist = Py_BuildValue("(is#)", (int)type, buffer, (int)total_size);
#endif
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->debug_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* return values from debug callbacks should be ignored */

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


PYCURL_INTERNAL curlioerr
ioctl_callback(CURL *curlobj, int cmd, void *stream)
{
    CurlObject *self;
    PyObject *arglist;
    PyObject *result = NULL;
    int ret = CURLIOE_FAILRESTART;       /* assume error */
    PYCURL_DECLARE_THREAD_STATE;

    UNUSED(curlobj);

    /* acquire thread */
    self = (CurlObject *)stream;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "ioctl_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return (curlioerr) ret;
    }

    /* check args */
    if (self->ioctl_cb == NULL)
        goto silent_error;

    /* run callback */
    arglist = Py_BuildValue("(i)", cmd);
    if (arglist == NULL)
        goto verbose_error;
    result = PyObject_Call(self->ioctl_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (result == NULL)
        goto verbose_error;

    /* handle result */
    if (result == Py_None) {
        ret = CURLIOE_OK;        /* None means success */
    }
    else if (PyInt_Check(result)) {
        ret = (int) PyInt_AsLong(result);
        if (ret >= CURLIOE_LAST || ret < 0) {
            PyErr_SetString(ErrorObject, "ioctl callback returned invalid value");
            goto verbose_error;
        }
    }

silent_error:
    Py_XDECREF(result);
    PYCURL_RELEASE_THREAD();
    return (curlioerr) ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}


#if defined(HAVE_CURL_OPENSSL)
/* internal helper that load certificates from buffer, returns -1 on error  */
static int
add_ca_certs(SSL_CTX *context, void *data, Py_ssize_t len)
{
    // this code was copied from _ssl module
    BIO *biobuf = NULL;
    X509_STORE *store;
    int retval = 0, err, loaded = 0;

    if (len <= 0) {
        PyErr_SetString(PyExc_ValueError,
                        "Empty certificate data");
        return -1;
    } else if (len > INT_MAX) {
        PyErr_SetString(PyExc_OverflowError,
                        "Certificate data is too long.");
        return -1;
    }

    biobuf = BIO_new_mem_buf(data, (int)len);
    if (biobuf == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Can't allocate buffer");
        ERR_clear_error();
        return -1;
    }

    store = SSL_CTX_get_cert_store(context);
    assert(store != NULL);

    while (1) {
        X509 *cert = NULL;
        int r;

        cert = PEM_read_bio_X509(biobuf, NULL, 0, NULL);
        if (cert == NULL) {
            break;
        }
        r = X509_STORE_add_cert(store, cert);
        X509_free(cert);
        if (!r) {
            err = ERR_peek_last_error();
            if ((ERR_GET_LIB(err) == ERR_LIB_X509) &&
                (ERR_GET_REASON(err) == X509_R_CERT_ALREADY_IN_HASH_TABLE)) {
                /* cert already in hash table, not an error */
                ERR_clear_error();
            } else {
                break;
            }
        }
        loaded++;
    }

    err = ERR_peek_last_error();
    if ((loaded > 0) &&
            (ERR_GET_LIB(err) == ERR_LIB_PEM) &&
            (ERR_GET_REASON(err) == PEM_R_NO_START_LINE)) {
        /* EOF PEM file, not an error */
        ERR_clear_error();
        retval = 0;
    } else {
        PyErr_SetString(ErrorObject, ERR_reason_error_string(err));
        ERR_clear_error();
        retval = -1;
    }

    BIO_free(biobuf);
    return retval;
}


PYCURL_INTERNAL CURLcode
ssl_ctx_callback(CURL *curl, void *ssl_ctx, void *ptr)
{
    CurlObject *self;
    PYCURL_DECLARE_THREAD_STATE;
    int r;

    UNUSED(curl);

    /* acquire thread */
    self = (CurlObject *)ptr;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "ssl_ctx_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return CURLE_FAILED_INIT;
    }

    r = add_ca_certs((SSL_CTX*)ssl_ctx,
                         PyBytes_AS_STRING(self->ca_certs_obj),
                         PyBytes_GET_SIZE(self->ca_certs_obj));

    if (r != 0)
        PyErr_Print();

    PYCURL_RELEASE_THREAD();
    return r == 0 ? CURLE_OK : CURLE_FAILED_INIT;
}
#endif


#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 80, 0)
PYCURL_INTERNAL int
prereq_callback(void *clientp, char *conn_primary_ip, char *conn_local_ip,
                int conn_primary_port, int conn_local_port)
{
    PyObject *arglist;
    CurlObject *self;
    int ret = CURL_PREREQFUNC_ABORT;
    PyObject *ret_obj = NULL;
    PYCURL_DECLARE_THREAD_STATE;

    self = (CurlObject *)clientp;
    if (!PYCURL_ACQUIRE_THREAD()) {
        PyGILState_STATE tmp_warn_state = PyGILState_Ensure();
        PyErr_WarnEx(PyExc_RuntimeWarning, "prereq_callback failed to acquire thread", 1);
        PyGILState_Release(tmp_warn_state);
        return ret;
    }

    arglist = Py_BuildValue("(ssii)", conn_primary_ip, conn_local_ip, conn_primary_port, conn_local_port);
    if (arglist == NULL)
        goto verbose_error;

    ret_obj = PyObject_Call(self->prereq_cb, arglist, NULL);
    Py_DECREF(arglist);
    if (!ret_obj)
       goto silent_error;
    if (!PyInt_Check(ret_obj) && !PyLong_Check(ret_obj)) {
        PyObject *ret_repr = PyObject_Repr(ret_obj);
        if (ret_repr) {
            PyObject *encoded_obj;
            char *str = PyText_AsString_NoNUL(ret_repr, &encoded_obj);
            fprintf(stderr, "prereq callback returned %s which is not an integer\n", str);
            /* PyErr_Format(PyExc_TypeError, "prereq callback returned %s which is not an integer", str); */
            Py_XDECREF(encoded_obj);
            Py_DECREF(ret_repr);
        }
        goto silent_error;
    }
    if (PyInt_Check(ret_obj)) {
        /* long to int cast */
        ret = (int) PyInt_AsLong(ret_obj);
    } else {
        /* long to int cast */
        ret = (int) PyLong_AsLong(ret_obj);
    }
    goto done;

silent_error:
    ret = -1;
done:
    Py_XDECREF(ret_obj);
    PYCURL_RELEASE_THREAD();
    return ret;
verbose_error:
    PyErr_Print();
    goto silent_error;
}
#endif
