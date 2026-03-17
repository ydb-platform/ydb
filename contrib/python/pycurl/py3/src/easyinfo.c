#include "pycurl.h"


/* Convert a curl slist (a list of strings) to a Python list.
 * In case of error return NULL with an exception set.
 */
static PyObject *convert_slist(struct curl_slist *slist, int free_flags)
{
    PyObject *ret = NULL;
    struct curl_slist *slist_start = slist;

    ret = PyList_New((Py_ssize_t)0);
    if (ret == NULL) goto error;

    for ( ; slist != NULL; slist = slist->next) {
        PyObject *v = NULL;

        if (slist->data == NULL) {
            v = Py_None; Py_INCREF(v);
        } else {
            v = PyByteStr_FromString(slist->data);
        }
        if (v == NULL || PyList_Append(ret, v) != 0) {
            Py_XDECREF(v);
            goto error;
        }
        Py_DECREF(v);
    }

    if ((free_flags & 1) && slist_start)
        curl_slist_free_all(slist_start);
    return ret;

error:
    Py_XDECREF(ret);
    if ((free_flags & 2) && slist_start)
        curl_slist_free_all(slist_start);
    return NULL;
}


#ifdef HAVE_CURLOPT_CERTINFO
/* Convert a struct curl_certinfo into a Python data structure.
 * In case of error return NULL with an exception set.
 */
static PyObject *convert_certinfo(struct curl_certinfo *cinfo, int decode)
{
    PyObject *certs;
    int cert_index;

    if (!cinfo)
        Py_RETURN_NONE;

    certs = PyList_New((Py_ssize_t)(cinfo->num_of_certs));
    if (!certs)
        return NULL;

    for (cert_index = 0; cert_index < cinfo->num_of_certs; cert_index ++) {
        struct curl_slist *fields = cinfo->certinfo[cert_index];
        struct curl_slist *field_cursor;
        int field_count, field_index;
        PyObject *cert;

        field_count = 0;
        field_cursor = fields;
        while (field_cursor != NULL) {
            field_cursor = field_cursor->next;
            field_count ++;
        }


        cert = PyTuple_New((Py_ssize_t)field_count);
        if (!cert)
            goto error;
        PyList_SetItem(certs, cert_index, cert); /* Eats the ref from New() */

        for(field_index = 0, field_cursor = fields;
            field_cursor != NULL;
            field_index ++, field_cursor = field_cursor->next) {
            const char *field = field_cursor->data;
            PyObject *field_tuple;

            if (!field) {
                field_tuple = Py_None; Py_INCREF(field_tuple);
            } else {
                const char *sep = strchr(field, ':');
                if (!sep) {
                    if (decode) {
                        field_tuple = PyText_FromString(field);
                    } else {
                        field_tuple = PyByteStr_FromString(field);
                    }
                } else {
                    /* XXX check */
                    if (decode) {
                        field_tuple = Py_BuildValue("s#s", field, (int)(sep - field), sep+1);
                    } else {
#if PY_MAJOR_VERSION >= 3
                        field_tuple = Py_BuildValue("y#y", field, (int)(sep - field), sep+1);
#else
                        field_tuple = Py_BuildValue("s#s", field, (int)(sep - field), sep+1);
#endif
                    }
                }
                if (!field_tuple)
                    goto error;
            }
            PyTuple_SET_ITEM(cert, field_index, field_tuple); /* Eats the ref */
        }
    }

    return certs;

 error:
    Py_DECREF(certs);
    return NULL;
}
#endif

PYCURL_INTERNAL PyObject *
do_curl_getinfo_raw(CurlObject *self, PyObject *args)
{
    int option;
    int res;

    if (!PyArg_ParseTuple(args, "i:getinfo_raw", &option)) {
        return NULL;
    }
    if (check_curl_state(self, 1 | 2, "getinfo") != 0) {
        return NULL;
    }

    switch (option) {
    case CURLINFO_FILETIME:
    case CURLINFO_HEADER_SIZE:
    case CURLINFO_RESPONSE_CODE:
    case CURLINFO_REDIRECT_COUNT:
    case CURLINFO_REQUEST_SIZE:
    case CURLINFO_SSL_VERIFYRESULT:
    case CURLINFO_HTTP_CONNECTCODE:
    case CURLINFO_HTTPAUTH_AVAIL:
    case CURLINFO_PROXYAUTH_AVAIL:
    case CURLINFO_OS_ERRNO:
    case CURLINFO_NUM_CONNECTS:
    case CURLINFO_LASTSOCKET:
#ifdef HAVE_CURLINFO_LOCAL_PORT
    case CURLINFO_LOCAL_PORT:
#endif
#ifdef HAVE_CURLINFO_PRIMARY_PORT
    case CURLINFO_PRIMARY_PORT:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    case CURLINFO_RTSP_CLIENT_CSEQ:
    case CURLINFO_RTSP_SERVER_CSEQ:
    case CURLINFO_RTSP_CSEQ_RECV:
#endif
#ifdef HAVE_CURLINFO_HTTP_VERSION
    case CURLINFO_HTTP_VERSION:
#endif
#ifdef HAVE_CURL_7_19_4_OPTS
    case CURLINFO_CONDITION_UNMET:
#endif
        {
            /* Return PyInt as result */
            long l_res = -1;

            res = curl_easy_getinfo(self->handle, (CURLINFO)option, &l_res);
            /* Check for errors and return result */
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            }
            return PyInt_FromLong(l_res);
        }

    case CURLINFO_CONTENT_TYPE:
    case CURLINFO_EFFECTIVE_URL:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 72, 0)
    case CURLINFO_EFFECTIVE_METHOD:
#endif
    case CURLINFO_FTP_ENTRY_PATH:
    case CURLINFO_REDIRECT_URL:
    case CURLINFO_PRIMARY_IP:
#ifdef HAVE_CURLINFO_LOCAL_IP
    case CURLINFO_LOCAL_IP:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    case CURLINFO_RTSP_SESSION_ID:
#endif
        {
            /* Return PyString as result */
            char *s_res = NULL;

            res = curl_easy_getinfo(self->handle, (CURLINFO)option, &s_res);
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            }
            /* If the resulting string is NULL, return None */
            if (s_res == NULL) {
                Py_RETURN_NONE;
            }
            return PyByteStr_FromString(s_res);

        }

    case CURLINFO_CONNECT_TIME:
    case CURLINFO_APPCONNECT_TIME:
    case CURLINFO_CONTENT_LENGTH_DOWNLOAD:
    case CURLINFO_CONTENT_LENGTH_UPLOAD:
    case CURLINFO_NAMELOOKUP_TIME:
    case CURLINFO_PRETRANSFER_TIME:
    case CURLINFO_REDIRECT_TIME:
    case CURLINFO_SIZE_DOWNLOAD:
    case CURLINFO_SIZE_UPLOAD:
    case CURLINFO_SPEED_DOWNLOAD:
    case CURLINFO_SPEED_UPLOAD:
    case CURLINFO_STARTTRANSFER_TIME:
    case CURLINFO_TOTAL_TIME:
        {
            /* Return PyFloat as result */
            double d_res = 0.0;

            res = curl_easy_getinfo(self->handle, (CURLINFO)option, &d_res);
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            }
            return PyFloat_FromDouble(d_res);
        }

    case CURLINFO_SSL_ENGINES:
    case CURLINFO_COOKIELIST:
        {
            /* Return a list of strings */
            struct curl_slist *slist = NULL;

            res = curl_easy_getinfo(self->handle, (CURLINFO)option, &slist);
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            }
            return convert_slist(slist, 1 | 2);
        }

#ifdef HAVE_CURLOPT_CERTINFO
    case CURLINFO_CERTINFO:
        {
            /* Return a list of lists of 2-tuples */
            struct curl_certinfo *clist = NULL;
            res = curl_easy_getinfo(self->handle, CURLINFO_CERTINFO, &clist);
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            } else {
                return convert_certinfo(clist, 0);
            }
        }
#endif

#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 55, 0)
    case CURLINFO_CONTENT_LENGTH_DOWNLOAD_T:
    case CURLINFO_CONTENT_LENGTH_UPLOAD_T:
    case CURLINFO_SIZE_DOWNLOAD_T:
    case CURLINFO_SIZE_UPLOAD_T:
    case CURLINFO_SPEED_DOWNLOAD_T:
    case CURLINFO_SPEED_UPLOAD_T:
/* endif for this section is after the block as this is the oldest _t */
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 59, 0)
    case CURLINFO_FILETIME_T:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 61, 0)
    case CURLINFO_APPCONNECT_TIME_T:
    case CURLINFO_CONNECT_TIME_T:
    case CURLINFO_NAMELOOKUP_TIME_T:
    case CURLINFO_PRETRANSFER_TIME_T:
    case CURLINFO_REDIRECT_TIME_T:
    case CURLINFO_STARTTRANSFER_TIME_T:
    case CURLINFO_TOTAL_TIME_T:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 6, 0)
    case CURLINFO_QUEUE_TIME_T:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 10, 0)
    case CURLINFO_POSTTRANSFER_TIME_T:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 11, 0)
    case CURLINFO_EARLYDATA_SENT_T:
#endif
        {
            /* Return PyLong as result */
            curl_off_t ot_res = 0;

            res = curl_easy_getinfo(self->handle, (CURLINFO)option, &ot_res);
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            }
            return PyLong_FromLongLong(ot_res);
        }
#endif

    }

    /* Got wrong option on the method call */
    PyErr_SetString(PyExc_ValueError, "invalid argument to getinfo");
    return NULL;
}


#if PY_MAJOR_VERSION >= 3
static PyObject *
decode_string_list(PyObject *list)
{
    PyObject *decoded_list = NULL;
    Py_ssize_t size = PyList_Size(list);
    int i;
    
    decoded_list = PyList_New(size);
    if (decoded_list == NULL) {
        return NULL;
    }
    
    for (i = 0; i < size; ++i) {
        PyObject *decoded_item = PyUnicode_FromEncodedObject(
            PyList_GET_ITEM(list, i),
            NULL,
            NULL);
        
        if (decoded_item == NULL) {
            goto err;
        }
	PyList_SetItem(decoded_list, i, decoded_item);
    }
    
    return decoded_list;
    
err:
    Py_DECREF(decoded_list);
    return NULL;
}

PYCURL_INTERNAL PyObject *
do_curl_getinfo(CurlObject *self, PyObject *args)
{
    int option, res;
    PyObject *rv;

    if (!PyArg_ParseTuple(args, "i:getinfo", &option)) {
        return NULL;
    }
    
#ifdef HAVE_CURLOPT_CERTINFO
    if (option == CURLINFO_CERTINFO) {
        /* Return a list of lists of 2-tuples */
        struct curl_certinfo *clist = NULL;
        res = curl_easy_getinfo(self->handle, CURLINFO_CERTINFO, &clist);
        if (res != CURLE_OK) {
            CURLERROR_RETVAL();
        } else {
            return convert_certinfo(clist, 1);
        }
    }
#endif
    
    rv = do_curl_getinfo_raw(self, args);
    if (rv == NULL) {
        return rv;
    }
    
    switch (option) {
    case CURLINFO_CONTENT_TYPE:
    case CURLINFO_EFFECTIVE_URL:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 72, 0)
    case CURLINFO_EFFECTIVE_METHOD:
#endif
    case CURLINFO_FTP_ENTRY_PATH:
    case CURLINFO_REDIRECT_URL:
    case CURLINFO_PRIMARY_IP:
#ifdef HAVE_CURLINFO_LOCAL_IP
    case CURLINFO_LOCAL_IP:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    case CURLINFO_RTSP_SESSION_ID:
#endif
        if (rv != Py_None) {
            PyObject *decoded;
        
            // Decode bytes into a Unicode string using default encoding
            decoded = PyUnicode_FromEncodedObject(rv, NULL, NULL);
            // success and failure paths both need to free bytes object
            Py_DECREF(rv);
            return decoded;
        }
        return rv;

    case CURLINFO_SSL_ENGINES:
    case CURLINFO_COOKIELIST:
        {
            PyObject *decoded = decode_string_list(rv);
            Py_DECREF(rv);
            return decoded;
        }
        
    default:
        return rv;
    }
}
#endif


PYCURL_INTERNAL PyObject *
do_curl_errstr(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    if (check_curl_state(self, 1 | 2, "errstr") != 0) {
        return NULL;
    }
    self->error[sizeof(self->error) - 1] = 0;

    return PyText_FromString(self->error);
}


#if PY_MAJOR_VERSION >= 3
PYCURL_INTERNAL PyObject *
do_curl_errstr_raw(CurlObject *self, PyObject *Py_UNUSED(ignored))
{
    if (check_curl_state(self, 1 | 2, "errstr") != 0) {
        return NULL;
    }
    self->error[sizeof(self->error) - 1] = 0;

    return PyByteStr_FromString(self->error);
}
#endif
