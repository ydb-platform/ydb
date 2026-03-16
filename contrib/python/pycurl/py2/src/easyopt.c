#include "pycurl.h"


static struct curl_slist *
pycurl_list_or_tuple_to_slist(int which, PyObject *obj, Py_ssize_t len)
{
    struct curl_slist *slist = NULL;
    Py_ssize_t i;

    for (i = 0; i < len; i++) {
        PyObject *listitem = PyListOrTuple_GetItem(obj, i, which);
        struct curl_slist *nlist;
        char *str;
        PyObject *sencoded_obj;

        if (!PyText_Check(listitem)) {
            curl_slist_free_all(slist);
            PyErr_SetString(PyExc_TypeError, "list items must be byte strings or Unicode strings with ASCII code points only");
            return NULL;
        }
        /* INFO: curl_slist_append() internally does strdup() the data, so
         * no embedded NUL characters allowed here. */
        str = PyText_AsString_NoNUL(listitem, &sencoded_obj);
        if (str == NULL) {
            curl_slist_free_all(slist);
            return NULL;
        }
        nlist = curl_slist_append(slist, str);
        PyText_EncodedDecref(sencoded_obj);
        if (nlist == NULL || nlist->data == NULL) {
            curl_slist_free_all(slist);
            PyErr_NoMemory();
            return NULL;
        }
        slist = nlist;
    }
    return slist;
}


static PyObject *
util_curl_unsetopt(CurlObject *self, int option)
{
    int res;

#define SETOPT2(o,x) \
    if ((res = curl_easy_setopt(self->handle, (o), (x))) != CURLE_OK) goto error
#define SETOPT(x)   SETOPT2((CURLoption)option, (x))
#define CLEAR_CALLBACK(callback_option, data_option, callback_field) \
    case callback_option: \
        if ((res = curl_easy_setopt(self->handle, callback_option, NULL)) != CURLE_OK) \
            goto error; \
        if ((res = curl_easy_setopt(self->handle, data_option, NULL)) != CURLE_OK) \
            goto error; \
        Py_CLEAR(callback_field); \
        break

    /* FIXME: implement more options. Have to carefully check lib/url.c in the
     *   libcurl source code to see if it's actually safe to simply
     *   unset the option. */
    switch (option)
    {
    case CURLOPT_SHARE:
        SETOPT((CURLSH *) NULL);
        Py_XDECREF(self->share);
        self->share = NULL;
        break;
    case CURLOPT_HTTPPOST:
        SETOPT((void *) 0);
        curl_formfree(self->httppost);
        util_curl_xdecref(self, PYCURL_MEMGROUP_HTTPPOST, self->handle);
        self->httppost = NULL;
        /* FIXME: what about data->set.httpreq ?? */
        break;
    case CURLOPT_INFILESIZE:
        SETOPT((long) -1);
        break;
    case CURLOPT_WRITEHEADER:
        SETOPT((void *) 0);
        Py_CLEAR(self->writeheader_fp);
        break;
    case CURLOPT_CAINFO:
    case CURLOPT_CAPATH:
    case CURLOPT_COOKIE:
    case CURLOPT_COOKIEJAR:
    case CURLOPT_CUSTOMREQUEST:
    case CURLOPT_EGDSOCKET:
    case CURLOPT_ENCODING:
    case CURLOPT_FTPPORT:
    case CURLOPT_PROXYUSERPWD:
#ifdef HAVE_CURLOPT_PROXYUSERNAME
    case CURLOPT_PROXYUSERNAME:
    case CURLOPT_PROXYPASSWORD:
#endif
    case CURLOPT_RANDOM_FILE:
    case CURLOPT_SSL_CIPHER_LIST:
    case CURLOPT_USERPWD:
#ifdef HAVE_CURLOPT_USERNAME
    case CURLOPT_USERNAME:
    case CURLOPT_PASSWORD:
#endif
    case CURLOPT_RANGE:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 43, 0)
    case CURLOPT_SERVICE_NAME:
    case CURLOPT_PROXY_SERVICE_NAME:
#endif
    case CURLOPT_HTTPHEADER:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    case CURLOPT_PROXYHEADER:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 52, 0)
    case CURLOPT_PROXY_CAPATH:
    case CURLOPT_PROXY_CAINFO:
    case CURLOPT_PRE_PROXY:
    case CURLOPT_PROXY_SSLCERT:
    case CURLOPT_PROXY_SSLCERTTYPE:
    case CURLOPT_PROXY_SSLKEY:
    case CURLOPT_PROXY_SSLKEYTYPE:
#endif
        SETOPT((char *) NULL);
        break;

#ifdef HAVE_CURLOPT_CERTINFO
    case CURLOPT_CERTINFO:
        SETOPT((long) 0);
        break;
#endif

    CLEAR_CALLBACK(CURLOPT_OPENSOCKETFUNCTION, CURLOPT_OPENSOCKETDATA, self->opensocket_cb);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
    CLEAR_CALLBACK(CURLOPT_CLOSESOCKETFUNCTION, CURLOPT_CLOSESOCKETDATA, self->closesocket_cb);
#endif
    CLEAR_CALLBACK(CURLOPT_SOCKOPTFUNCTION, CURLOPT_SOCKOPTDATA, self->sockopt_cb);
#ifdef HAVE_CURL_7_19_6_OPTS
    CLEAR_CALLBACK(CURLOPT_SSH_KEYFUNCTION, CURLOPT_SSH_KEYDATA, self->ssh_key_cb);
#endif

    /* info: we explicitly list unsupported options here */
    case CURLOPT_COOKIEFILE:
    default:
        PyErr_SetString(PyExc_TypeError, "unsetopt() is not supported for this option");
        return NULL;
    }

    Py_RETURN_NONE;

error:
    CURLERROR_RETVAL();

#undef SETOPT
#undef SETOPT2
#undef CLEAR_CALLBACK
}


PYCURL_INTERNAL PyObject *
do_curl_unsetopt(CurlObject *self, PyObject *args)
{
    int option;

    if (!PyArg_ParseTuple(args, "i:unsetopt", &option)) {
        return NULL;
    }
    if (check_curl_state(self, 1 | 2, "unsetopt") != 0) {
        return NULL;
    }

    /* early checks of option value */
    if (option <= 0)
        goto error;
    if (option >= (int)CURLOPTTYPE_OFF_T + OPTIONS_SIZE)
        goto error;
    if (option % 10000 >= OPTIONS_SIZE)
        goto error;

    return util_curl_unsetopt(self, option);

error:
    PyErr_SetString(PyExc_TypeError, "invalid arguments to unsetopt");
    return NULL;
}


static PyObject *
do_curl_setopt_string_impl(CurlObject *self, int option, PyObject *obj)
{
    char *str = NULL;
    Py_ssize_t len = -1;
    PyObject *encoded_obj;
    int res;

    /* Check that the option specified a string as well as the input */
    switch (option) {
    case CURLOPT_CAINFO:
    case CURLOPT_CAPATH:
    case CURLOPT_COOKIE:
    case CURLOPT_COOKIEFILE:
    case CURLOPT_COOKIELIST:
    case CURLOPT_COOKIEJAR:
    case CURLOPT_CUSTOMREQUEST:
    case CURLOPT_EGDSOCKET:
    /* use CURLOPT_ENCODING instead of CURLOPT_ACCEPT_ENCODING
    for compatibility with older libcurls */
    case CURLOPT_ENCODING:
    case CURLOPT_FTPPORT:
    case CURLOPT_INTERFACE:
    case CURLOPT_KEYPASSWD:
    case CURLOPT_NETRC_FILE:
    case CURLOPT_PROXY:
    case CURLOPT_PROXYUSERPWD:
#ifdef HAVE_CURLOPT_PROXYUSERNAME
    case CURLOPT_PROXYUSERNAME:
    case CURLOPT_PROXYPASSWORD:
#endif
    case CURLOPT_RANDOM_FILE:
    case CURLOPT_RANGE:
    case CURLOPT_REFERER:
    case CURLOPT_SSLCERT:
    case CURLOPT_SSLCERTTYPE:
    case CURLOPT_SSLENGINE:
    case CURLOPT_SSLKEY:
    case CURLOPT_SSLKEYTYPE:
    case CURLOPT_SSL_CIPHER_LIST:
    case CURLOPT_URL:
    case CURLOPT_USERAGENT:
    case CURLOPT_USERPWD:
#ifdef HAVE_CURLOPT_USERNAME
    case CURLOPT_USERNAME:
    case CURLOPT_PASSWORD:
#endif
    case CURLOPT_FTP_ALTERNATIVE_TO_USER:
    case CURLOPT_SSH_PUBLIC_KEYFILE:
    case CURLOPT_SSH_PRIVATE_KEYFILE:
    case CURLOPT_COPYPOSTFIELDS:
    case CURLOPT_SSH_HOST_PUBLIC_KEY_MD5:
    case CURLOPT_CRLFILE:
    case CURLOPT_ISSUERCERT:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    case CURLOPT_RTSP_STREAM_URI:
    case CURLOPT_RTSP_SESSION_ID:
    case CURLOPT_RTSP_TRANSPORT:
#endif
#ifdef HAVE_CURLOPT_DNS_SERVERS
    case CURLOPT_DNS_SERVERS:
#endif
#ifdef HAVE_CURLOPT_NOPROXY
    case CURLOPT_NOPROXY:
#endif
#ifdef HAVE_CURL_7_19_4_OPTS
    case CURLOPT_SOCKS5_GSSAPI_SERVICE:
#endif
#ifdef HAVE_CURL_7_19_6_OPTS
    case CURLOPT_SSH_KNOWNHOSTS:
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    case CURLOPT_MAIL_FROM:
#endif
#ifdef HAVE_CURL_7_25_0_OPTS
    case CURLOPT_MAIL_AUTH:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 39, 0)
    case CURLOPT_PINNEDPUBLICKEY:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 43, 0)
    case CURLOPT_SERVICE_NAME:
    case CURLOPT_PROXY_SERVICE_NAME:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 0)
    case CURLOPT_WILDCARDMATCH:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 40, 0)
    case CURLOPT_UNIX_SOCKET_PATH:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 4)
    case CURLOPT_TLSAUTH_TYPE:
    case CURLOPT_TLSAUTH_USERNAME:
    case CURLOPT_TLSAUTH_PASSWORD:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 45, 0)
    case CURLOPT_DEFAULT_PROTOCOL:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 34, 0)
    case CURLOPT_LOGIN_OPTIONS:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 33, 0)
    case CURLOPT_XOAUTH2_BEARER:
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 52, 0)
    case CURLOPT_PROXY_CAPATH:
    case CURLOPT_PROXY_CAINFO:
    case CURLOPT_PRE_PROXY:
    case CURLOPT_PROXY_SSLCERT:
    case CURLOPT_PROXY_SSLCERTTYPE:
    case CURLOPT_PROXY_SSLKEY:
    case CURLOPT_PROXY_SSLKEYTYPE:
#endif
    case CURLOPT_KRBLEVEL:
        str = PyText_AsString_NoNUL(obj, &encoded_obj);
        if (str == NULL)
            return NULL;
        break;
    case CURLOPT_POSTFIELDS:
        if (PyText_AsStringAndSize(obj, &str, &len, &encoded_obj) != 0)
            return NULL;
        /* automatically set POSTFIELDSIZE */
        if (len <= INT_MAX) {
            res = curl_easy_setopt(self->handle, CURLOPT_POSTFIELDSIZE, (long)len);
        } else {
            res = curl_easy_setopt(self->handle, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)len);
        }
        if (res != CURLE_OK) {
            PyText_EncodedDecref(encoded_obj);
            CURLERROR_RETVAL();
        }
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "strings are not supported for this option");
        return NULL;
    }
    assert(str != NULL);
    /* Call setopt */
    res = curl_easy_setopt(self->handle, (CURLoption)option, str);
    /* Check for errors */
    if (res != CURLE_OK) {
        PyText_EncodedDecref(encoded_obj);
        CURLERROR_RETVAL();
    }
    /* libcurl does not copy the value of CURLOPT_POSTFIELDS */
    if (option == CURLOPT_POSTFIELDS) {
        PyObject *store_obj;

        /* if obj was bytes, it was not encoded, and we need to incref obj.
         * if obj was unicode, it was encoded, and we need to incref
         * encoded_obj - except we can simply transfer ownership.
         */
        if (encoded_obj) {
            store_obj = encoded_obj;
        } else {
            /* no encoding is performed, incref the original object. */
            store_obj = obj;
            Py_INCREF(store_obj);
        }

        util_curl_xdecref(self, PYCURL_MEMGROUP_POSTFIELDS, self->handle);
        self->postfields_obj = store_obj;
    } else {
        PyText_EncodedDecref(encoded_obj);
    }
    Py_RETURN_NONE;
}


#define IS_LONG_OPTION(o)   (o < CURLOPTTYPE_OBJECTPOINT)
#define IS_OFF_T_OPTION(o)  (o >= CURLOPTTYPE_OFF_T)


static PyObject *
do_curl_setopt_int(CurlObject *self, int option, PyObject *obj)
{
    long d;
    PY_LONG_LONG ld;
    int res;

    if (IS_LONG_OPTION(option)) {
        d = PyInt_AsLong(obj);
        res = curl_easy_setopt(self->handle, (CURLoption)option, (long)d);
    } else if (IS_OFF_T_OPTION(option)) {
        /* this path should only be taken in Python 3 */
        ld = PyLong_AsLongLong(obj);
        res = curl_easy_setopt(self->handle, (CURLoption)option, (curl_off_t)ld);
    } else {
        PyErr_SetString(PyExc_TypeError, "integers are not supported for this option");
        return NULL;
    }
    if (res != CURLE_OK) {
        CURLERROR_RETVAL();
    }
    Py_RETURN_NONE;
}


static PyObject *
do_curl_setopt_long(CurlObject *self, int option, PyObject *obj)
{
    int res;
    PY_LONG_LONG d = PyLong_AsLongLong(obj);
    if (d == -1 && PyErr_Occurred())
        return NULL;

    if (IS_LONG_OPTION(option) && (long)d == d)
        res = curl_easy_setopt(self->handle, (CURLoption)option, (long)d);
    else if (IS_OFF_T_OPTION(option) && (curl_off_t)d == d)
        res = curl_easy_setopt(self->handle, (CURLoption)option, (curl_off_t)d);
    else {
        PyErr_SetString(PyExc_TypeError, "longs are not supported for this option");
        return NULL;
    }
    if (res != CURLE_OK) {
        CURLERROR_RETVAL();
    }
    Py_RETURN_NONE;
}


#undef IS_LONG_OPTION
#undef IS_OFF_T_OPTION


#if PY_MAJOR_VERSION < 3 && !defined(PYCURL_AVOID_STDIO)
static PyObject *
do_curl_setopt_file_passthrough(CurlObject *self, int option, PyObject *obj)
{
    FILE *fp;
    int res;

    fp = PyFile_AsFile(obj);
    if (fp == NULL) {
        PyErr_SetString(PyExc_TypeError, "second argument must be open file");
        return NULL;
    }
    
    switch (option) {
    case CURLOPT_READDATA:
        res = curl_easy_setopt(self->handle, CURLOPT_READFUNCTION, fread);
        if (res != CURLE_OK) {
            CURLERROR_RETVAL();
        }
        break;
    case CURLOPT_WRITEDATA:
        res = curl_easy_setopt(self->handle, CURLOPT_WRITEFUNCTION, fwrite);
        if (res != CURLE_OK) {
            CURLERROR_RETVAL();
        }
        break;
    case CURLOPT_WRITEHEADER:
        res = curl_easy_setopt(self->handle, CURLOPT_HEADERFUNCTION, fwrite);
        if (res != CURLE_OK) {
            CURLERROR_RETVAL();
        }
        break;
    default:
        PyErr_SetString(PyExc_TypeError, "files are not supported for this option");
        return NULL;
    }

    res = curl_easy_setopt(self->handle, (CURLoption)option, fp);
    if (res != CURLE_OK) {
        /*
        If we get here fread/fwrite are set as callbacks but the file pointer
        is not set, program will crash if it does not reset read/write
        callback. Also, we won't do the memory management later in this
        function.
        */
        CURLERROR_RETVAL();
    }
    Py_INCREF(obj);

    switch (option) {
    case CURLOPT_READDATA:
        Py_CLEAR(self->readdata_fp);
        self->readdata_fp = obj;
        break;
    case CURLOPT_WRITEDATA:
        Py_CLEAR(self->writedata_fp);
        self->writedata_fp = obj;
        break;
    case CURLOPT_WRITEHEADER:
        Py_CLEAR(self->writeheader_fp);
        self->writeheader_fp = obj;
        break;
    default:
        assert(0);
        break;
    }
    /* Return success */
    Py_RETURN_NONE;
}
#endif


static PyObject *
do_curl_setopt_httppost(CurlObject *self, int option, int which, PyObject *obj)
{
    struct curl_httppost *post = NULL;
    struct curl_httppost *last = NULL;
    /* List of all references that have been INCed as a result of
     * this operation */
    PyObject *ref_params = NULL;
    PyObject *nencoded_obj, *cencoded_obj, *oencoded_obj;
    int which_httppost_item, which_httppost_option;
    PyObject *httppost_option;
    Py_ssize_t i, len;
    int res;

    len = PyListOrTuple_Size(obj, which);
    if (len == 0)
        Py_RETURN_NONE;

    for (i = 0; i < len; i++) {
        char *nstr = NULL, *cstr = NULL;
        Py_ssize_t nlen = -1, clen = -1;
        PyObject *listitem = PyListOrTuple_GetItem(obj, i, which);

        which_httppost_item = PyListOrTuple_Check(listitem);
        if (!which_httppost_item) {
            PyErr_SetString(PyExc_TypeError, "list items must be list or tuple objects");
            goto error;
        }
        if (PyListOrTuple_Size(listitem, which_httppost_item) != 2) {
            PyErr_SetString(PyExc_TypeError, "list or tuple must contain two elements (name, value)");
            goto error;
        }
        if (PyText_AsStringAndSize(PyListOrTuple_GetItem(listitem, 0, which_httppost_item),
                &nstr, &nlen, &nencoded_obj) != 0) {
            PyErr_SetString(PyExc_TypeError, "list or tuple must contain a byte string or Unicode string with ASCII code points only as first element");
            goto error;
        }
        httppost_option = PyListOrTuple_GetItem(listitem, 1, which_httppost_item);
        if (PyText_Check(httppost_option)) {
            /* Handle strings as second argument for backwards compatibility */

            if (PyText_AsStringAndSize(httppost_option, &cstr, &clen, &cencoded_obj)) {
                PyText_EncodedDecref(nencoded_obj);
                create_and_set_error_object(self, CURLE_BAD_FUNCTION_ARGUMENT);
                goto error;
            }
            /* INFO: curl_formadd() internally does memdup() the data, so
             * embedded NUL characters _are_ allowed here. */
            res = curl_formadd(&post, &last,
                               CURLFORM_COPYNAME, nstr,
                               CURLFORM_NAMELENGTH, (long) nlen,
                               CURLFORM_COPYCONTENTS, cstr,
                               CURLFORM_CONTENTSLENGTH, (long) clen,
                               CURLFORM_END);
            PyText_EncodedDecref(cencoded_obj);
            if (res != CURLE_OK) {
                PyText_EncodedDecref(nencoded_obj);
                CURLERROR_SET_RETVAL();
                goto error;
            }
        }
        /* assignment is intended */
        else if ((which_httppost_option = PyListOrTuple_Check(httppost_option))) {
            /* Supports content, file and content-type */
            Py_ssize_t tlen = PyListOrTuple_Size(httppost_option, which_httppost_option);
            int j, k, l;
            struct curl_forms *forms = NULL;

            /* Sanity check that there are at least two tuple items */
            if (tlen < 2) {
                PyText_EncodedDecref(nencoded_obj);
                PyErr_SetString(PyExc_TypeError, "list or tuple must contain at least one option and one value");
                goto error;
            }

            if (tlen % 2 == 1) {
                PyText_EncodedDecref(nencoded_obj);
                PyErr_SetString(PyExc_TypeError, "list or tuple must contain an even number of items");
                goto error;
            }

            /* Allocate enough space to accommodate length options for content or buffers, plus a terminator. */
            forms = PyMem_New(struct curl_forms, (tlen*2) + 1);
            if (forms == NULL) {
                PyText_EncodedDecref(nencoded_obj);
                PyErr_NoMemory();
                goto error;
            }

            /* Iterate all the tuple members pairwise */
            for (j = 0, k = 0, l = 0; j < tlen; j += 2, l++) {
                char *ostr;
                Py_ssize_t olen;
                int val;

                if (j == (tlen-1)) {
                    PyErr_SetString(PyExc_TypeError, "expected value");
                    PyMem_Free(forms);
                    PyText_EncodedDecref(nencoded_obj);
                    goto error;
                }
                if (!PyInt_Check(PyListOrTuple_GetItem(httppost_option, j, which_httppost_option))) {
                    PyErr_SetString(PyExc_TypeError, "option must be an integer");
                    PyMem_Free(forms);
                    PyText_EncodedDecref(nencoded_obj);
                    goto error;
                }
                if (!PyText_Check(PyListOrTuple_GetItem(httppost_option, j+1, which_httppost_option))) {
                    PyErr_SetString(PyExc_TypeError, "value must be a byte string or a Unicode string with ASCII code points only");
                    PyMem_Free(forms);
                    PyText_EncodedDecref(nencoded_obj);
                    goto error;
                }

                val = PyLong_AsLong(PyListOrTuple_GetItem(httppost_option, j, which_httppost_option));
                if (val != CURLFORM_COPYCONTENTS &&
                    val != CURLFORM_FILE &&
                    val != CURLFORM_FILENAME &&
                    val != CURLFORM_CONTENTTYPE &&
                    val != CURLFORM_BUFFER &&
                    val != CURLFORM_BUFFERPTR)
                {
                    PyErr_SetString(PyExc_TypeError, "unsupported option");
                    PyMem_Free(forms);
                    PyText_EncodedDecref(nencoded_obj);
                    goto error;
                }

                if (PyText_AsStringAndSize(PyListOrTuple_GetItem(httppost_option, j+1, which_httppost_option), &ostr, &olen, &oencoded_obj)) {
                    /* exception should be already set */
                    PyMem_Free(forms);
                    PyText_EncodedDecref(nencoded_obj);
                    goto error;
                }
                forms[k].option = val;
                forms[k].value = ostr;
                ++k;

                if (val == CURLFORM_COPYCONTENTS) {
                    /* Contents can contain \0 bytes so we specify the length */
                    forms[k].option = CURLFORM_CONTENTSLENGTH;
                    forms[k].value = (const char *)olen;
                    ++k;
                } else if (val == CURLFORM_BUFFERPTR) {
                    PyObject *obj = NULL;

                    if (ref_params == NULL) {
                        ref_params = PyList_New((Py_ssize_t)0);
                        if (ref_params == NULL) {
                            PyText_EncodedDecref(oencoded_obj);
                            PyMem_Free(forms);
                            PyText_EncodedDecref(nencoded_obj);
                            goto error;
                        }
                    }

                    /* Keep a reference to the object that holds the ostr buffer. */
                    if (oencoded_obj == NULL) {
                        obj = PyListOrTuple_GetItem(httppost_option, j+1, which_httppost_option);
                    }
                    else {
                        obj = oencoded_obj;
                    }

                    /* Ensure that the buffer remains alive until curl_easy_cleanup() */
                    if (PyList_Append(ref_params, obj) != 0) {
                        PyText_EncodedDecref(oencoded_obj);
                        PyMem_Free(forms);
                        PyText_EncodedDecref(nencoded_obj);
                        goto error;
                    }

                    /* As with CURLFORM_COPYCONTENTS, specify the length. */
                    forms[k].option = CURLFORM_BUFFERLENGTH;
                    forms[k].value = (const char *)olen;
                    ++k;
                }
            }
            forms[k].option = CURLFORM_END;
            res = curl_formadd(&post, &last,
                               CURLFORM_COPYNAME, nstr,
                               CURLFORM_NAMELENGTH, (long) nlen,
                               CURLFORM_ARRAY, forms,
                               CURLFORM_END);
            PyText_EncodedDecref(oencoded_obj);
            PyMem_Free(forms);
            if (res != CURLE_OK) {
                PyText_EncodedDecref(nencoded_obj);
                CURLERROR_SET_RETVAL();
                goto error;
            }
        } else {
            /* Some other type was given, ignore */
            PyText_EncodedDecref(nencoded_obj);
            PyErr_SetString(PyExc_TypeError, "unsupported second type in tuple");
            goto error;
        }
        PyText_EncodedDecref(nencoded_obj);
    }
    res = curl_easy_setopt(self->handle, CURLOPT_HTTPPOST, post);
    /* Check for errors */
    if (res != CURLE_OK) {
        CURLERROR_SET_RETVAL();
        goto error;
    }
    /* Finally, free previously allocated httppost, ZAP any
     * buffer references, and update */
    curl_formfree(self->httppost);
    util_curl_xdecref(self, PYCURL_MEMGROUP_HTTPPOST, self->handle);
    self->httppost = post;

    /* The previous list of INCed references was ZAPed above; save
     * the new one so that we can clean it up on the next
     * self->httppost free. */
    self->httppost_ref_list = ref_params;

    Py_RETURN_NONE;

error:
    curl_formfree(post);
    Py_XDECREF(ref_params);
    return NULL;
}


static PyObject *
do_curl_setopt_list(CurlObject *self, int option, int which, PyObject *obj)
{
    struct curl_slist **old_slist = NULL;
    struct curl_slist *slist = NULL;
    Py_ssize_t len;
    int res;

    switch (option) {
    case CURLOPT_HTTP200ALIASES:
        old_slist = &self->http200aliases;
        break;
    case CURLOPT_HTTPHEADER:
        old_slist = &self->httpheader;
        break;
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    case CURLOPT_PROXYHEADER:
        old_slist = &self->proxyheader;
        break;
#endif
    case CURLOPT_POSTQUOTE:
        old_slist = &self->postquote;
        break;
    case CURLOPT_PREQUOTE:
        old_slist = &self->prequote;
        break;
    case CURLOPT_QUOTE:
        old_slist = &self->quote;
        break;
    case CURLOPT_TELNETOPTIONS:
        old_slist = &self->telnetoptions;
        break;
#ifdef HAVE_CURLOPT_RESOLVE
    case CURLOPT_RESOLVE:
        old_slist = &self->resolve;
        break;
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    case CURLOPT_MAIL_RCPT:
        old_slist = &self->mail_rcpt;
        break;
#endif
#ifdef HAVE_CURLOPT_CONNECT_TO
    case CURLOPT_CONNECT_TO:
        old_slist = &self->connect_to;
        break;
#endif
    default:
        /* None of the list options were recognized, raise exception */
        PyErr_SetString(PyExc_TypeError, "lists are not supported for this option");
        return NULL;
    }

    len = PyListOrTuple_Size(obj, which);
    if (len == 0)
        Py_RETURN_NONE;

    /* Just to be sure we do not bug off here */
    assert(old_slist != NULL && slist == NULL);

    /* Handle regular list operations on the other options */
    slist = pycurl_list_or_tuple_to_slist(which, obj, len);
    if (slist == NULL) {
        return NULL;
    }
    res = curl_easy_setopt(self->handle, (CURLoption)option, slist);
    /* Check for errors */
    if (res != CURLE_OK) {
        curl_slist_free_all(slist);
        CURLERROR_RETVAL();
    }
    /* Finally, free previously allocated list and update */
    curl_slist_free_all(*old_slist);
    *old_slist = slist;

    Py_RETURN_NONE;
}


static PyObject *
do_curl_setopt_callable(CurlObject *self, int option, PyObject *obj)
{
    /* We use function types here to make sure that our callback
     * definitions exactly match the <curl/curl.h> interface.
     */
    const curl_write_callback w_cb = write_callback;
    const curl_write_callback h_cb = header_callback;
    const curl_read_callback r_cb = read_callback;
    const curl_progress_callback pro_cb = progress_callback;
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
    const curl_xferinfo_callback xferinfo_cb = xferinfo_callback;
#endif
    const curl_debug_callback debug_cb = debug_callback;
    const curl_ioctl_callback ioctl_cb = ioctl_callback;
    const curl_opensocket_callback opensocket_cb = opensocket_callback;
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
    const curl_closesocket_callback closesocket_cb = closesocket_callback;
#endif
    const curl_seek_callback seek_cb = seek_callback;

    switch(option) {
    case CURLOPT_WRITEFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->writedata_fp);
        Py_CLEAR(self->w_cb);
        self->w_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_WRITEFUNCTION, w_cb);
        curl_easy_setopt(self->handle, CURLOPT_WRITEDATA, self);
        break;
    case CURLOPT_HEADERFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->writeheader_fp);
        Py_CLEAR(self->h_cb);
        self->h_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_HEADERFUNCTION, h_cb);
        curl_easy_setopt(self->handle, CURLOPT_WRITEHEADER, self);
        break;
    case CURLOPT_READFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->readdata_fp);
        Py_CLEAR(self->r_cb);
        self->r_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_READFUNCTION, r_cb);
        curl_easy_setopt(self->handle, CURLOPT_READDATA, self);
        break;
    case CURLOPT_PROGRESSFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->pro_cb);
        self->pro_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_PROGRESSFUNCTION, pro_cb);
        curl_easy_setopt(self->handle, CURLOPT_PROGRESSDATA, self);
        break;
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
    case CURLOPT_XFERINFOFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->xferinfo_cb);
        self->xferinfo_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_XFERINFOFUNCTION, xferinfo_cb);
        curl_easy_setopt(self->handle, CURLOPT_XFERINFODATA, self);
        break;
#endif
    case CURLOPT_DEBUGFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->debug_cb);
        self->debug_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_DEBUGFUNCTION, debug_cb);
        curl_easy_setopt(self->handle, CURLOPT_DEBUGDATA, self);
        break;
    case CURLOPT_IOCTLFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->ioctl_cb);
        self->ioctl_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_IOCTLFUNCTION, ioctl_cb);
        curl_easy_setopt(self->handle, CURLOPT_IOCTLDATA, self);
        break;
    case CURLOPT_OPENSOCKETFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->opensocket_cb);
        self->opensocket_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_OPENSOCKETFUNCTION, opensocket_cb);
        curl_easy_setopt(self->handle, CURLOPT_OPENSOCKETDATA, self);
        break;
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
    case CURLOPT_CLOSESOCKETFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->closesocket_cb);
        self->closesocket_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_CLOSESOCKETFUNCTION, closesocket_cb);
        curl_easy_setopt(self->handle, CURLOPT_CLOSESOCKETDATA, self);
        break;
#endif
    case CURLOPT_SOCKOPTFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->sockopt_cb);
        self->sockopt_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_SOCKOPTFUNCTION, sockopt_cb);
        curl_easy_setopt(self->handle, CURLOPT_SOCKOPTDATA, self);
        break;
#ifdef HAVE_CURL_7_19_6_OPTS
    case CURLOPT_SSH_KEYFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->ssh_key_cb);
        self->ssh_key_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_SSH_KEYFUNCTION, ssh_key_cb);
        curl_easy_setopt(self->handle, CURLOPT_SSH_KEYDATA, self);
        break;
#endif
    case CURLOPT_SEEKFUNCTION:
        Py_INCREF(obj);
        Py_CLEAR(self->seek_cb);
        self->seek_cb = obj;
        curl_easy_setopt(self->handle, CURLOPT_SEEKFUNCTION, seek_cb);
        curl_easy_setopt(self->handle, CURLOPT_SEEKDATA, self);
        break;

    default:
        /* None of the function options were recognized, raise exception */
        PyErr_SetString(PyExc_TypeError, "functions are not supported for this option");
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
do_curl_setopt_share(CurlObject *self, PyObject *obj)
{
    CurlShareObject *share;
    int res;

    if (self->share == NULL && (obj == NULL || obj == Py_None))
        Py_RETURN_NONE;

    if (self->share) {
        if (obj != Py_None) {
            PyErr_SetString(ErrorObject, "Curl object already sharing. Unshare first.");
            return NULL;
        }
        else {
            share = self->share;
            res = curl_easy_setopt(self->handle, CURLOPT_SHARE, NULL);
            if (res != CURLE_OK) {
                CURLERROR_RETVAL();
            }
            self->share = NULL;
            Py_DECREF(share);
            Py_RETURN_NONE;
        }
    }
    if (Py_TYPE(obj) != p_CurlShare_Type) {
        PyErr_SetString(PyExc_TypeError, "invalid arguments to setopt");
        return NULL;
    }
    share = (CurlShareObject*)obj;
    res = curl_easy_setopt(self->handle, CURLOPT_SHARE, share->share_handle);
    if (res != CURLE_OK) {
        CURLERROR_RETVAL();
    }
    self->share = share;
    Py_INCREF(share);
    Py_RETURN_NONE;
}


PYCURL_INTERNAL PyObject *
do_curl_setopt_filelike(CurlObject *self, int option, PyObject *obj)
{
    const char *method_name;
    PyObject *method;

    if (option == CURLOPT_READDATA) {
        method_name = "read";
    } else {
        method_name = "write";
    }
    method = PyObject_GetAttrString(obj, method_name);
    if (method) {
        PyObject *arglist;
        PyObject *rv;

        switch (option) {
            case CURLOPT_READDATA:
                option = CURLOPT_READFUNCTION;
                break;
            case CURLOPT_WRITEDATA:
                option = CURLOPT_WRITEFUNCTION;
                break;
            case CURLOPT_WRITEHEADER:
                option = CURLOPT_HEADERFUNCTION;
                break;
            default:
                PyErr_SetString(PyExc_TypeError, "objects are not supported for this option");
                Py_DECREF(method);
                return NULL;
        }

        arglist = Py_BuildValue("(iO)", option, method);
        /* reference is now in arglist */
        Py_DECREF(method);
        if (arglist == NULL) {
            return NULL;
        }
        rv = do_curl_setopt(self, arglist);
        Py_DECREF(arglist);
        return rv;
    } else {
        if (option == CURLOPT_READDATA) {
            PyErr_SetString(PyExc_TypeError, "object given without a read method");
        } else {
            PyErr_SetString(PyExc_TypeError, "object given without a write method");
        }
        return NULL;
    }
}


PYCURL_INTERNAL PyObject *
do_curl_setopt(CurlObject *self, PyObject *args)
{
    int option;
    PyObject *obj;
    int which;

    if (!PyArg_ParseTuple(args, "iO:setopt", &option, &obj))
        return NULL;
    if (check_curl_state(self, 1 | 2, "setopt") != 0)
        return NULL;

    /* early checks of option value */
    if (option <= 0)
        goto error;
    if (option >= (int)CURLOPTTYPE_OFF_T + OPTIONS_SIZE)
        goto error;
    if (option % 10000 >= OPTIONS_SIZE)
        goto error;

    /* Handle the case of None as the call of unsetopt() */
    if (obj == Py_None) {
        return util_curl_unsetopt(self, option);
    }

    /* Handle the case of string arguments */
    if (PyText_Check(obj)) {
        return do_curl_setopt_string_impl(self, option, obj);
    }

    /* Handle the case of integer arguments */
    if (PyInt_Check(obj)) {
        return do_curl_setopt_int(self, option, obj);
    }

    /* Handle the case of long arguments (used by *_LARGE options) */
    if (PyLong_Check(obj)) {
        return do_curl_setopt_long(self, option, obj);
    }

#if PY_MAJOR_VERSION < 3 && !defined(PYCURL_AVOID_STDIO)
    /* Handle the case of file objects */
    if (PyFile_Check(obj)) {
        return do_curl_setopt_file_passthrough(self, option, obj);
    }
#endif

    /* Handle the case of list or tuple objects */
    which = PyListOrTuple_Check(obj);
    if (which) {
        if (option == CURLOPT_HTTPPOST) {
            return do_curl_setopt_httppost(self, option, which, obj);
        } else {
            return do_curl_setopt_list(self, option, which, obj);
        }
    }

    /* Handle the case of function objects for callbacks */
    if (PyFunction_Check(obj) || PyCFunction_Check(obj) ||
        PyCallable_Check(obj) || PyMethod_Check(obj)) {
        return do_curl_setopt_callable(self, option, obj);
    }
    /* handle the SHARE case */
    if (option == CURLOPT_SHARE) {
        return do_curl_setopt_share(self, obj);
    }

    /*
    Handle the case of file-like objects.

    Given an object with a write method, we will call the write method
    from the appropriate callback.

    Files in Python 3 are no longer FILE * instances and therefore cannot
    be directly given to curl, therefore this method handles all I/O to
    Python objects.
    
    In Python 2 true file objects are FILE * instances and will be handled
    by stdio passthrough code invoked above, and file-like objects will
    be handled by this method.
    */
    if (option == CURLOPT_READDATA ||
        option == CURLOPT_WRITEDATA ||
        option == CURLOPT_WRITEHEADER)
    {
        return do_curl_setopt_filelike(self, option, obj);
    }

    /* Failed to match any of the function signatures -- return error */
error:
    PyErr_SetString(PyExc_TypeError, "invalid arguments to setopt");
    return NULL;
}


PYCURL_INTERNAL PyObject *
do_curl_setopt_string(CurlObject *self, PyObject *args)
{
    int option;
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "iO:setopt", &option, &obj))
        return NULL;
    if (check_curl_state(self, 1 | 2, "setopt") != 0)
        return NULL;

    /* Handle the case of string arguments */
    if (PyText_Check(obj)) {
        return do_curl_setopt_string_impl(self, option, obj);
    }

    /* Failed to match any of the function signatures -- return error */
    PyErr_SetString(PyExc_TypeError, "invalid arguments to setopt_string");
    return NULL;
}


#if defined(HAVE_CURL_OPENSSL)
/* load ca certs from string */
PYCURL_INTERNAL PyObject *
do_curl_set_ca_certs(CurlObject *self, PyObject *args)
{
    PyObject *cadata;
    PyObject *encoded_obj;
    char *buffer;
    Py_ssize_t length;
    int res;

    if (!PyArg_ParseTuple(args, "O:cadata", &cadata))
        return NULL;

    // This may result in cadata string being encoded twice,
    // not going to worry about it for now
    if (!PyText_Check(cadata)) {
        PyErr_SetString(PyExc_TypeError, "set_ca_certs argument must be a byte string or a Unicode string with ASCII code points only");
        return NULL;
    }

    res = PyText_AsStringAndSize(cadata, &buffer, &length, &encoded_obj);
    if (res) {
        PyErr_SetString(PyExc_TypeError, "set_ca_certs argument must be a byte string or a Unicode string with ASCII code points only");
        return NULL;
    }

    Py_CLEAR(self->ca_certs_obj);
    if (encoded_obj) {
        self->ca_certs_obj = encoded_obj;
    } else {
        Py_INCREF(cadata);
        self->ca_certs_obj = cadata;
    }

    res = curl_easy_setopt(self->handle, CURLOPT_SSL_CTX_FUNCTION, (curl_ssl_ctx_callback) ssl_ctx_callback);
    if (res != CURLE_OK) {
        Py_CLEAR(self->ca_certs_obj);
        CURLERROR_RETVAL();
    }

    res = curl_easy_setopt(self->handle, CURLOPT_SSL_CTX_DATA, self);
    if (res != CURLE_OK) {
        Py_CLEAR(self->ca_certs_obj);
        CURLERROR_RETVAL();
    }

    Py_RETURN_NONE;
}
#endif
