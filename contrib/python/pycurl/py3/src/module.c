#include "pycurl.h"
#include "docstrings.h"

#if defined(WIN32)
# define PYCURL_STRINGIZE_IMP(x) #x
# define PYCURL_STRINGIZE(x) PYCURL_STRINGIZE_IMP(x)
# define PYCURL_VERSION_STRING PYCURL_STRINGIZE(PYCURL_VERSION)
#else
# define PYCURL_VERSION_STRING PYCURL_VERSION
#endif

#define PYCURL_VERSION_PREFIX "PycURL/" PYCURL_VERSION_STRING

/* needed for compatibility with python < 3.10, as suggested at:
 * https://docs.python.org/3.10/whatsnew/3.10.html#id2 */
#if PY_VERSION_HEX < 0x030900A4
#  define Py_SET_TYPE(obj, type) ((Py_TYPE(obj) = (type)), (void)0)
#endif

PYCURL_INTERNAL char *empty_keywords[] = { NULL };

PYCURL_INTERNAL PyObject *bytesio = NULL;
PYCURL_INTERNAL PyObject *stringio = NULL;

/* Initialized during module init */
PYCURL_INTERNAL char *g_pycurl_useragent = NULL;

/* Type objects */
PYCURL_INTERNAL PyObject *ErrorObject = NULL;
PYCURL_INTERNAL PyTypeObject *p_Curl_Type = NULL;
PYCURL_INTERNAL PyTypeObject *p_CurlSlist_Type = NULL;
PYCURL_INTERNAL PyTypeObject *p_CurlHttppost_Type = NULL;
PYCURL_INTERNAL PyTypeObject *p_CurlMulti_Type = NULL;
PYCURL_INTERNAL PyTypeObject *p_CurlShare_Type = NULL;
#ifdef HAVE_CURL_7_19_6_OPTS
PYCURL_INTERNAL PyObject *khkey_type = NULL;
#endif
PYCURL_INTERNAL PyObject *curl_sockaddr_type = NULL;

PYCURL_INTERNAL PyObject *curlobject_constants = NULL;
PYCURL_INTERNAL PyObject *curlmultiobject_constants = NULL;
PYCURL_INTERNAL PyObject *curlshareobject_constants = NULL;


/* List of functions defined in this module */
static PyMethodDef curl_methods[] = {
    {"global_init", (PyCFunction)do_global_init, METH_VARARGS, pycurl_global_init_doc},
    {"global_cleanup", (PyCFunction)do_global_cleanup, METH_NOARGS, pycurl_global_cleanup_doc},
    {"version_info", (PyCFunction)do_version_info, METH_VARARGS, pycurl_version_info_doc},
    {NULL, NULL, 0, NULL}
};


/*************************************************************************
// module level
// Note that the object constructors (do_curl_new, do_multi_new)
// are module-level functions as well.
**************************************************************************/

static int
are_global_init_flags_valid(int flags)
{
#ifdef CURL_GLOBAL_ACK_EINTR
    /* CURL_GLOBAL_ACK_EINTR was introduced in libcurl-7.30.0 */
    return !(flags & ~(CURL_GLOBAL_ALL | CURL_GLOBAL_ACK_EINTR));
#else
    return !(flags & ~(CURL_GLOBAL_ALL));
#endif
}

PYCURL_INTERNAL PyObject *
do_global_init(PyObject *dummy, PyObject *args)
{
    int res, option;

    UNUSED(dummy);
    if (!PyArg_ParseTuple(args, "i:global_init", &option)) {
        return NULL;
    }

    if (!are_global_init_flags_valid(option)) {
        PyErr_SetString(PyExc_ValueError, "invalid option to global_init");
        return NULL;
    }

    res = curl_global_init(option);
    if (res != CURLE_OK) {
        PyErr_SetString(ErrorObject, "unable to set global option");
        return NULL;
    }

    Py_RETURN_NONE;
}


PYCURL_INTERNAL PyObject *
do_global_cleanup(PyObject *dummy, PyObject *Py_UNUSED(ignored))
{
    UNUSED(dummy);
    curl_global_cleanup();
#ifdef PYCURL_NEED_SSL_TSL
    pycurl_ssl_cleanup();
#endif
    Py_RETURN_NONE;
}


static PyObject *vi_str(const char *s)
{
    if (s == NULL)
        Py_RETURN_NONE;
    while (*s == ' ' || *s == '\t')
        s++;
    return PyText_FromString(s);
}

PYCURL_INTERNAL PyObject *
do_version_info(PyObject *dummy, PyObject *args)
{
    const curl_version_info_data *vi;
    PyObject *ret = NULL;
    PyObject *protocols = NULL;
    PyObject *tmp;
    Py_ssize_t i;
    int stamp = CURLVERSION_NOW;

    UNUSED(dummy);
    if (!PyArg_ParseTuple(args, "|i:version_info", &stamp)) {
        return NULL;
    }
    vi = curl_version_info((CURLversion) stamp);
    if (vi == NULL) {
        PyErr_SetString(ErrorObject, "unable to get version info");
        return NULL;
    }

    /* INFO: actually libcurl in lib/version.c does ignore
     * the "stamp" parameter, and so do we. */

    for (i = 0; vi->protocols[i] != NULL; )
        i++;
    protocols = PyTuple_New(i);
    if (protocols == NULL)
        goto error;
    for (i = 0; vi->protocols[i] != NULL; i++) {
        tmp = vi_str(vi->protocols[i]);
        if (tmp == NULL)
            goto error;
        PyTuple_SET_ITEM(protocols, i, tmp);
    }
    ret = PyTuple_New((Py_ssize_t)12);
    if (ret == NULL)
        goto error;

#define SET(i, v) \
        tmp = (v); if (tmp == NULL) goto error; PyTuple_SET_ITEM(ret, i, tmp)
    SET(0, PyInt_FromLong((long) vi->age));
    SET(1, vi_str(vi->version));
    SET(2, PyInt_FromLong(vi->version_num));
    SET(3, vi_str(vi->host));
    SET(4, PyInt_FromLong(vi->features));
    SET(5, vi_str(vi->ssl_version));
    SET(6, PyInt_FromLong(vi->ssl_version_num));
    SET(7, vi_str(vi->libz_version));
    SET(8, protocols);
    SET(9, vi_str(vi->ares));
    SET(10, PyInt_FromLong(vi->ares_num));
    SET(11, vi_str(vi->libidn));
#undef SET
    return ret;

error:
    Py_XDECREF(ret);
    Py_XDECREF(protocols);
    return NULL;
}


/* Helper functions for inserting constants into the module namespace */

static int
insobj2(PyObject *dict1, PyObject *dict2, char *name, PyObject *value)
{
    /* Insert an object into one or two dicts. Eats a reference to value.
     * See also the implementation of PyDict_SetItemString(). */
    PyObject *key = NULL;

    if (dict1 == NULL && dict2 == NULL)
        goto error;
    if (value == NULL)
        goto error;

    key = PyText_FromString(name);

    if (key == NULL)
        goto error;
#if 0
    PyString_InternInPlace(&key);   /* XXX Should we really? */
#endif
    if (dict1 != NULL) {
#if !defined(NDEBUG)
        if (PyDict_GetItem(dict1, key) != NULL) {
            fprintf(stderr, "Symbol already defined: %s\n", name);
            assert(0);
        }
#endif
        if (PyDict_SetItem(dict1, key, value) != 0)
            goto error;
    }
    if (dict2 != NULL && dict2 != dict1) {
        assert(PyDict_GetItem(dict2, key) == NULL);
        if (PyDict_SetItem(dict2, key, value) != 0)
            goto error;
    }
    Py_DECREF(key);
    Py_DECREF(value);
    return 0;

error:
    Py_XDECREF(key);
    return -1;
}

#define insobj2_modinit(dict1, dict2, name, value) \
    if (insobj2(dict1, dict2, name, value) < 0) \
        goto error


static int
insstr(PyObject *d, char *name, char *value)
{
    PyObject *v;
    int rv;

    v = PyText_FromString(value);
    if (v == NULL)
        return -1;

    rv = insobj2(d, NULL, name, v);
    if (rv < 0) {
        Py_DECREF(v);
    }
    return rv;
}

#define insstr_modinit(d, name, value) \
    do { \
        if (insstr(d, name, value) < 0) \
            goto error; \
    } while(0)

static int
insint_worker(PyObject *d, PyObject *extra, char *name, long value)
{
    PyObject *v = PyInt_FromLong(value);
    if (v == NULL)
        return -1;
    if (insobj2(d, extra, name, v) < 0) {
        Py_DECREF(v);
        return -1;
    }
    return 0;
}

#define insint(d, name, value) \
    do { \
        if (insint_worker(d, NULL, name, value) < 0) \
            goto error; \
    } while(0)

#define insint_c(d, name, value) \
    do { \
        if (insint_worker(d, curlobject_constants, name, value) < 0) \
            goto error; \
    } while(0)

#define insint_m(d, name, value) \
    do { \
        if (insint_worker(d, curlmultiobject_constants, name, value) < 0) \
            goto error; \
    } while(0)

#define insint_s(d, name, value) \
    do { \
        if (insint_worker(d, curlshareobject_constants, name, value) < 0) \
            goto error; \
    } while(0)


#ifdef PYCURL_AUTODETECT_CA
/* This autodetection of cainfo/capath is primarily intended for use in Linux
   wheels where we build one copy of libcurl that is used on multiple Linux
   distributions.
*/
PYCURL_INTERNAL char* g_pycurl_autodetected_cainfo = NULL;
PYCURL_INTERNAL char* g_pycurl_autodetected_capath = NULL;
static char *cainfos[] = {
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/usr/share/ssl/certs/ca-bundle.crt",
    "/usr/local/share/certs/ca-root-nss.crt",
    "/etc/ssl/cert.pem",
};
static char *capaths[] = {
    "/etc/ssl/certs",
};

static void pycurl_autodetect_ca()
{
    size_t i;
    size_t ninfos = sizeof(cainfos) / sizeof(cainfos[0]);
    size_t npaths = sizeof(capaths) / sizeof(capaths[0]);
    struct stat stat_buf;

    for (i = 0; i < ninfos; i++) {
        if (stat(cainfos[i], &stat_buf) == 0 && S_ISREG(stat_buf.st_mode)) {
            g_pycurl_autodetected_cainfo = cainfos[i];
            return;
        }
    }

    for (i = 0; i < npaths; i++) {
        if (stat(capaths[i], &stat_buf) == 0 && S_ISDIR(stat_buf.st_mode)) {
            g_pycurl_autodetected_capath = capaths[i];
            return;
        }
    }
}
#endif


#if PY_MAJOR_VERSION >= 3
/* Used in Python 3 only, and even then this function seems to never get
 * called. Python 2 has no module cleanup:
 * http://stackoverflow.com/questions/20741856/run-a-function-when-a-c-extension-module-is-freed-on-python-2
 */
static void do_curlmod_free(void *unused) {
    PyMem_Free(g_pycurl_useragent);
    g_pycurl_useragent = NULL;
}

static PyModuleDef curlmodule = {
    PyModuleDef_HEAD_INIT,
    "pycurl",           /* m_name */
    pycurl_module_doc,  /* m_doc */
    -1,                 /* m_size */
    curl_methods,       /* m_methods */
    NULL,               /* m_reload */
    NULL,               /* m_traverse */
    NULL,               /* m_clear */
    do_curlmod_free     /* m_free */
};
#endif


#if PY_MAJOR_VERSION >= 3
#define PYCURL_MODINIT_RETURN_NULL return NULL
PyMODINIT_FUNC PyInit_pycurl(void)
#else
#define PYCURL_MODINIT_RETURN_NULL return
/* Initialization function for the module */
#if defined(PyMODINIT_FUNC)
PyMODINIT_FUNC
#else
#if defined(__cplusplus)
extern "C"
#endif
DL_EXPORT(void)
#endif
initpycurl(void)
#endif
{
    PyObject *m, *d;
    const curl_version_info_data *vi;
    const char *libcurl_version;
    size_t libcurl_version_len, pycurl_version_len;
    PyObject *xio_module = NULL;
    PyObject *collections_module = NULL;
    PyObject *named_tuple = NULL;
    PyObject *arglist = NULL;
#ifdef HAVE_CURL_GLOBAL_SSLSET
    const curl_ssl_backend **ssllist = NULL;
    CURLsslset sslset;
    int i, runtime_supported_backend_found = 0;
    char backends[200];
    size_t backends_len = 0;
#else
    const char *runtime_ssl_lib;
#endif

    assert(Curl_Type.tp_weaklistoffset > 0);
    assert(CurlMulti_Type.tp_weaklistoffset > 0);
    assert(CurlShare_Type.tp_weaklistoffset > 0);

    /* Check the version, as this has caused nasty problems in
     * some cases. */
    vi = curl_version_info(CURLVERSION_NOW);
    if (vi == NULL) {
        PyErr_SetString(PyExc_ImportError, "pycurl: curl_version_info() failed");
        goto error;
    }
    if (vi->version_num < LIBCURL_VERSION_NUM) {
        PyErr_Format(PyExc_ImportError, "pycurl: libcurl link-time version (%s) is older than compile-time version (%s)", vi->version, LIBCURL_VERSION);
        goto error;
    }

    /* Our compiled crypto locks should correspond to runtime ssl library. */
#ifdef HAVE_CURL_GLOBAL_SSLSET
    sslset = curl_global_sslset(-1, COMPILE_SSL_LIB, &ssllist);
    if (sslset != CURLSSLSET_OK) {
        if (sslset == CURLSSLSET_NO_BACKENDS) {
            strcpy(backends, "none");
        } else {
            for (i = 0; ssllist[i] != NULL; i++) {
                switch (ssllist[i]->id) {
                case CURLSSLBACKEND_OPENSSL:
                case CURLSSLBACKEND_GNUTLS:
                case CURLSSLBACKEND_NSS:
                case CURLSSLBACKEND_WOLFSSL:
                case CURLSSLBACKEND_SCHANNEL:
                case CURLSSLBACKEND_MBEDTLS:
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 64, 1)
                case CURLSSLBACKEND_SECURETRANSPORT:
#else
                case CURLSSLBACKEND_DARWINSSL:
#endif
                    runtime_supported_backend_found = 1;
                    break;
                default:
                    break;
                }
                if (backends_len < sizeof(backends)) {
                    backends_len += snprintf(backends + backends_len, sizeof(backends) - backends_len, "%s%s", (i > 0) ? ", " : "", ssllist[i]->name);
                }
            }
        }
        /* Don't error if both the curl library and pycurl itself is compiled without SSL */
        if (runtime_supported_backend_found || COMPILE_SUPPORTED_SSL_BACKEND_FOUND) {
            PyErr_Format(PyExc_ImportError, "pycurl: libcurl link-time ssl backends (%s) do not include compile-time ssl backend (%s)", backends, COMPILE_SSL_LIB);
            goto error;
        }
    }
#else
    if (vi->ssl_version == NULL) {
        runtime_ssl_lib = "none/other";
    } else if (!strncmp(vi->ssl_version, "OpenSSL/", 8) || !strncmp(vi->ssl_version, "LibreSSL/", 9) ||
               !strncmp(vi->ssl_version, "BoringSSL", 9)) {
        runtime_ssl_lib = "openssl";
    } else if (!strncmp(vi->ssl_version, "wolfSSL/", 8)) {
        runtime_ssl_lib = "wolfssl";
    } else if (!strncmp(vi->ssl_version, "GnuTLS/", 7)) {
        runtime_ssl_lib = "gnutls";
    } else if (!strncmp(vi->ssl_version, "NSS/", 4)) {
        runtime_ssl_lib = "nss";
    } else if (!strncmp(vi->ssl_version, "mbedTLS/", 8)) {
        runtime_ssl_lib = "mbedtls";
    } else if (!strncmp(vi->ssl_version, "Secure Transport", 16)) {
        runtime_ssl_lib = "secure-transport";
    } else if (!strncmp(vi->ssl_version, "Schannel", 8)) {
        runtime_ssl_lib = "schannel";
    } else {
        runtime_ssl_lib = "none/other";
    }
    if (strcmp(runtime_ssl_lib, COMPILE_SSL_LIB)) {
        PyErr_Format(PyExc_ImportError, "pycurl: libcurl link-time ssl backend (%s) is different from compile-time ssl backend (%s)", runtime_ssl_lib, COMPILE_SSL_LIB);
        goto error;
    }
#endif

    /* Initialize the type of the new type objects here; doing it here
     * is required for portability to Windows without requiring C++. */
    p_Curl_Type = &Curl_Type;
    p_CurlSlist_Type = &CurlSlist_Type;
    p_CurlHttppost_Type = &CurlHttppost_Type;
    p_CurlMulti_Type = &CurlMulti_Type;
    p_CurlShare_Type = &CurlShare_Type;
    Py_SET_TYPE(&Curl_Type, &PyType_Type);
    Py_SET_TYPE(&CurlSlist_Type, &PyType_Type);
    Py_SET_TYPE(&CurlHttppost_Type, &PyType_Type);
    Py_SET_TYPE(&CurlMulti_Type, &PyType_Type);
    Py_SET_TYPE(&CurlShare_Type, &PyType_Type);

    /* Create the module and add the functions */
    if (PyType_Ready(&Curl_Type) < 0)
        goto error;

    if (PyType_Ready(&CurlSlist_Type) < 0)
        goto error;

    if (PyType_Ready(&CurlHttppost_Type) < 0)
        goto error;

    if (PyType_Ready(&CurlMulti_Type) < 0)
        goto error;

    if (PyType_Ready(&CurlShare_Type) < 0)
        goto error;


#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&curlmodule);
    if (m == NULL)
        goto error;
#else
    /* returns a borrowed reference, XDECREFing it crashes the interpreter */
    m = Py_InitModule3("pycurl", curl_methods, pycurl_module_doc);
    if (m == NULL || !PyModule_Check(m))
        goto error;
#endif

    /* Add error object to the module */
    d = PyModule_GetDict(m);
    assert(d != NULL);
    ErrorObject = PyErr_NewException("pycurl.error", NULL, NULL);
    if (ErrorObject == NULL)
        goto error;
    if (PyDict_SetItemString(d, "error", ErrorObject) < 0) {
        goto error;
    }

    curlobject_constants = PyDict_New();
    if (curlobject_constants == NULL)
        goto error;

    curlmultiobject_constants = PyDict_New();
    if (curlmultiobject_constants == NULL)
        goto error;

    curlshareobject_constants = PyDict_New();
    if (curlshareobject_constants == NULL)
        goto error;

    /* Add version strings to the module */
    libcurl_version = curl_version();
    libcurl_version_len = strlen(libcurl_version);
#define PYCURL_VERSION_PREFIX_SIZE sizeof(PYCURL_VERSION_PREFIX)
    /* PYCURL_VERSION_PREFIX_SIZE includes terminating null which will be
     * replaced with the space; libcurl_version_len does not include
     * terminating null. */
    pycurl_version_len = PYCURL_VERSION_PREFIX_SIZE + libcurl_version_len + 1;
    g_pycurl_useragent = PyMem_New(char, pycurl_version_len);
    if (g_pycurl_useragent == NULL)
        goto error;
    memcpy(g_pycurl_useragent, PYCURL_VERSION_PREFIX, PYCURL_VERSION_PREFIX_SIZE);
    g_pycurl_useragent[PYCURL_VERSION_PREFIX_SIZE-1] = ' ';
    memcpy(g_pycurl_useragent + PYCURL_VERSION_PREFIX_SIZE,
        libcurl_version, libcurl_version_len);
    g_pycurl_useragent[pycurl_version_len - 1] = 0;
#undef PYCURL_VERSION_PREFIX_SIZE

    insstr_modinit(d, "version", g_pycurl_useragent);
    insint(d, "COMPILE_PY_VERSION_HEX", PY_VERSION_HEX);
    insint(d, "COMPILE_LIBCURL_VERSION_NUM", LIBCURL_VERSION_NUM);
    insstr_modinit(d, "COMPILE_SSL_LIB", COMPILE_SSL_LIB);

    /* Types */
    insobj2_modinit(d, NULL, "Curl", (PyObject *) p_Curl_Type);
    insobj2_modinit(d, NULL, "CurlMulti", (PyObject *) p_CurlMulti_Type);
    insobj2_modinit(d, NULL, "CurlShare", (PyObject *) p_CurlShare_Type);

    /**
     ** the order of these constants mostly follows <curl/curl.h>
     **/

    /* Abort curl_read_callback(). */
    insint_c(d, "READFUNC_ABORT", CURL_READFUNC_ABORT);
    insint_c(d, "READFUNC_PAUSE", CURL_READFUNC_PAUSE);

    /* Pause curl_write_callback(). */
    insint_c(d, "WRITEFUNC_PAUSE", CURL_WRITEFUNC_PAUSE);

    /* constants for ioctl callback return values */
    insint_c(d, "IOE_OK", CURLIOE_OK);
    insint_c(d, "IOE_UNKNOWNCMD", CURLIOE_UNKNOWNCMD);
    insint_c(d, "IOE_FAILRESTART", CURLIOE_FAILRESTART);

    /* constants for ioctl callback argument values */
    insint_c(d, "IOCMD_NOP", CURLIOCMD_NOP);
    insint_c(d, "IOCMD_RESTARTREAD", CURLIOCMD_RESTARTREAD);

    /* opensocketfunction return value */
    insint_c(d, "SOCKET_BAD", CURL_SOCKET_BAD);

    /* curl_infotype: the kind of data that is passed to information_callback */
/* XXX do we actually need curl_infotype in pycurl ??? */
    insint_c(d, "INFOTYPE_TEXT", CURLINFO_TEXT);
    insint_c(d, "INFOTYPE_HEADER_IN", CURLINFO_HEADER_IN);
    insint_c(d, "INFOTYPE_HEADER_OUT", CURLINFO_HEADER_OUT);
    insint_c(d, "INFOTYPE_DATA_IN", CURLINFO_DATA_IN);
    insint_c(d, "INFOTYPE_DATA_OUT", CURLINFO_DATA_OUT);
    insint_c(d, "INFOTYPE_SSL_DATA_IN", CURLINFO_SSL_DATA_IN);
    insint_c(d, "INFOTYPE_SSL_DATA_OUT", CURLINFO_SSL_DATA_OUT);

    /* CURLcode: error codes */
    insint_c(d, "E_OK", CURLE_OK);
    insint_c(d, "E_AGAIN", CURLE_AGAIN);
    insint_c(d, "E_ALREADY_COMPLETE", CURLE_ALREADY_COMPLETE);
    insint_c(d, "E_BAD_CALLING_ORDER", CURLE_BAD_CALLING_ORDER);
    insint_c(d, "E_BAD_PASSWORD_ENTERED", CURLE_BAD_PASSWORD_ENTERED);
    insint_c(d, "E_FTP_BAD_DOWNLOAD_RESUME", CURLE_FTP_BAD_DOWNLOAD_RESUME);
    insint_c(d, "E_FTP_COULDNT_SET_TYPE", CURLE_FTP_COULDNT_SET_TYPE);
    insint_c(d, "E_FTP_PARTIAL_FILE", CURLE_FTP_PARTIAL_FILE);
    insint_c(d, "E_FTP_USER_PASSWORD_INCORRECT", CURLE_FTP_USER_PASSWORD_INCORRECT);
    insint_c(d, "E_HTTP_NOT_FOUND", CURLE_HTTP_NOT_FOUND);
    insint_c(d, "E_HTTP_PORT_FAILED", CURLE_HTTP_PORT_FAILED);
    insint_c(d, "E_MALFORMAT_USER", CURLE_MALFORMAT_USER);
    insint_c(d, "E_QUOTE_ERROR", CURLE_QUOTE_ERROR);
    insint_c(d, "E_RANGE_ERROR", CURLE_RANGE_ERROR);
    insint_c(d, "E_REMOTE_ACCESS_DENIED", CURLE_REMOTE_ACCESS_DENIED);
    insint_c(d, "E_REMOTE_DISK_FULL", CURLE_REMOTE_DISK_FULL);
    insint_c(d, "E_REMOTE_FILE_EXISTS", CURLE_REMOTE_FILE_EXISTS);
    insint_c(d, "E_UPLOAD_FAILED", CURLE_UPLOAD_FAILED);
    insint_c(d, "E_URL_MALFORMAT_USER", CURLE_URL_MALFORMAT_USER);
    insint_c(d, "E_USE_SSL_FAILED", CURLE_USE_SSL_FAILED);
    insint_c(d, "E_UNSUPPORTED_PROTOCOL", CURLE_UNSUPPORTED_PROTOCOL);
    insint_c(d, "E_FAILED_INIT", CURLE_FAILED_INIT);
    insint_c(d, "E_URL_MALFORMAT", CURLE_URL_MALFORMAT);
#ifdef HAVE_CURL_7_21_5
    insint_c(d, "E_NOT_BUILT_IN", CURLE_NOT_BUILT_IN);
#endif
    insint_c(d, "E_COULDNT_RESOLVE_PROXY", CURLE_COULDNT_RESOLVE_PROXY);
    insint_c(d, "E_COULDNT_RESOLVE_HOST", CURLE_COULDNT_RESOLVE_HOST);
    insint_c(d, "E_COULDNT_CONNECT", CURLE_COULDNT_CONNECT);
    insint_c(d, "E_FTP_WEIRD_SERVER_REPLY", CURLE_FTP_WEIRD_SERVER_REPLY);
    insint_c(d, "E_FTP_ACCESS_DENIED", CURLE_FTP_ACCESS_DENIED);
#ifdef HAVE_CURL_7_24_0
    insint_c(d, "E_FTP_ACCEPT_FAILED", CURLE_FTP_ACCEPT_FAILED);
#endif
    insint_c(d, "E_FTP_WEIRD_PASS_REPLY", CURLE_FTP_WEIRD_PASS_REPLY);
    insint_c(d, "E_FTP_WEIRD_USER_REPLY", CURLE_FTP_WEIRD_USER_REPLY);
    insint_c(d, "E_FTP_WEIRD_PASV_REPLY", CURLE_FTP_WEIRD_PASV_REPLY);
    insint_c(d, "E_FTP_WEIRD_227_FORMAT", CURLE_FTP_WEIRD_227_FORMAT);
    insint_c(d, "E_FTP_CANT_GET_HOST", CURLE_FTP_CANT_GET_HOST);
    insint_c(d, "E_FTP_CANT_RECONNECT", CURLE_FTP_CANT_RECONNECT);
    insint_c(d, "E_FTP_COULDNT_SET_BINARY", CURLE_FTP_COULDNT_SET_BINARY);
    insint_c(d, "E_PARTIAL_FILE", CURLE_PARTIAL_FILE);
    insint_c(d, "E_FTP_COULDNT_RETR_FILE", CURLE_FTP_COULDNT_RETR_FILE);
    insint_c(d, "E_FTP_WRITE_ERROR", CURLE_FTP_WRITE_ERROR);
    insint_c(d, "E_FTP_QUOTE_ERROR", CURLE_FTP_QUOTE_ERROR);
    insint_c(d, "E_HTTP_RETURNED_ERROR", CURLE_HTTP_RETURNED_ERROR);
    insint_c(d, "E_WRITE_ERROR", CURLE_WRITE_ERROR);
    insint_c(d, "E_FTP_COULDNT_STOR_FILE", CURLE_FTP_COULDNT_STOR_FILE);
    insint_c(d, "E_READ_ERROR", CURLE_READ_ERROR);
    insint_c(d, "E_OUT_OF_MEMORY", CURLE_OUT_OF_MEMORY);
    insint_c(d, "E_OPERATION_TIMEOUTED", CURLE_OPERATION_TIMEOUTED);
    insint_c(d, "E_OPERATION_TIMEDOUT", CURLE_OPERATION_TIMEDOUT);
    insint_c(d, "E_FTP_COULDNT_SET_ASCII", CURLE_FTP_COULDNT_SET_ASCII);
    insint_c(d, "E_FTP_PORT_FAILED", CURLE_FTP_PORT_FAILED);
    insint_c(d, "E_FTP_COULDNT_USE_REST", CURLE_FTP_COULDNT_USE_REST);
    insint_c(d, "E_FTP_COULDNT_GET_SIZE", CURLE_FTP_COULDNT_GET_SIZE);
    insint_c(d, "E_HTTP_RANGE_ERROR", CURLE_HTTP_RANGE_ERROR);
    insint_c(d, "E_HTTP_POST_ERROR", CURLE_HTTP_POST_ERROR);
    insint_c(d, "E_SSL_CACERT", CURLE_SSL_CACERT);
    insint_c(d, "E_SSL_CACERT_BADFILE", CURLE_SSL_CACERT_BADFILE);
    insint_c(d, "E_SSL_CERTPROBLEM", CURLE_SSL_CERTPROBLEM);
    insint_c(d, "E_SSL_CIPHER", CURLE_SSL_CIPHER);
    insint_c(d, "E_SSL_CONNECT_ERROR", CURLE_SSL_CONNECT_ERROR);
    insint_c(d, "E_SSL_CRL_BADFILE", CURLE_SSL_CRL_BADFILE);
    insint_c(d, "E_SSL_ENGINE_INITFAILED", CURLE_SSL_ENGINE_INITFAILED);
    insint_c(d, "E_SSL_ENGINE_NOTFOUND", CURLE_SSL_ENGINE_NOTFOUND);
    insint_c(d, "E_SSL_ENGINE_SETFAILED", CURLE_SSL_ENGINE_SETFAILED);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 41, 0)
    insint_c(d, "E_SSL_INVALIDCERTSTATUS", CURLE_SSL_INVALIDCERTSTATUS);
#endif
    insint_c(d, "E_SSL_ISSUER_ERROR", CURLE_SSL_ISSUER_ERROR);
    insint_c(d, "E_SSL_PEER_CERTIFICATE", CURLE_SSL_PEER_CERTIFICATE);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 39, 0)
    insint_c(d, "E_SSL_PINNEDPUBKEYNOTMATCH", CURLE_SSL_PINNEDPUBKEYNOTMATCH);
#endif
    insint_c(d, "E_SSL_SHUTDOWN_FAILED", CURLE_SSL_SHUTDOWN_FAILED);
    insint_c(d, "E_BAD_DOWNLOAD_RESUME", CURLE_BAD_DOWNLOAD_RESUME);
    insint_c(d, "E_FILE_COULDNT_READ_FILE", CURLE_FILE_COULDNT_READ_FILE);
    insint_c(d, "E_LDAP_CANNOT_BIND", CURLE_LDAP_CANNOT_BIND);
    insint_c(d, "E_LDAP_SEARCH_FAILED", CURLE_LDAP_SEARCH_FAILED);
    insint_c(d, "E_LIBRARY_NOT_FOUND", CURLE_LIBRARY_NOT_FOUND);
    insint_c(d, "E_FUNCTION_NOT_FOUND", CURLE_FUNCTION_NOT_FOUND);
    insint_c(d, "E_ABORTED_BY_CALLBACK", CURLE_ABORTED_BY_CALLBACK);
    insint_c(d, "E_BAD_FUNCTION_ARGUMENT", CURLE_BAD_FUNCTION_ARGUMENT);
    insint_c(d, "E_INTERFACE_FAILED", CURLE_INTERFACE_FAILED);
    insint_c(d, "E_TOO_MANY_REDIRECTS", CURLE_TOO_MANY_REDIRECTS);
#ifdef HAVE_CURL_7_21_5
    insint_c(d, "E_UNKNOWN_OPTION", CURLE_UNKNOWN_OPTION);
#endif
    /* same as E_UNKNOWN_OPTION */
    insint_c(d, "E_UNKNOWN_TELNET_OPTION", CURLE_UNKNOWN_TELNET_OPTION);
    insint_c(d, "E_TELNET_OPTION_SYNTAX", CURLE_TELNET_OPTION_SYNTAX);
    insint_c(d, "E_GOT_NOTHING", CURLE_GOT_NOTHING);
    insint_c(d, "E_SEND_ERROR", CURLE_SEND_ERROR);
    insint_c(d, "E_RECV_ERROR", CURLE_RECV_ERROR);
    insint_c(d, "E_SHARE_IN_USE", CURLE_SHARE_IN_USE);
    insint_c(d, "E_BAD_CONTENT_ENCODING", CURLE_BAD_CONTENT_ENCODING);
    insint_c(d, "E_LDAP_INVALID_URL", CURLE_LDAP_INVALID_URL);
    insint_c(d, "E_FILESIZE_EXCEEDED", CURLE_FILESIZE_EXCEEDED);
    insint_c(d, "E_FTP_SSL_FAILED", CURLE_FTP_SSL_FAILED);
    insint_c(d, "E_SEND_FAIL_REWIND", CURLE_SEND_FAIL_REWIND);
    insint_c(d, "E_LOGIN_DENIED", CURLE_LOGIN_DENIED);
    insint_c(d, "E_PEER_FAILED_VERIFICATION", CURLE_PEER_FAILED_VERIFICATION);
    insint_c(d, "E_TFTP_NOTFOUND", CURLE_TFTP_NOTFOUND);
    insint_c(d, "E_TFTP_PERM", CURLE_TFTP_PERM);
    insint_c(d, "E_TFTP_DISKFULL", CURLE_TFTP_DISKFULL);
    insint_c(d, "E_TFTP_ILLEGAL", CURLE_TFTP_ILLEGAL);
    insint_c(d, "E_TFTP_UNKNOWNID", CURLE_TFTP_UNKNOWNID);
    insint_c(d, "E_TFTP_EXISTS", CURLE_TFTP_EXISTS);
    insint_c(d, "E_TFTP_NOSUCHUSER", CURLE_TFTP_NOSUCHUSER);
    insint_c(d, "E_CONV_FAILED", CURLE_CONV_FAILED);
    insint_c(d, "E_CONV_REQD", CURLE_CONV_REQD);
    insint_c(d, "E_REMOTE_FILE_NOT_FOUND", CURLE_REMOTE_FILE_NOT_FOUND);
    insint_c(d, "E_SSH", CURLE_SSH);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    insint_c(d, "E_FTP_PRET_FAILED", CURLE_FTP_PRET_FAILED);
    insint_c(d, "E_RTSP_CSEQ_ERROR", CURLE_RTSP_CSEQ_ERROR);
    insint_c(d, "E_RTSP_SESSION_ERROR", CURLE_RTSP_SESSION_ERROR);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 0)
    insint_c(d, "E_CHUNK_FAILED", CURLE_CHUNK_FAILED);
    insint_c(d, "E_FTP_BAD_FILE_LIST", CURLE_FTP_BAD_FILE_LIST);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 24, 0)
    insint_c(d, "E_FTP_ACCEPT_TIMEOUT", CURLE_FTP_ACCEPT_TIMEOUT);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 30, 0)
    insint_c(d, "E_NO_CONNECTION_AVAILABLE", CURLE_NO_CONNECTION_AVAILABLE);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 38, 0)
    insint_c(d, "E_HTTP2", CURLE_HTTP2);
#endif

    /* curl_proxytype: constants for setopt(PROXYTYPE, x) */
    insint_c(d, "PROXYTYPE_HTTP", CURLPROXY_HTTP);
#ifdef HAVE_CURL_7_19_4_OPTS
    insint_c(d, "PROXYTYPE_HTTP_1_0", CURLPROXY_HTTP_1_0);
#endif
    insint_c(d, "PROXYTYPE_SOCKS4", CURLPROXY_SOCKS4);
    insint_c(d, "PROXYTYPE_SOCKS4A", CURLPROXY_SOCKS4A);
    insint_c(d, "PROXYTYPE_SOCKS5", CURLPROXY_SOCKS5);
    insint_c(d, "PROXYTYPE_SOCKS5_HOSTNAME", CURLPROXY_SOCKS5_HOSTNAME);

    /* curl_httpauth: constants for setopt(HTTPAUTH, x) */
    insint_c(d, "HTTPAUTH_ANY", CURLAUTH_ANY);
    insint_c(d, "HTTPAUTH_ANYSAFE", CURLAUTH_ANYSAFE);
    insint_c(d, "HTTPAUTH_BASIC", CURLAUTH_BASIC);
    insint_c(d, "HTTPAUTH_DIGEST", CURLAUTH_DIGEST);
#ifdef HAVE_CURLAUTH_DIGEST_IE
    insint_c(d, "HTTPAUTH_DIGEST_IE", CURLAUTH_DIGEST_IE);
#endif
    insint_c(d, "HTTPAUTH_GSSNEGOTIATE", CURLAUTH_GSSNEGOTIATE);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 38, 0)
    insint_c(d, "HTTPAUTH_NEGOTIATE", CURLAUTH_NEGOTIATE);
#endif
    insint_c(d, "HTTPAUTH_NTLM", CURLAUTH_NTLM);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 22, 0)
    insint_c(d, "HTTPAUTH_NTLM_WB", CURLAUTH_NTLM_WB);
#endif
    insint_c(d, "HTTPAUTH_NONE", CURLAUTH_NONE);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 3)
    insint_c(d, "HTTPAUTH_ONLY", CURLAUTH_ONLY);
#endif

#ifdef HAVE_CURL_7_22_0_OPTS
    insint_c(d, "GSSAPI_DELEGATION_FLAG", CURLGSSAPI_DELEGATION_FLAG);
    insint_c(d, "GSSAPI_DELEGATION_NONE", CURLGSSAPI_DELEGATION_NONE);
    insint_c(d, "GSSAPI_DELEGATION_POLICY_FLAG", CURLGSSAPI_DELEGATION_POLICY_FLAG);

    insint_c(d, "GSSAPI_DELEGATION", CURLOPT_GSSAPI_DELEGATION);
#endif

    /* curl_ftpssl: constants for setopt(FTP_SSL, x) */
    insint_c(d, "FTPSSL_NONE", CURLFTPSSL_NONE);
    insint_c(d, "FTPSSL_TRY", CURLFTPSSL_TRY);
    insint_c(d, "FTPSSL_CONTROL", CURLFTPSSL_CONTROL);
    insint_c(d, "FTPSSL_ALL", CURLFTPSSL_ALL);

    /* curl_ftpauth: constants for setopt(FTPSSLAUTH, x) */
    insint_c(d, "FTPAUTH_DEFAULT", CURLFTPAUTH_DEFAULT);
    insint_c(d, "FTPAUTH_SSL", CURLFTPAUTH_SSL);
    insint_c(d, "FTPAUTH_TLS", CURLFTPAUTH_TLS);

    /* curl_ftpauth: constants for setopt(FTPSSLAUTH, x) */
    insint_c(d, "FORM_BUFFER", CURLFORM_BUFFER);
    insint_c(d, "FORM_BUFFERPTR", CURLFORM_BUFFERPTR);
    insint_c(d, "FORM_CONTENTS", CURLFORM_COPYCONTENTS);
    insint_c(d, "FORM_FILE", CURLFORM_FILE);
    insint_c(d, "FORM_CONTENTTYPE", CURLFORM_CONTENTTYPE);
    insint_c(d, "FORM_FILENAME", CURLFORM_FILENAME);

    /* FTP_FILEMETHOD options */
    insint_c(d, "FTPMETHOD_DEFAULT", CURLFTPMETHOD_DEFAULT);
    insint_c(d, "FTPMETHOD_MULTICWD", CURLFTPMETHOD_MULTICWD);
    insint_c(d, "FTPMETHOD_NOCWD", CURLFTPMETHOD_NOCWD);
    insint_c(d, "FTPMETHOD_SINGLECWD", CURLFTPMETHOD_SINGLECWD);

    /* CURLoption: symbolic constants for setopt() */
    insint_c(d, "APPEND", CURLOPT_APPEND);
    insint_c(d, "COOKIESESSION", CURLOPT_COOKIESESSION);
    insint_c(d, "DIRLISTONLY", CURLOPT_DIRLISTONLY);
    /* ERRORBUFFER is not supported */
    insint_c(d, "FILE", CURLOPT_WRITEDATA);
    insint_c(d, "FTPPORT", CURLOPT_FTPPORT);
    insint_c(d, "INFILE", CURLOPT_READDATA);
    insint_c(d, "INFILESIZE", CURLOPT_INFILESIZE_LARGE);    /* _LARGE ! */
    insint_c(d, "KEYPASSWD", CURLOPT_KEYPASSWD);
    insint_c(d, "LOW_SPEED_LIMIT", CURLOPT_LOW_SPEED_LIMIT);
    insint_c(d, "LOW_SPEED_TIME", CURLOPT_LOW_SPEED_TIME);
    insint_c(d, "PORT", CURLOPT_PORT);
    insint_c(d, "POSTFIELDS", CURLOPT_POSTFIELDS);
    insint_c(d, "PROXY", CURLOPT_PROXY);
#ifdef HAVE_CURLOPT_PROXYUSERNAME
    insint_c(d, "PROXYPASSWORD", CURLOPT_PROXYPASSWORD);
    insint_c(d, "PROXYUSERNAME", CURLOPT_PROXYUSERNAME);
#endif
    insint_c(d, "PROXYUSERPWD", CURLOPT_PROXYUSERPWD);
    insint_c(d, "RANGE", CURLOPT_RANGE);
    insint_c(d, "READFUNCTION", CURLOPT_READFUNCTION);
    insint_c(d, "REFERER", CURLOPT_REFERER);
    insint_c(d, "RESUME_FROM", CURLOPT_RESUME_FROM_LARGE);  /* _LARGE ! */
    insint_c(d, "TELNETOPTIONS", CURLOPT_TELNETOPTIONS);
    insint_c(d, "TIMEOUT", CURLOPT_TIMEOUT);
    insint_c(d, "URL", CURLOPT_URL);
    insint_c(d, "USE_SSL", CURLOPT_USE_SSL);
    insint_c(d, "USERAGENT", CURLOPT_USERAGENT);
    insint_c(d, "USERPWD", CURLOPT_USERPWD);
    insint_c(d, "WRITEFUNCTION", CURLOPT_WRITEFUNCTION);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    insint_c(d, "OPT_RTSP_CLIENT_CSEQ", CURLOPT_RTSP_CLIENT_CSEQ);
    insint_c(d, "OPT_RTSP_REQUEST", CURLOPT_RTSP_REQUEST);
    insint_c(d, "OPT_RTSP_SERVER_CSEQ", CURLOPT_RTSP_SERVER_CSEQ);
    insint_c(d, "OPT_RTSP_SESSION_ID", CURLOPT_RTSP_SESSION_ID);
    insint_c(d, "OPT_RTSP_STREAM_URI", CURLOPT_RTSP_STREAM_URI);
    insint_c(d, "OPT_RTSP_TRANSPORT", CURLOPT_RTSP_TRANSPORT);
#endif
#ifdef HAVE_CURLOPT_USERNAME
    insint_c(d, "USERNAME", CURLOPT_USERNAME);
    insint_c(d, "PASSWORD", CURLOPT_PASSWORD);
#endif
    insint_c(d, "WRITEDATA", CURLOPT_WRITEDATA);
    insint_c(d, "READDATA", CURLOPT_READDATA);
    insint_c(d, "PROXYPORT", CURLOPT_PROXYPORT);
    insint_c(d, "HTTPPROXYTUNNEL", CURLOPT_HTTPPROXYTUNNEL);
    insint_c(d, "VERBOSE", CURLOPT_VERBOSE);
    insint_c(d, "HEADER", CURLOPT_HEADER);
    insint_c(d, "NOPROGRESS", CURLOPT_NOPROGRESS);
    insint_c(d, "NOBODY", CURLOPT_NOBODY);
    insint_c(d, "FAILONERROR", CURLOPT_FAILONERROR);
    insint_c(d, "UPLOAD", CURLOPT_UPLOAD);
    insint_c(d, "POST", CURLOPT_POST);
    insint_c(d, "FTPLISTONLY", CURLOPT_FTPLISTONLY);
    insint_c(d, "FTPAPPEND", CURLOPT_FTPAPPEND);
    insint_c(d, "NETRC", CURLOPT_NETRC);
    insint_c(d, "FOLLOWLOCATION", CURLOPT_FOLLOWLOCATION);
    insint_c(d, "TRANSFERTEXT", CURLOPT_TRANSFERTEXT);
    insint_c(d, "PUT", CURLOPT_PUT);
    insint_c(d, "POSTFIELDSIZE", CURLOPT_POSTFIELDSIZE_LARGE);  /* _LARGE ! */
    insint_c(d, "COOKIE", CURLOPT_COOKIE);
    insint_c(d, "HTTPHEADER", CURLOPT_HTTPHEADER);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    insint_c(d, "PROXYHEADER", CURLOPT_PROXYHEADER);
    insint_c(d, "HEADEROPT", CURLOPT_HEADEROPT);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 42, 0)
    insint_c(d, "PATH_AS_IS", CURLOPT_PATH_AS_IS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 43, 0)
    insint_c(d, "PIPEWAIT", CURLOPT_PIPEWAIT);
#endif
    insint_c(d, "HTTPPOST", CURLOPT_HTTPPOST);
    insint_c(d, "SSLCERT", CURLOPT_SSLCERT);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 71, 0)
    insint_c(d, "SSLCERT_BLOB", CURLOPT_SSLCERT_BLOB);
    insint_c(d, "SSLKEY_BLOB", CURLOPT_SSLKEY_BLOB);
    insint_c(d, "PROXY_SSLCERT_BLOB", CURLOPT_PROXY_SSLCERT_BLOB);
    insint_c(d, "PROXY_SSLKEY_BLOB", CURLOPT_PROXY_SSLKEY_BLOB);
    insint_c(d, "ISSUERCERT_BLOB", CURLOPT_ISSUERCERT_BLOB);
    insint_c(d, "PROXY_ISSUERCERT_BLOB", CURLOPT_PROXY_ISSUERCERT_BLOB);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 77, 0)
    insint_c(d, "CAINFO_BLOB", CURLOPT_CAINFO_BLOB);
    insint_c(d, "PROXY_CAINFO_BLOB", CURLOPT_PROXY_CAINFO_BLOB);
#endif
    insint_c(d, "SSLCERTPASSWD", CURLOPT_SSLCERTPASSWD);
    insint_c(d, "CRLF", CURLOPT_CRLF);
    insint_c(d, "QUOTE", CURLOPT_QUOTE);
    insint_c(d, "POSTQUOTE", CURLOPT_POSTQUOTE);
    insint_c(d, "PREQUOTE", CURLOPT_PREQUOTE);
    insint_c(d, "WRITEHEADER", CURLOPT_WRITEHEADER);
    insint_c(d, "HEADERFUNCTION", CURLOPT_HEADERFUNCTION);
    insint_c(d, "SEEKFUNCTION", CURLOPT_SEEKFUNCTION);
    insint_c(d, "COOKIEFILE", CURLOPT_COOKIEFILE);
    insint_c(d, "SSLVERSION", CURLOPT_SSLVERSION);
    insint_c(d, "TIMECONDITION", CURLOPT_TIMECONDITION);
    insint_c(d, "TIMEVALUE", CURLOPT_TIMEVALUE);
    insint_c(d, "CUSTOMREQUEST", CURLOPT_CUSTOMREQUEST);
    insint_c(d, "STDERR", CURLOPT_STDERR);
    insint_c(d, "INTERFACE", CURLOPT_INTERFACE);
    insint_c(d, "KRB4LEVEL", CURLOPT_KRB4LEVEL);
    insint_c(d, "KRBLEVEL", CURLOPT_KRBLEVEL);
    insint_c(d, "PROGRESSFUNCTION", CURLOPT_PROGRESSFUNCTION);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 0)
    insint_c(d, "XFERINFOFUNCTION", CURLOPT_XFERINFOFUNCTION);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    insint_c(d, "FTP_USE_PRET", CURLOPT_FTP_USE_PRET);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 34, 0)
    insint_c(d, "LOGIN_OPTIONS", CURLOPT_LOGIN_OPTIONS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 31, 0)
    insint_c(d, "SASL_IR", CURLOPT_SASL_IR);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 33, 0)
    insint_c(d, "XOAUTH2_BEARER", CURLOPT_XOAUTH2_BEARER);
#endif
    insint_c(d, "SSL_VERIFYPEER", CURLOPT_SSL_VERIFYPEER);
    insint_c(d, "CAPATH", CURLOPT_CAPATH);
    insint_c(d, "CAINFO", CURLOPT_CAINFO);
    insint_c(d, "OPT_FILETIME", CURLOPT_FILETIME);
    insint_c(d, "MAXREDIRS", CURLOPT_MAXREDIRS);
    insint_c(d, "MAXCONNECTS", CURLOPT_MAXCONNECTS);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 65, 0)
    insint_c(d, "MAXAGE_CONN", CURLOPT_MAXAGE_CONN);
#endif
    insint_c(d, "FRESH_CONNECT", CURLOPT_FRESH_CONNECT);
    insint_c(d, "FORBID_REUSE", CURLOPT_FORBID_REUSE);
    insint_c(d, "RANDOM_FILE", CURLOPT_RANDOM_FILE);
    insint_c(d, "EGDSOCKET", CURLOPT_EGDSOCKET);
    insint_c(d, "CONNECTTIMEOUT", CURLOPT_CONNECTTIMEOUT);
    insint_c(d, "HTTPGET", CURLOPT_HTTPGET);
    insint_c(d, "SSL_VERIFYHOST", CURLOPT_SSL_VERIFYHOST);
    insint_c(d, "COOKIEJAR", CURLOPT_COOKIEJAR);
    insint_c(d, "SSL_CIPHER_LIST", CURLOPT_SSL_CIPHER_LIST);
    insint_c(d, "HTTP_VERSION", CURLOPT_HTTP_VERSION);
    insint_c(d, "FTP_USE_EPSV", CURLOPT_FTP_USE_EPSV);
    insint_c(d, "SSLCERTTYPE", CURLOPT_SSLCERTTYPE);
    insint_c(d, "SSLKEY", CURLOPT_SSLKEY);
    insint_c(d, "SSLKEYTYPE", CURLOPT_SSLKEYTYPE);
    /* same as CURLOPT_KEYPASSWD */
    insint_c(d, "SSLKEYPASSWD", CURLOPT_SSLKEYPASSWD);
    insint_c(d, "SSLENGINE", CURLOPT_SSLENGINE);
    insint_c(d, "SSLENGINE_DEFAULT", CURLOPT_SSLENGINE_DEFAULT);
    insint_c(d, "DNS_CACHE_TIMEOUT", CURLOPT_DNS_CACHE_TIMEOUT);
    insint_c(d, "DNS_USE_GLOBAL_CACHE", CURLOPT_DNS_USE_GLOBAL_CACHE);
    insint_c(d, "DEBUGFUNCTION", CURLOPT_DEBUGFUNCTION);
    insint_c(d, "BUFFERSIZE", CURLOPT_BUFFERSIZE);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 62, 0)
    insint_c(d, "UPLOAD_BUFFERSIZE", CURLOPT_UPLOAD_BUFFERSIZE);
#endif
    insint_c(d, "NOSIGNAL", CURLOPT_NOSIGNAL);
    insint_c(d, "SHARE", CURLOPT_SHARE);
    insint_c(d, "PROXYTYPE", CURLOPT_PROXYTYPE);
    /* superseded by ACCEPT_ENCODING */
    insint_c(d, "ENCODING", CURLOPT_ENCODING);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 6)
    insint_c(d, "ACCEPT_ENCODING", CURLOPT_ACCEPT_ENCODING);
    insint_c(d, "TRANSFER_ENCODING", CURLOPT_TRANSFER_ENCODING);
#endif
    insint_c(d, "HTTP200ALIASES", CURLOPT_HTTP200ALIASES);
    insint_c(d, "UNRESTRICTED_AUTH", CURLOPT_UNRESTRICTED_AUTH);
    insint_c(d, "FTP_USE_EPRT", CURLOPT_FTP_USE_EPRT);
    insint_c(d, "HTTPAUTH", CURLOPT_HTTPAUTH);
    insint_c(d, "FTP_CREATE_MISSING_DIRS", CURLOPT_FTP_CREATE_MISSING_DIRS);
    insint_c(d, "PROXYAUTH", CURLOPT_PROXYAUTH);
    insint_c(d, "FTP_RESPONSE_TIMEOUT", CURLOPT_FTP_RESPONSE_TIMEOUT);
    insint_c(d, "IPRESOLVE", CURLOPT_IPRESOLVE);
    insint_c(d, "MAXFILESIZE", CURLOPT_MAXFILESIZE_LARGE);  /* _LARGE ! */
    insint_c(d, "INFILESIZE_LARGE", CURLOPT_INFILESIZE_LARGE);
    insint_c(d, "RESUME_FROM_LARGE", CURLOPT_RESUME_FROM_LARGE);
    insint_c(d, "MAXFILESIZE_LARGE", CURLOPT_MAXFILESIZE_LARGE);
    insint_c(d, "NETRC_FILE", CURLOPT_NETRC_FILE);
    insint_c(d, "FTP_SSL", CURLOPT_FTP_SSL);
    insint_c(d, "POSTFIELDSIZE_LARGE", CURLOPT_POSTFIELDSIZE_LARGE);
    insint_c(d, "TCP_NODELAY", CURLOPT_TCP_NODELAY);
    insint_c(d, "FTPSSLAUTH", CURLOPT_FTPSSLAUTH);
    insint_c(d, "IOCTLFUNCTION", CURLOPT_IOCTLFUNCTION);
    insint_c(d, "OPENSOCKETFUNCTION", CURLOPT_OPENSOCKETFUNCTION);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 7)
    insint_c(d, "CLOSESOCKETFUNCTION", CURLOPT_CLOSESOCKETFUNCTION);
#endif
    insint_c(d, "SOCKOPTFUNCTION", CURLOPT_SOCKOPTFUNCTION);
    insint_c(d, "FTP_ACCOUNT", CURLOPT_FTP_ACCOUNT);
    insint_c(d, "IGNORE_CONTENT_LENGTH", CURLOPT_IGNORE_CONTENT_LENGTH);
    insint_c(d, "COOKIELIST", CURLOPT_COOKIELIST);
    insint_c(d, "OPT_COOKIELIST", CURLOPT_COOKIELIST);
    insint_c(d, "FTP_SKIP_PASV_IP", CURLOPT_FTP_SKIP_PASV_IP);
    insint_c(d, "FTP_FILEMETHOD", CURLOPT_FTP_FILEMETHOD);
    insint_c(d, "CONNECT_ONLY", CURLOPT_CONNECT_ONLY);
    insint_c(d, "LOCALPORT", CURLOPT_LOCALPORT);
    insint_c(d, "LOCALPORTRANGE", CURLOPT_LOCALPORTRANGE);
    insint_c(d, "FTP_ALTERNATIVE_TO_USER", CURLOPT_FTP_ALTERNATIVE_TO_USER);
    insint_c(d, "MAX_SEND_SPEED_LARGE", CURLOPT_MAX_SEND_SPEED_LARGE);
    insint_c(d, "MAX_RECV_SPEED_LARGE", CURLOPT_MAX_RECV_SPEED_LARGE);
    insint_c(d, "SSL_SESSIONID_CACHE", CURLOPT_SSL_SESSIONID_CACHE);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 41, 0)
    insint_c(d, "SSL_VERIFYSTATUS", CURLOPT_SSL_VERIFYSTATUS);
#endif
    insint_c(d, "SSH_AUTH_TYPES", CURLOPT_SSH_AUTH_TYPES);
    insint_c(d, "SSH_PUBLIC_KEYFILE", CURLOPT_SSH_PUBLIC_KEYFILE);
    insint_c(d, "SSH_PRIVATE_KEYFILE", CURLOPT_SSH_PRIVATE_KEYFILE);
#ifdef HAVE_CURL_7_19_6_OPTS
    insint_c(d, "SSH_KNOWNHOSTS", CURLOPT_SSH_KNOWNHOSTS);
    insint_c(d, "SSH_KEYFUNCTION", CURLOPT_SSH_KEYFUNCTION);
#endif
    insint_c(d, "FTP_SSL_CCC", CURLOPT_FTP_SSL_CCC);
    insint_c(d, "TIMEOUT_MS", CURLOPT_TIMEOUT_MS);
    insint_c(d, "CONNECTTIMEOUT_MS", CURLOPT_CONNECTTIMEOUT_MS);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 24, 0)
    insint_c(d, "ACCEPTTIMEOUT_MS", CURLOPT_ACCEPTTIMEOUT_MS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 36, 0)
    insint_c(d, "EXPECT_100_TIMEOUT_MS", CURLOPT_EXPECT_100_TIMEOUT_MS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 25, 0)
    insint_c(d, "TCP_KEEPALIVE", CURLOPT_TCP_KEEPALIVE);
    insint_c(d, "TCP_KEEPIDLE", CURLOPT_TCP_KEEPIDLE);
    insint_c(d, "TCP_KEEPINTVL", CURLOPT_TCP_KEEPINTVL);
#endif
    insint_c(d, "HTTP_TRANSFER_DECODING", CURLOPT_HTTP_TRANSFER_DECODING);
    insint_c(d, "HTTP_CONTENT_DECODING", CURLOPT_HTTP_CONTENT_DECODING);
    insint_c(d, "NEW_FILE_PERMS", CURLOPT_NEW_FILE_PERMS);
    insint_c(d, "NEW_DIRECTORY_PERMS", CURLOPT_NEW_DIRECTORY_PERMS);
    insint_c(d, "POST301", CURLOPT_POST301);
    insint_c(d, "PROXY_TRANSFER_MODE", CURLOPT_PROXY_TRANSFER_MODE);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 43, 0)
    insint_c(d, "SERVICE_NAME", CURLOPT_SERVICE_NAME);
    insint_c(d, "PROXY_SERVICE_NAME", CURLOPT_PROXY_SERVICE_NAME);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 52, 0)
    insint_c(d, "PROXY_CAPATH", CURLOPT_PROXY_CAPATH);
    insint_c(d, "PROXY_CAINFO", CURLOPT_PROXY_CAINFO);
    insint_c(d, "PROXY_CRLFILE", CURLOPT_PROXY_CRLFILE);
    insint_c(d, "PRE_PROXY", CURLOPT_PRE_PROXY);
    insint_c(d, "PROXY_SSLCERT", CURLOPT_PROXY_SSLCERT);
    insint_c(d, "PROXY_SSLCERTTYPE", CURLOPT_PROXY_SSLCERTTYPE);
    insint_c(d, "PROXY_SSLKEY", CURLOPT_PROXY_SSLKEY);
    insint_c(d, "PROXY_SSLKEYTYPE", CURLOPT_PROXY_SSLKEYTYPE);
    insint_c(d, "PROXY_KEYPASSWD", CURLOPT_PROXY_KEYPASSWD);
    insint_c(d, "PROXY_SSL_VERIFYPEER", CURLOPT_PROXY_SSL_VERIFYPEER);
    insint_c(d, "PROXY_SSL_VERIFYHOST", CURLOPT_PROXY_SSL_VERIFYHOST);
    insint_c(d, "PROXY_PINNEDPUBLICKEY", CURLOPT_PROXY_PINNEDPUBLICKEY);
    insint_c(d, "PROXY_SSLVERSION", CURLOPT_PROXY_SSLVERSION);
    insint_c(d, "PROXY_SSL_CIPHER_LIST", CURLOPT_PROXY_SSL_CIPHER_LIST);
    insint_c(d, "PROXY_SSL_OPTIONS", CURLOPT_PROXY_SSL_OPTIONS);
    insint_c(d, "PROXY_TLSAUTH_TYPE", CURLOPT_PROXY_TLSAUTH_TYPE);
    insint_c(d, "PROXY_TLSAUTH_USERNAME", CURLOPT_PROXY_TLSAUTH_USERNAME);
    insint_c(d, "PROXY_TLSAUTH_PASSWORD", CURLOPT_PROXY_TLSAUTH_PASSWORD);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 71, 0)
    insint_c(d, "PROXY_ISSUERCERT", CURLOPT_PROXY_ISSUERCERT);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 55, 0)
    insint_c(d, "REQUEST_TARGET", CURLOPT_REQUEST_TARGET);
#endif
    insint_c(d, "COPYPOSTFIELDS", CURLOPT_COPYPOSTFIELDS);
    insint_c(d, "SSH_HOST_PUBLIC_KEY_MD5", CURLOPT_SSH_HOST_PUBLIC_KEY_MD5);
    insint_c(d, "AUTOREFERER", CURLOPT_AUTOREFERER);
    insint_c(d, "CRLFILE", CURLOPT_CRLFILE);
    insint_c(d, "ISSUERCERT", CURLOPT_ISSUERCERT);
    insint_c(d, "ADDRESS_SCOPE", CURLOPT_ADDRESS_SCOPE);
#ifdef HAVE_CURLOPT_RESOLVE
    insint_c(d, "RESOLVE", CURLOPT_RESOLVE);
#endif
#ifdef HAVE_CURLOPT_CERTINFO
    insint_c(d, "OPT_CERTINFO", CURLOPT_CERTINFO);
#endif
#ifdef HAVE_CURLOPT_POSTREDIR
    insint_c(d, "POSTREDIR", CURLOPT_POSTREDIR);
#endif
#ifdef HAVE_CURLOPT_NOPROXY
    insint_c(d, "NOPROXY", CURLOPT_NOPROXY);
#endif
#ifdef HAVE_CURLOPT_PROTOCOLS
    insint_c(d, "PROTOCOLS", CURLOPT_PROTOCOLS);
    insint_c(d, "REDIR_PROTOCOLS", CURLOPT_REDIR_PROTOCOLS);
    insint_c(d, "PROTO_HTTP", CURLPROTO_HTTP);
    insint_c(d, "PROTO_HTTPS", CURLPROTO_HTTPS);
    insint_c(d, "PROTO_FTP", CURLPROTO_FTP);
    insint_c(d, "PROTO_FTPS", CURLPROTO_FTPS);
    insint_c(d, "PROTO_SCP", CURLPROTO_SCP);
    insint_c(d, "PROTO_SFTP", CURLPROTO_SFTP);
    insint_c(d, "PROTO_TELNET", CURLPROTO_TELNET);
    insint_c(d, "PROTO_LDAP", CURLPROTO_LDAP);
    insint_c(d, "PROTO_LDAPS", CURLPROTO_LDAPS);
    insint_c(d, "PROTO_DICT", CURLPROTO_DICT);
    insint_c(d, "PROTO_FILE", CURLPROTO_FILE);
    insint_c(d, "PROTO_TFTP", CURLPROTO_TFTP);
#ifdef HAVE_CURL_7_20_0_OPTS
    insint_c(d, "PROTO_IMAP", CURLPROTO_IMAP);
    insint_c(d, "PROTO_IMAPS", CURLPROTO_IMAPS);
    insint_c(d, "PROTO_POP3", CURLPROTO_POP3);
    insint_c(d, "PROTO_POP3S", CURLPROTO_POP3S);
    insint_c(d, "PROTO_SMTP", CURLPROTO_SMTP);
    insint_c(d, "PROTO_SMTPS", CURLPROTO_SMTPS);
#endif
#ifdef HAVE_CURL_7_21_0_OPTS
    insint_c(d, "PROTO_RTSP", CURLPROTO_RTSP);
    insint_c(d, "PROTO_RTMP", CURLPROTO_RTMP);
    insint_c(d, "PROTO_RTMPT", CURLPROTO_RTMPT);
    insint_c(d, "PROTO_RTMPE", CURLPROTO_RTMPE);
    insint_c(d, "PROTO_RTMPTE", CURLPROTO_RTMPTE);
    insint_c(d, "PROTO_RTMPS", CURLPROTO_RTMPS);
    insint_c(d, "PROTO_RTMPTS", CURLPROTO_RTMPTS);
#endif
#ifdef HAVE_CURL_7_21_2_OPTS
    insint_c(d, "PROTO_GOPHER", CURLPROTO_GOPHER);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 40, 0)
    insint_c(d, "PROTO_SMB", CURLPROTO_SMB);
    insint_c(d, "PROTO_SMBS", CURLPROTO_SMBS);
#endif
    insint_c(d, "PROTO_ALL", CURLPROTO_ALL);
#endif
#ifdef HAVE_CURL_7_19_4_OPTS
    insint_c(d, "TFTP_BLKSIZE", CURLOPT_TFTP_BLKSIZE);
    insint_c(d, "SOCKS5_GSSAPI_SERVICE", CURLOPT_SOCKS5_GSSAPI_SERVICE);
    insint_c(d, "SOCKS5_GSSAPI_NEC", CURLOPT_SOCKS5_GSSAPI_NEC);
#endif
#ifdef HAVE_CURL_7_20_0_OPTS
    insint_c(d, "MAIL_FROM", CURLOPT_MAIL_FROM);
    insint_c(d, "MAIL_RCPT", CURLOPT_MAIL_RCPT);
#endif
#ifdef HAVE_CURL_7_25_0_OPTS
    insint_c(d, "MAIL_AUTH", CURLOPT_MAIL_AUTH);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 39, 0)
    insint_c(d, "PINNEDPUBLICKEY", CURLOPT_PINNEDPUBLICKEY);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 0)
    insint_c(d, "WILDCARDMATCH", CURLOPT_WILDCARDMATCH);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 40, 0)
    insint_c(d, "UNIX_SOCKET_PATH", CURLOPT_UNIX_SOCKET_PATH);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 36, 0)
    insint_c(d, "SSL_ENABLE_ALPN", CURLOPT_SSL_ENABLE_ALPN);
    insint_c(d, "SSL_ENABLE_NPN", CURLOPT_SSL_ENABLE_NPN);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 42, 0)
    insint_c(d, "SSL_FALSESTART", CURLOPT_SSL_FALSESTART);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 25, 0)
    insint_c(d, "SSL_OPTIONS", CURLOPT_SSL_OPTIONS);
    insint_c(d, "SSLOPT_ALLOW_BEAST", CURLSSLOPT_ALLOW_BEAST);
# if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 44, 0)
    insint_c(d, "SSLOPT_NO_REVOKE", CURLSSLOPT_NO_REVOKE);
# endif
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 4)
    insint_c(d, "TLSAUTH_TYPE", CURLOPT_TLSAUTH_TYPE);
    insint_c(d, "TLSAUTH_USERNAME", CURLOPT_TLSAUTH_USERNAME);
    insint_c(d, "TLSAUTH_PASSWORD", CURLOPT_TLSAUTH_PASSWORD);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 45, 0)
    insint_c(d, "DEFAULT_PROTOCOL", CURLOPT_DEFAULT_PROTOCOL);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 61, 0)
    insint_c(d, "TLS13_CIPHERS", CURLOPT_TLS13_CIPHERS);
    insint_c(d, "PROXY_TLS13_CIPHERS", CURLOPT_PROXY_TLS13_CIPHERS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 62, 0)
    insint_c(d, "DOH_URL", CURLOPT_DOH_URL);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 64, 0)
    insint_c(d, "HTTP09_ALLOWED", CURLOPT_HTTP09_ALLOWED);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 75, 0)
    insint_c(d, "AWS_SIGV4", CURLOPT_AWS_SIGV4);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 80, 0)
    insint_c(d, "MAXLIFETIME_CONN", CURLOPT_MAXLIFETIME_CONN);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 80, 0)
    insint_c(d, "PREREQFUNCTION", CURLOPT_PREREQFUNCTION);
    insint_c(d, "PREREQFUNC_OK", CURL_PREREQFUNC_OK);
    insint_c(d, "PREREQFUNC_ABORT", CURL_PREREQFUNC_ABORT);
#endif

    insint_m(d, "M_TIMERFUNCTION", CURLMOPT_TIMERFUNCTION);
    insint_m(d, "M_SOCKETFUNCTION", CURLMOPT_SOCKETFUNCTION);
    insint_m(d, "M_PIPELINING", CURLMOPT_PIPELINING);
    insint_m(d, "M_MAXCONNECTS", CURLMOPT_MAXCONNECTS);
#ifdef HAVE_CURL_7_30_0_PIPELINE_OPTS
    insint_m(d, "M_MAX_HOST_CONNECTIONS", CURLMOPT_MAX_HOST_CONNECTIONS);
    insint_m(d, "M_MAX_TOTAL_CONNECTIONS", CURLMOPT_MAX_TOTAL_CONNECTIONS);
    insint_m(d, "M_MAX_PIPELINE_LENGTH", CURLMOPT_MAX_PIPELINE_LENGTH);
    insint_m(d, "M_CONTENT_LENGTH_PENALTY_SIZE", CURLMOPT_CONTENT_LENGTH_PENALTY_SIZE);
    insint_m(d, "M_CHUNK_LENGTH_PENALTY_SIZE", CURLMOPT_CHUNK_LENGTH_PENALTY_SIZE);
    insint_m(d, "M_PIPELINING_SITE_BL", CURLMOPT_PIPELINING_SITE_BL);
    insint_m(d, "M_PIPELINING_SERVER_BL", CURLMOPT_PIPELINING_SERVER_BL);
#endif
#ifdef HAVE_CURL_7_67_0_MULTI_STREAMS
    insint_m(d, "M_MAX_CONCURRENT_STREAMS", CURLMOPT_MAX_CONCURRENT_STREAMS);
#endif

#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 43, 0)
    insint_m(d, "PIPE_NOTHING", CURLPIPE_NOTHING);
    insint_m(d, "PIPE_HTTP1", CURLPIPE_HTTP1);
    insint_m(d, "PIPE_MULTIPLEX", CURLPIPE_MULTIPLEX);
#endif

    /* constants for setopt(IPRESOLVE, x) */
    insint_c(d, "IPRESOLVE_WHATEVER", CURL_IPRESOLVE_WHATEVER);
    insint_c(d, "IPRESOLVE_V4", CURL_IPRESOLVE_V4);
    insint_c(d, "IPRESOLVE_V6", CURL_IPRESOLVE_V6);

    /* constants for setopt(HTTP_VERSION, x) */
    insint_c(d, "CURL_HTTP_VERSION_NONE", CURL_HTTP_VERSION_NONE);
    insint_c(d, "CURL_HTTP_VERSION_1_0", CURL_HTTP_VERSION_1_0);
    insint_c(d, "CURL_HTTP_VERSION_1_1", CURL_HTTP_VERSION_1_1);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 33, 0)
    insint_c(d, "CURL_HTTP_VERSION_2_0", CURL_HTTP_VERSION_2_0);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 43, 0)
    insint_c(d, "CURL_HTTP_VERSION_2", CURL_HTTP_VERSION_2);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 47, 0)
    insint_c(d, "CURL_HTTP_VERSION_2TLS", CURL_HTTP_VERSION_2TLS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 49, 0)
    insint_c(d, "CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE", CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE);
    insint_c(d, "TCP_FASTOPEN", CURLOPT_TCP_FASTOPEN);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 66, 0)
    insint_c(d, "CURL_HTTP_VERSION_3", CURL_HTTP_VERSION_3);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 88, 0)
    insint_c(d, "CURL_HTTP_VERSION_3ONLY", CURL_HTTP_VERSION_3ONLY);
#endif
    insint_c(d, "CURL_HTTP_VERSION_LAST", CURL_HTTP_VERSION_LAST);

    /* CURL_NETRC_OPTION: constants for setopt(NETRC, x) */
    insint_c(d, "NETRC_OPTIONAL", CURL_NETRC_OPTIONAL);
    insint_c(d, "NETRC_IGNORED", CURL_NETRC_IGNORED);
    insint_c(d, "NETRC_REQUIRED", CURL_NETRC_REQUIRED);

    /* constants for setopt(SSLVERSION, x) */
    insint_c(d, "SSLVERSION_DEFAULT", CURL_SSLVERSION_DEFAULT);
    insint_c(d, "SSLVERSION_SSLv2", CURL_SSLVERSION_SSLv2);
    insint_c(d, "SSLVERSION_SSLv3", CURL_SSLVERSION_SSLv3);
    insint_c(d, "SSLVERSION_TLSv1", CURL_SSLVERSION_TLSv1);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 34, 0)
    insint_c(d, "SSLVERSION_TLSv1_0", CURL_SSLVERSION_TLSv1_0);
    insint_c(d, "SSLVERSION_TLSv1_1", CURL_SSLVERSION_TLSv1_1);
    insint_c(d, "SSLVERSION_TLSv1_2", CURL_SSLVERSION_TLSv1_2);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 52, 0)
    insint_c(d, "SSLVERSION_TLSv1_3", CURL_SSLVERSION_TLSv1_3);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 54, 0)
    insint_c(d, "SSLVERSION_MAX_DEFAULT", CURL_SSLVERSION_MAX_DEFAULT);
    insint_c(d, "SSLVERSION_MAX_TLSv1_0", CURL_SSLVERSION_MAX_TLSv1_0);
    insint_c(d, "SSLVERSION_MAX_TLSv1_1", CURL_SSLVERSION_MAX_TLSv1_1);
    insint_c(d, "SSLVERSION_MAX_TLSv1_2", CURL_SSLVERSION_MAX_TLSv1_2);
    insint_c(d, "SSLVERSION_MAX_TLSv1_3", CURL_SSLVERSION_MAX_TLSv1_3);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 60, 0)
    insint_c(d, "HAPROXYPROTOCOL", CURLOPT_HAPROXYPROTOCOL);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 2, 0)
    insint_c(d, "HAPROXY_CLIENT_IP", CURLOPT_HAPROXY_CLIENT_IP);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 8, 0)
    insint_c(d, "ECH", CURLOPT_ECH);
#endif

    /* curl_TimeCond: constants for setopt(TIMECONDITION, x) */
    insint_c(d, "TIMECONDITION_NONE", CURL_TIMECOND_NONE);
    insint_c(d, "TIMECONDITION_IFMODSINCE", CURL_TIMECOND_IFMODSINCE);
    insint_c(d, "TIMECONDITION_IFUNMODSINCE", CURL_TIMECOND_IFUNMODSINCE);
    insint_c(d, "TIMECONDITION_LASTMOD", CURL_TIMECOND_LASTMOD);

    /* constants for setopt(CURLOPT_SSH_AUTH_TYPES, x) */
    insint_c(d, "SSH_AUTH_ANY", CURLSSH_AUTH_ANY);
    insint_c(d, "SSH_AUTH_NONE", CURLSSH_AUTH_NONE);
    insint_c(d, "SSH_AUTH_PUBLICKEY", CURLSSH_AUTH_PUBLICKEY);
    insint_c(d, "SSH_AUTH_PASSWORD", CURLSSH_AUTH_PASSWORD);
    insint_c(d, "SSH_AUTH_HOST", CURLSSH_AUTH_HOST);
    insint_c(d, "SSH_AUTH_KEYBOARD", CURLSSH_AUTH_KEYBOARD);
    insint_c(d, "SSH_AUTH_DEFAULT", CURLSSH_AUTH_DEFAULT);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 28, 0)
    insint_c(d, "SSH_AUTH_AGENT", CURLSSH_AUTH_AGENT);
#endif

#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 37, 0)
    insint_c(d, "HEADER_UNIFIED", CURLHEADER_UNIFIED);
    insint_c(d, "HEADER_SEPARATE", CURLHEADER_SEPARATE);
#endif

#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 5)
    insint_c(d, "SOCKOPT_ALREADY_CONNECTED", CURL_SOCKOPT_ALREADY_CONNECTED);
    insint_c(d, "SOCKOPT_ERROR", CURL_SOCKOPT_ERROR);
    insint_c(d, "SOCKOPT_OK", CURL_SOCKOPT_OK);
#endif

#ifdef HAVE_CURL_7_19_6_OPTS
    /* curl_khtype constants */
    insint_c(d, "KHTYPE_UNKNOWN", CURLKHTYPE_UNKNOWN);
    insint_c(d, "KHTYPE_RSA1", CURLKHTYPE_RSA1);
    insint_c(d, "KHTYPE_RSA", CURLKHTYPE_RSA);
    insint_c(d, "KHTYPE_DSS", CURLKHTYPE_DSS);

    /* curl_khmatch constants, passed to sshkeycallback */
    insint_c(d, "KHMATCH_OK", CURLKHMATCH_OK);
    insint_c(d, "KHMATCH_MISMATCH", CURLKHMATCH_MISMATCH);
    insint_c(d, "KHMATCH_MISSING", CURLKHMATCH_MISSING);

    /* return values for CURLOPT_SSH_KEYFUNCTION */
    insint_c(d, "KHSTAT_FINE_ADD_TO_FILE", CURLKHSTAT_FINE_ADD_TO_FILE);
    insint_c(d, "KHSTAT_FINE", CURLKHSTAT_FINE);
    insint_c(d, "KHSTAT_REJECT", CURLKHSTAT_REJECT);
    insint_c(d, "KHSTAT_DEFER", CURLKHSTAT_DEFER);
#endif

#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 28, 0)
    insint_c(d, "SOCKTYPE_ACCEPT", CURLSOCKTYPE_ACCEPT);
#endif
    insint_c(d, "SOCKTYPE_IPCXN", CURLSOCKTYPE_IPCXN);

    insint_c(d, "USESSL_NONE", CURLUSESSL_NONE);
    insint_c(d, "USESSL_TRY", CURLUSESSL_TRY);
    insint_c(d, "USESSL_CONTROL", CURLUSESSL_CONTROL);
    insint_c(d, "USESSL_ALL", CURLUSESSL_ALL);

    /* CURLINFO: symbolic constants for getinfo(x) */
    insint_c(d, "EFFECTIVE_URL", CURLINFO_EFFECTIVE_URL);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 72, 0)
    insint_c(d, "EFFECTIVE_METHOD", CURLINFO_EFFECTIVE_METHOD);
#endif
    /* same as CURLINFO_RESPONSE_CODE */
    insint_c(d, "HTTP_CODE", CURLINFO_HTTP_CODE);
    insint_c(d, "RESPONSE_CODE", CURLINFO_RESPONSE_CODE);
    insint_c(d, "TOTAL_TIME", CURLINFO_TOTAL_TIME);
    insint_c(d, "NAMELOOKUP_TIME", CURLINFO_NAMELOOKUP_TIME);
    insint_c(d, "CONNECT_TIME", CURLINFO_CONNECT_TIME);
    insint_c(d, "APPCONNECT_TIME", CURLINFO_APPCONNECT_TIME);
    insint_c(d, "PRETRANSFER_TIME", CURLINFO_PRETRANSFER_TIME);
    insint_c(d, "SIZE_UPLOAD", CURLINFO_SIZE_UPLOAD);
    insint_c(d, "SIZE_DOWNLOAD", CURLINFO_SIZE_DOWNLOAD);
    insint_c(d, "SPEED_DOWNLOAD", CURLINFO_SPEED_DOWNLOAD);
    insint_c(d, "SPEED_UPLOAD", CURLINFO_SPEED_UPLOAD);
    insint_c(d, "HEADER_SIZE", CURLINFO_HEADER_SIZE);
    insint_c(d, "REQUEST_SIZE", CURLINFO_REQUEST_SIZE);
    insint_c(d, "SSL_VERIFYRESULT", CURLINFO_SSL_VERIFYRESULT);
    insint_c(d, "INFO_FILETIME", CURLINFO_FILETIME);
    insint_c(d, "CONTENT_LENGTH_DOWNLOAD", CURLINFO_CONTENT_LENGTH_DOWNLOAD);
    insint_c(d, "CONTENT_LENGTH_UPLOAD", CURLINFO_CONTENT_LENGTH_UPLOAD);
    insint_c(d, "STARTTRANSFER_TIME", CURLINFO_STARTTRANSFER_TIME);
    insint_c(d, "CONTENT_TYPE", CURLINFO_CONTENT_TYPE);
    insint_c(d, "REDIRECT_TIME", CURLINFO_REDIRECT_TIME);
    insint_c(d, "REDIRECT_COUNT", CURLINFO_REDIRECT_COUNT);
    insint_c(d, "REDIRECT_URL", CURLINFO_REDIRECT_URL);
    insint_c(d, "PRIMARY_IP", CURLINFO_PRIMARY_IP);
#ifdef HAVE_CURLINFO_PRIMARY_PORT
    insint_c(d, "PRIMARY_PORT", CURLINFO_PRIMARY_PORT);
#endif
#ifdef HAVE_CURLINFO_LOCAL_IP
    insint_c(d, "LOCAL_IP", CURLINFO_LOCAL_IP);
#endif
#ifdef HAVE_CURLINFO_LOCAL_PORT
    insint_c(d, "LOCAL_PORT", CURLINFO_LOCAL_PORT);
#endif
    insint_c(d, "HTTP_CONNECTCODE", CURLINFO_HTTP_CONNECTCODE);
    insint_c(d, "HTTPAUTH_AVAIL", CURLINFO_HTTPAUTH_AVAIL);
    insint_c(d, "PROXYAUTH_AVAIL", CURLINFO_PROXYAUTH_AVAIL);
    insint_c(d, "OS_ERRNO", CURLINFO_OS_ERRNO);
    insint_c(d, "NUM_CONNECTS", CURLINFO_NUM_CONNECTS);
    insint_c(d, "SSL_ENGINES", CURLINFO_SSL_ENGINES);
    insint_c(d, "INFO_COOKIELIST", CURLINFO_COOKIELIST);
    insint_c(d, "LASTSOCKET", CURLINFO_LASTSOCKET);
    insint_c(d, "FTP_ENTRY_PATH", CURLINFO_FTP_ENTRY_PATH);
#ifdef HAVE_CURLOPT_CERTINFO
    insint_c(d, "INFO_CERTINFO", CURLINFO_CERTINFO);
#endif
#ifdef HAVE_CURL_7_19_4_OPTS
    insint_c(d, "CONDITION_UNMET", CURLINFO_CONDITION_UNMET);
#endif

/* CURLINFO_*_T constants */
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 55, 0)
    insint_c(d, "CONTENT_LENGTH_DOWNLOAD_T", CURLINFO_CONTENT_LENGTH_DOWNLOAD_T);
    insint_c(d, "CONTENT_LENGTH_UPLOAD_T", CURLINFO_CONTENT_LENGTH_UPLOAD_T);
    insint_c(d, "SIZE_DOWNLOAD_T", CURLINFO_SIZE_DOWNLOAD_T);
    insint_c(d, "SIZE_UPLOAD_T", CURLINFO_SIZE_UPLOAD_T);
    insint_c(d, "SPEED_DOWNLOAD_T", CURLINFO_SPEED_DOWNLOAD_T);
    insint_c(d, "SPEED_UPLOAD_T", CURLINFO_SPEED_UPLOAD_T);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 59, 0)
    insint_c(d, "FILETIME_T", CURLINFO_FILETIME_T);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 61, 0)
    insint_c(d, "APPCONNECT_TIME_T", CURLINFO_APPCONNECT_TIME_T);
    insint_c(d, "CONNECT_TIME_T", CURLINFO_CONNECT_TIME_T);
    insint_c(d, "NAMELOOKUP_TIME_T", CURLINFO_NAMELOOKUP_TIME_T);
    insint_c(d, "PRETRANSFER_TIME_T", CURLINFO_PRETRANSFER_TIME_T);
    insint_c(d, "REDIRECT_TIME_T", CURLINFO_REDIRECT_TIME_T);
    insint_c(d, "STARTTRANSFER_TIME_T", CURLINFO_STARTTRANSFER_TIME_T);
    insint_c(d, "TOTAL_TIME_T", CURLINFO_TOTAL_TIME_T);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 6, 0)
    insint_c(d, "QUEUE_TIME_T", CURLINFO_QUEUE_TIME_T);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 10, 0)
    insint_c(d, "POSTTRANSFER_TIME_T", CURLINFO_POSTTRANSFER_TIME_T);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(8, 11, 0)
    insint_c(d, "EARLYDATA_SENT_T", CURLINFO_EARLYDATA_SENT_T);
#endif

#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 20, 0)
    insint_c(d, "INFO_RTSP_CLIENT_CSEQ", CURLINFO_RTSP_CLIENT_CSEQ);
    insint_c(d, "INFO_RTSP_CSEQ_RECV", CURLINFO_RTSP_CSEQ_RECV);
    insint_c(d, "INFO_RTSP_SERVER_CSEQ", CURLINFO_RTSP_SERVER_CSEQ);
    insint_c(d, "INFO_RTSP_SESSION_ID", CURLINFO_RTSP_SESSION_ID);
    insint_c(d, "RTSPREQ_NONE",CURL_RTSPREQ_NONE);
    insint_c(d, "RTSPREQ_OPTIONS",CURL_RTSPREQ_OPTIONS);
    insint_c(d, "RTSPREQ_DESCRIBE",CURL_RTSPREQ_DESCRIBE);
    insint_c(d, "RTSPREQ_ANNOUNCE",CURL_RTSPREQ_ANNOUNCE);
    insint_c(d, "RTSPREQ_SETUP",CURL_RTSPREQ_SETUP);
    insint_c(d, "RTSPREQ_PLAY",CURL_RTSPREQ_PLAY);
    insint_c(d, "RTSPREQ_PAUSE",CURL_RTSPREQ_PAUSE);
    insint_c(d, "RTSPREQ_TEARDOWN",CURL_RTSPREQ_TEARDOWN);
    insint_c(d, "RTSPREQ_GET_PARAMETER",CURL_RTSPREQ_GET_PARAMETER);
    insint_c(d, "RTSPREQ_SET_PARAMETER",CURL_RTSPREQ_SET_PARAMETER);
    insint_c(d, "RTSPREQ_RECORD",CURL_RTSPREQ_RECORD);
    insint_c(d, "RTSPREQ_RECEIVE",CURL_RTSPREQ_RECEIVE);
    insint_c(d, "RTSPREQ_LAST",CURL_RTSPREQ_LAST);
#endif

    /* CURLPAUSE: symbolic constants for pause(bitmask) */
    insint_c(d, "PAUSE_RECV", CURLPAUSE_RECV);
    insint_c(d, "PAUSE_SEND", CURLPAUSE_SEND);
    insint_c(d, "PAUSE_ALL",  CURLPAUSE_ALL);
    insint_c(d, "PAUSE_CONT", CURLPAUSE_CONT);

#ifdef HAVE_CURL_7_19_5_OPTS
    /* CURL_SEEKFUNC: return values for seek function */
    insint_c(d, "SEEKFUNC_OK", CURL_SEEKFUNC_OK);
    insint_c(d, "SEEKFUNC_FAIL", CURL_SEEKFUNC_FAIL);
    insint_c(d, "SEEKFUNC_CANTSEEK", CURL_SEEKFUNC_CANTSEEK);
#endif

#ifdef HAVE_CURLOPT_DNS_SERVERS
    insint_c(d, "DNS_SERVERS", CURLOPT_DNS_SERVERS);
#endif

#ifdef HAVE_CURLOPT_POSTREDIR
    insint_c(d, "REDIR_POST_301", CURL_REDIR_POST_301);
    insint_c(d, "REDIR_POST_302", CURL_REDIR_POST_302);
# ifdef HAVE_CURL_REDIR_POST_303
    insint_c(d, "REDIR_POST_303", CURL_REDIR_POST_303);
# endif
    insint_c(d, "REDIR_POST_ALL", CURL_REDIR_POST_ALL);
#endif

#ifdef HAVE_CURLOPT_CONNECT_TO
    insint_c(d, "CONNECT_TO", CURLOPT_CONNECT_TO);
#endif

#ifdef HAVE_CURLINFO_HTTP_VERSION
    insint_c(d, "INFO_HTTP_VERSION", CURLINFO_HTTP_VERSION);
#endif

    /* options for global_init() */
    insint(d, "GLOBAL_SSL", CURL_GLOBAL_SSL);
    insint(d, "GLOBAL_WIN32", CURL_GLOBAL_WIN32);
    insint(d, "GLOBAL_ALL", CURL_GLOBAL_ALL);
    insint(d, "GLOBAL_NOTHING", CURL_GLOBAL_NOTHING);
    insint(d, "GLOBAL_DEFAULT", CURL_GLOBAL_DEFAULT);
#ifdef CURL_GLOBAL_ACK_EINTR
    /* CURL_GLOBAL_ACK_EINTR was introduced in libcurl-7.30.0 */
    insint(d, "GLOBAL_ACK_EINTR", CURL_GLOBAL_ACK_EINTR);
#endif


    /* constants for curl_multi_socket interface */
    insint(d, "CSELECT_IN", CURL_CSELECT_IN);
    insint(d, "CSELECT_OUT", CURL_CSELECT_OUT);
    insint(d, "CSELECT_ERR", CURL_CSELECT_ERR);
    insint(d, "SOCKET_TIMEOUT", CURL_SOCKET_TIMEOUT);
    insint(d, "POLL_NONE", CURL_POLL_NONE);
    insint(d, "POLL_IN", CURL_POLL_IN);
    insint(d, "POLL_OUT", CURL_POLL_OUT);
    insint(d, "POLL_INOUT", CURL_POLL_INOUT);
    insint(d, "POLL_REMOVE", CURL_POLL_REMOVE);

    /* curl_lock_data: XXX do we need this in pycurl ??? */
    /* curl_lock_access: XXX do we need this in pycurl ??? */
    /* CURLSHcode: XXX do we need this in pycurl ??? */
    /* CURLSHoption: XXX do we need this in pycurl ??? */

    /* CURLversion: constants for curl_version_info(x) */
#if 0
    /* XXX - do we need these ?? */
    insint(d, "VERSION_FIRST", CURLVERSION_FIRST);
    insint(d, "VERSION_SECOND", CURLVERSION_SECOND);
    insint(d, "VERSION_THIRD", CURLVERSION_THIRD);
    insint(d, "VERSION_NOW", CURLVERSION_NOW);
#endif

    /* version features - bitmasks for curl_version_info_data.features */
    insint(d, "VERSION_IPV6", CURL_VERSION_IPV6);
    insint(d, "VERSION_KERBEROS4", CURL_VERSION_KERBEROS4);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 40, 0)
    insint(d, "VERSION_KERBEROS5", CURL_VERSION_KERBEROS5);
#endif
    insint(d, "VERSION_SSL", CURL_VERSION_SSL);
    insint(d, "VERSION_LIBZ", CURL_VERSION_LIBZ);
    insint(d, "VERSION_NTLM", CURL_VERSION_NTLM);
    insint(d, "VERSION_GSSNEGOTIATE", CURL_VERSION_GSSNEGOTIATE);
    insint(d, "VERSION_DEBUG", CURL_VERSION_DEBUG);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 19, 6)
    insint(d, "VERSION_CURLDEBUG", CURL_VERSION_CURLDEBUG);
#endif
    insint(d, "VERSION_ASYNCHDNS", CURL_VERSION_ASYNCHDNS);
    insint(d, "VERSION_SPNEGO", CURL_VERSION_SPNEGO);
    insint(d, "VERSION_LARGEFILE", CURL_VERSION_LARGEFILE);
    insint(d, "VERSION_IDN", CURL_VERSION_IDN);
    insint(d, "VERSION_SSPI", CURL_VERSION_SSPI);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 38, 0)
    insint(d, "VERSION_GSSAPI", CURL_VERSION_GSSAPI);
#endif
    insint(d, "VERSION_CONV", CURL_VERSION_CONV);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 21, 4)
    insint(d, "VERSION_TLSAUTH_SRP", CURL_VERSION_TLSAUTH_SRP);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 22, 0)
    insint(d, "VERSION_NTLM_WB", CURL_VERSION_NTLM_WB);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 33, 0)
    insint(d, "VERSION_HTTP2", CURL_VERSION_HTTP2);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 40, 0)
    insint(d, "VERSION_UNIX_SOCKETS", CURL_VERSION_UNIX_SOCKETS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 47, 0)
    insint(d, "VERSION_PSL", CURL_VERSION_PSL);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 52, 0)
    insint(d, "CURL_VERSION_HTTPS_PROXY", CURL_VERSION_HTTPS_PROXY);
    insint(d, "VERSION_HTTPS_PROXY", CURL_VERSION_HTTPS_PROXY);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 56, 0)
    insint(d, "CURL_VERSION_MULTI_SSL", CURL_VERSION_MULTI_SSL);
    insint(d, "VERSION_MULTI_SSL", CURL_VERSION_MULTI_SSL);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 57, 0)
    insint(d, "CURL_VERSION_BROTLI", CURL_VERSION_BROTLI);
    insint(d, "VERSION_BROTLI", CURL_VERSION_BROTLI);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 64, 1)
    insint(d, "CURL_VERSION_ALTSVC", CURL_VERSION_ALTSVC);
    insint(d, "VERSION_ALTSVC", CURL_VERSION_ALTSVC);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 66, 0)
    insint(d, "CURL_VERSION_HTTP3", CURL_VERSION_HTTP3);
    insint(d, "VERSION_HTTP3", CURL_VERSION_HTTP3);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 72, 0)
    insint(d, "CURL_VERSION_UNICODE", CURL_VERSION_UNICODE);
    insint(d, "CURL_VERSION_ZSTD", CURL_VERSION_ZSTD);
    insint(d, "VERSION_UNICODE", CURL_VERSION_UNICODE);
    insint(d, "VERSION_ZSTD", CURL_VERSION_ZSTD);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 74, 0)
    insint(d, "CURL_VERSION_HSTS", CURL_VERSION_HSTS);
    insint(d, "VERSION_HSTS", CURL_VERSION_HSTS);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 76, 0)
    insint(d, "CURL_VERSION_GSASL", CURL_VERSION_GSASL);
    insint(d, "VERSION_GSASL", CURL_VERSION_GSASL);
#endif

    /**
     ** the order of these constants mostly follows <curl/multi.h>
     **/

    /* CURLMcode: multi error codes */
    /* old symbol */
    insint_m(d, "E_CALL_MULTI_PERFORM", CURLM_CALL_MULTI_PERFORM);
    /* new symbol for consistency */
    insint_m(d, "E_MULTI_CALL_MULTI_PERFORM", CURLM_CALL_MULTI_PERFORM);
    insint_m(d, "E_MULTI_OK", CURLM_OK);
    insint_m(d, "E_MULTI_BAD_HANDLE", CURLM_BAD_HANDLE);
    insint_m(d, "E_MULTI_BAD_EASY_HANDLE", CURLM_BAD_EASY_HANDLE);
    insint_m(d, "E_MULTI_BAD_SOCKET", CURLM_BAD_SOCKET);
    insint_m(d, "E_MULTI_CALL_MULTI_SOCKET", CURLM_CALL_MULTI_SOCKET);
    insint_m(d, "E_MULTI_OUT_OF_MEMORY", CURLM_OUT_OF_MEMORY);
    insint_m(d, "E_MULTI_INTERNAL_ERROR", CURLM_INTERNAL_ERROR);
    insint_m(d, "E_MULTI_UNKNOWN_OPTION", CURLM_UNKNOWN_OPTION);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 32, 1)
    insint_m(d, "E_MULTI_ADDED_ALREADY", CURLM_ADDED_ALREADY);
#endif
    /* curl shared constants */
    insint_s(d, "SH_SHARE", CURLSHOPT_SHARE);
    insint_s(d, "SH_UNSHARE", CURLSHOPT_UNSHARE);

    insint_s(d, "LOCK_DATA_COOKIE", CURL_LOCK_DATA_COOKIE);
    insint_s(d, "LOCK_DATA_DNS", CURL_LOCK_DATA_DNS);
    insint_s(d, "LOCK_DATA_SSL_SESSION", CURL_LOCK_DATA_SSL_SESSION);
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 57, 0)
    insint_s(d, "LOCK_DATA_CONNECT", CURL_LOCK_DATA_CONNECT);
#endif
#if LIBCURL_VERSION_NUM >= MAKE_LIBCURL_VERSION(7, 61, 0)
    insint_s(d, "LOCK_DATA_PSL", CURL_LOCK_DATA_PSL);
#endif

    /* Initialize callback locks if ssl is enabled */
#if defined(PYCURL_NEED_SSL_TSL)
    if (pycurl_ssl_init() != 0) {
        goto error;
    }
#endif

#if PY_MAJOR_VERSION >= 3
    xio_module = PyImport_ImportModule("io");
    if (xio_module == NULL) {
        goto error;
    }
    bytesio = PyObject_GetAttrString(xio_module, "BytesIO");
    if (bytesio == NULL) {
        goto error;
    }
    stringio = PyObject_GetAttrString(xio_module, "StringIO");
    if (stringio == NULL) {
        goto error;
    }
#else
    xio_module = PyImport_ImportModule("cStringIO");
    if (xio_module == NULL) {
        PyErr_Clear();
        xio_module = PyImport_ImportModule("StringIO");
        if (xio_module == NULL) {
            goto error;
        }
    }
    stringio = PyObject_GetAttrString(xio_module, "StringIO");
    if (stringio == NULL) {
        goto error;
    }
    bytesio = stringio;
    Py_INCREF(bytesio);
#endif

    collections_module = PyImport_ImportModule("collections");
    if (collections_module == NULL) {
        goto error;
    }
    named_tuple = PyObject_GetAttrString(collections_module, "namedtuple");
    if (named_tuple == NULL) {
        goto error;
    }
#ifdef HAVE_CURL_7_19_6_OPTS
    arglist = Py_BuildValue("ss", "KhKey", "key keytype");
    if (arglist == NULL) {
        goto error;
    }
    khkey_type = PyObject_Call(named_tuple, arglist, NULL);
    if (khkey_type == NULL) {
        goto error;
    }
    Py_DECREF(arglist);
    PyDict_SetItemString(d, "KhKey", khkey_type);
#endif

    arglist = Py_BuildValue("ss", "CurlSockAddr", "family socktype protocol addr");
    if (arglist == NULL) {
        goto error;
    }
    curl_sockaddr_type = PyObject_Call(named_tuple, arglist, NULL);
    if (curl_sockaddr_type == NULL) {
        goto error;
    }
    Py_DECREF(arglist);
    PyDict_SetItemString(d, "CurlSockAddr", curl_sockaddr_type);

#if defined(WITH_THREAD) && (PY_MAJOR_VERSION < 3 || PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 9)
    /* Finally initialize global interpreter lock */
    PyEval_InitThreads();
#endif

#ifdef PYCURL_AUTODETECT_CA
    pycurl_autodetect_ca();
#endif

#if PY_MAJOR_VERSION >= 3
    return m;
#else
    PYCURL_MODINIT_RETURN_NULL;
#endif

error:
    Py_XDECREF(curlobject_constants);
    Py_XDECREF(curlmultiobject_constants);
    Py_XDECREF(curlshareobject_constants);
    Py_XDECREF(ErrorObject);
    Py_XDECREF(collections_module);
    Py_XDECREF(named_tuple);
    Py_XDECREF(xio_module);
    Py_XDECREF(bytesio);
    Py_XDECREF(stringio);
    Py_XDECREF(arglist);
#ifdef HAVE_CURL_7_19_6_OPTS
    Py_XDECREF(khkey_type);
    Py_XDECREF(curl_sockaddr_type);
#endif
    PyMem_Free(g_pycurl_useragent);
    if (!PyErr_Occurred())
        PyErr_SetString(PyExc_ImportError, "curl module init failed");
    PYCURL_MODINIT_RETURN_NULL;
}
