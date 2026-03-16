/* Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved. */
/* $Id$ */

%{
/* for time_t_bits */
#include <time.h>
/* For snprintf, strncpy */
#include <string.h>
#include <stdio.h>

#include <openssl/bn.h>
#include <openssl/dh.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#include <ceval.h>
%}

/* OpenSSL 1.1 compatibility shim */
%include _lib11_compat.i

/* Python 3 compatibility shim */
%include _py3k_compat.i

%inline %{
/* test for time_t size */
int time_t_bits() {
    return sizeof(time_t) * 8;
}
%}

%{
/* OpenSSL 1.0.2 copmatbility shim */
#if OPENSSL_VERSION_NUMBER < 0x10002000L
typedef void (*OPENSSL_sk_freefunc)(void *);
typedef void *(*OPENSSL_sk_copyfunc)(const void *);
typedef struct stack_st OPENSSL_STACK;

# define MIN_NODES       4
# define sk_deep_copy OPENSSL_sk_deep_copy

void OPENSSL_sk_free(OPENSSL_STACK *st)
{
    if (st == NULL)
        return;
    OPENSSL_free(st->data);
    OPENSSL_free(st);
}

OPENSSL_STACK *OPENSSL_sk_deep_copy(const OPENSSL_STACK *sk,
                             OPENSSL_sk_copyfunc copy_func,
                             OPENSSL_sk_freefunc free_func)
{
    OPENSSL_STACK *ret;
    int i;

    if (sk->num < 0)
        return NULL;

    if ((ret = OPENSSL_malloc(sizeof(*ret))) == NULL)
        return NULL;

    /* direct structure assignment */
    *ret = *sk;

    ret->num_alloc = sk->num > MIN_NODES ? (size_t)sk->num : MIN_NODES;
    ret->data = OPENSSL_zalloc(sizeof(*ret->data) * ret->num_alloc);
    if (ret->data == NULL) {
        OPENSSL_free(ret);
        return NULL;
    }

    for (i = 0; i < ret->num; ++i) {
        if (sk->data[i] == NULL)
            continue;
        if ((ret->data[i] = copy_func(sk->data[i])) == NULL) {
            while (--i >= 0)
                if (ret->data[i] != NULL)
                    free_func((void *)ret->data[i]);
            OPENSSL_sk_free(ret);
            return NULL;
        }
    }
    return ret;
}
#endif /* OpenSSL 1.0.2 copmatbility shim */


/* Blob interface. Deprecated. */

Blob *blob_new(int len, const char *errmsg) {

    Blob *blob;
    if (!(blob=(Blob *)PyMem_Malloc(sizeof(Blob)))){
        PyErr_SetString(PyExc_MemoryError, errmsg);
        return NULL;
    }
    if (!(blob->data=(unsigned char *)PyMem_Malloc(len))) {
        PyMem_Free(blob);
        PyErr_SetString(PyExc_MemoryError, errmsg);
        return NULL;
    }
    blob->len=len;
    return blob;
}

Blob *blob_copy(Blob *from, const char *errmsg) {
    Blob *blob=blob_new(from->len, errmsg);
    if (!blob) {
        PyErr_SetString(PyExc_MemoryError, errmsg);
        return NULL;
    }
    memcpy(blob->data, from->data, from->len);
    return blob;
}

void blob_free(Blob *blob) {
    PyMem_Free(blob->data);
    PyMem_Free(blob);
}


/* Python helpers. */

%}
%ignore PyObject_CheckBuffer;
%ignore PyObject_GetBuffer;
%ignore PyBuffer_Release;
%ignore m2_PyErr_SetString_from_openssl_error;
%ignore m2_PyObject_AsReadBuffer;
%ignore m2_PyObject_AsReadBufferInt;
%ignore m2_PyObject_GetBufferInt;
%ignore m2_PyString_AsStringAndSizeInt;

%newobject mpi_to_bn;
%newobject bin_to_bn;

%nodefaultctor bignum_st;
struct bignum_st {};
typedef struct bignum_st BIGNUM;

%extend bignum_st {
   ~bignum_st() {
      BN_free($self);
   }
};

%{
/*
 * Convert an OpenSSL error code into a Python Exception string.
 */
void m2_PyErr_SetString_from_openssl_error(PyObject *err_type, unsigned long err_code) {
    char err_buf[256];
    const char *reason_str = NULL;

    reason_str = ERR_reason_error_string(err_code);

    if (reason_str != NULL) {
        // OpenSSL provided a reason string. Use it directly.
        strncpy(err_buf, reason_str, sizeof(err_buf));
        err_buf[sizeof(err_buf) - 1] = '\0';
    } else {
        // OpenSSL did not provide a specific reason string for this code.
        // Create a fallback message including the raw error code for diagnostics.
        snprintf(err_buf, sizeof(err_buf),
                 "Unknown OpenSSL error code: %lu", err_code);
    }

    PyErr_SetString(err_type, err_buf);
}

static int
m2_PyObject_AsReadBuffer(PyObject * obj, const void **buffer,
			 Py_ssize_t * buffer_len)
{
    Py_ssize_t len = 0;
    Py_buffer view;

    if (PyObject_CheckBuffer(obj)) {
	if (PyObject_GetBuffer(obj, &view, PyBUF_SIMPLE) == 0) {
	    *buffer = view.buf;
	    len = view.len;
	}
    } else {
    PyErr_SetString(PyExc_TypeError, "expected a readable buffer object");
	return -1;
    }
    if (len > INT_MAX) {
	PyBuffer_Release(&view);
	PyErr_SetString(PyExc_ValueError, "object too large");
	return -1;
    }
    *buffer_len = len;
    PyBuffer_Release(&view);
    return 0;
}

static int
m2_PyObject_AsReadBufferInt(PyObject * obj, const void **buffer,
			    int *buffer_len)
{
    int ret = 0;
    Py_ssize_t len = 0;

    ret = m2_PyObject_AsReadBuffer(obj, buffer, &len);
    *buffer_len = len;
    return ret;
}

static int m2_PyObject_GetBufferInt(PyObject *obj, Py_buffer *view, int flags)
{
    int ret;

    if (PyObject_CheckBuffer(obj))
        ret = PyObject_GetBuffer(obj, view, flags);
    else {
        PyErr_SetString(PyExc_TypeError, "expected a readable buffer object");
        return -1;
    }
    if (ret)
        return ret;
    if (view->len > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "object too large");
        PyBuffer_Release(view);
        return -1;
    }

    return 0;
}

static BIGNUM*
m2_PyObject_AsBIGNUM(PyObject* value, PyObject* _py_exc)
{
    BIGNUM* bn = NULL;
    const void* vbuf = NULL;
    int vlen = 0;

    if (!_py_exc || !PyExceptionClass_Check(_py_exc)) {
         PyErr_SetString(PyExc_TypeError, "Invalid exception type provided internally.");
         return NULL;
    }

    if (m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1) {
        return NULL;
    }

    ERR_clear_error();
    if (!(bn = BN_mpi2bn((unsigned char *)vbuf, vlen, NULL))) {
        unsigned long err_code = ERR_get_error();

        m2_PyErr_SetString_from_openssl_error(_py_exc, err_code);

        return NULL;
    }

    return bn;
}

static int
m2_PyString_AsStringAndSizeInt(PyObject *obj, char **s, int *len)
{
    int ret;
    Py_ssize_t len2;

    ret = PyBytes_AsStringAndSize(obj, s, &len2);

    if (ret)
       return ret;
    if (len2 > INT_MAX) {
       PyErr_SetString(PyExc_ValueError, "string too large");
       return -1;
    }
    *len = len2;
    return 0;
}

/* Works as PyFile_Name, but always returns a new object. */
PyObject *m2_PyFile_Name(PyObject *pyfile) {
    PyObject *out = NULL;
#if PY_MAJOR_VERSION >= 3
   out = PyObject_GetAttrString(pyfile, "name");
#else
   out = PyFile_Name(pyfile);
   Py_XINCREF(out);
#endif
    return out;
}

/* Yes, __FUNCTION__ is a non-standard symbol, but it is supported by
 * both gcc and MSVC. */
#define m2_PyErr_Msg(type) m2_PyErr_Msg_Caller(type, (const char*) __FUNCTION__)

static void m2_PyErr_Msg_Caller(PyObject *err_type, const char* caller) {
    const char *err_reason;
    const char *data;
    int flags;
    /* This max size of a (longer than ours) OpenSSL error string is hardcoded
     * in OpenSSL's crypto/err/err_prn.c:ERR_print_errors_cb() */
    char err_msg[4096];
    unsigned long err_code = ERR_get_error_line_data(NULL, NULL, &data, &flags);

    if (err_code != 0) {
        err_reason = ERR_reason_error_string(err_code);
        if (data && (flags & ERR_TXT_STRING))
            snprintf(err_msg, sizeof(err_msg), "%s (%s)", err_reason, data);
        else
            snprintf(err_msg, sizeof(err_msg), "%s", err_reason);

        PyErr_SetString(err_type, err_msg);
    } else {
        PyErr_Format(err_type, "Unknown error in function %s.", caller);
    }
}

/* C callbacks invoked by OpenSSL; these in turn call back into
Python. */

int ssl_verify_callback(int ok, X509_STORE_CTX *ctx) {
    PyObject *argv, *ret;
    PyObject *_x509_store_ctx_swigptr=0, *_x509_store_ctx_obj=0, *_x509_store_ctx_inst=0, *_klass=0;
    PyObject *_x509=0, *_ssl_ctx=0;
    SSL *ssl;
    SSL_CTX *ssl_ctx;
    X509 *x509;
    int errnum, errdepth;
    int cret;
    int new_style_callback = 0, warning_raised_exception=0;
    PyGILState_STATE gilstate;
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    ssl = (SSL *)X509_STORE_CTX_get_app_data(ctx);

    gilstate = PyGILState_Ensure();

    if (PyMethod_Check(ssl_verify_cb_func)) {
        PyObject *func;
        PyCodeObject *code;
        func = PyMethod_Function(ssl_verify_cb_func);
        code = (PyCodeObject *) PyFunction_GetCode(func);
        if (code && code->co_argcount == 3) { /* XXX Python internals */
            new_style_callback = 1;
        }
    } else if (PyFunction_Check(ssl_verify_cb_func)) {
        PyCodeObject *code = (PyCodeObject *) PyFunction_GetCode(ssl_verify_cb_func);
        if (code && code->co_argcount == 2) { /* XXX Python internals */
            new_style_callback = 1;
        }
    } else {
        /* XXX There are lots of other callable types, but we will assume
         * XXX that any other type of callable uses the new style callback,
         * XXX although this is not entirely safe assumption.
         */
        new_style_callback = 1;
    }

    if (new_style_callback) {
        PyObject *x509mod;

        x509mod = PyDict_GetItemString(PyImport_GetModuleDict(), "M2Crypto.X509");
        _klass = PyObject_GetAttrString(x509mod, "X509_Store_Context");

        _x509_store_ctx_swigptr = SWIG_NewPointerObj((void *)ctx, SWIGTYPE_p_X509_STORE_CTX, 0);
        _x509_store_ctx_obj = Py_BuildValue("(Oi)", _x509_store_ctx_swigptr, 0);

        _x509_store_ctx_inst = PyObject_CallObject(_klass, _x509_store_ctx_obj);

        argv = Py_BuildValue("(iO)", ok, _x509_store_ctx_inst);
    } else {
        if (PyErr_Warn(PyExc_DeprecationWarning, "Old style callback, use cb_func(ok, store) instead")) {
            warning_raised_exception = 1;
        }

        x509 = X509_STORE_CTX_get_current_cert(ctx);
        errnum = X509_STORE_CTX_get_error(ctx);
        errdepth = X509_STORE_CTX_get_error_depth(ctx);

        ssl = (SSL *)X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx());
        ssl_ctx = SSL_get_SSL_CTX(ssl);

        _x509 = SWIG_NewPointerObj((void *)x509, SWIGTYPE_p_X509, 0);
        _ssl_ctx = SWIG_NewPointerObj((void *)ssl_ctx, SWIGTYPE_p_SSL_CTX, 0);
        argv = Py_BuildValue("(OOiii)", _ssl_ctx, _x509, errnum, errdepth, ok);
    }

    if (!warning_raised_exception) {
        ret = PyObject_CallObject(ssl_verify_cb_func, argv);
    } else {
        ret = 0;
    }

    if (!ret) {
        /* Got an exception in PyObject_CallObject(), let's fail verification
         * to be safe.
         */
        cret = 0;
    } else {
        /* FIXME This is possibly problematic if ret > MAXINT */
        cret = (int)PyLong_AsLong(ret);
    }
    Py_XDECREF(ret);
    Py_XDECREF(argv);
    if (new_style_callback) {
        Py_XDECREF(_x509_store_ctx_inst);
        Py_XDECREF(_x509_store_ctx_obj);
        Py_XDECREF(_x509_store_ctx_swigptr);
        Py_XDECREF(_klass);
    } else {
        Py_XDECREF(_x509);
        Py_XDECREF(_ssl_ctx);
    }

    PyGILState_Release(gilstate);

    return cret;
}

int x509_store_verify_callback(int ok, X509_STORE_CTX *ctx) {
    PyGILState_STATE gilstate;
    PyObject *argv, *ret;
    PyObject *_x509_store_ctx_swigptr=0, *_x509_store_ctx_obj=0, *_x509_store_ctx_inst=0, *_klass=0;
    int cret;
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */
    PyObject *x509mod;


    gilstate = PyGILState_Ensure();

    /* Below, handle only what is called 'new style callback' in ssl_verify_callback().
       TODO: does 'old style callback' exist any more? */
    x509mod = PyDict_GetItemString(PyImport_GetModuleDict(), "M2Crypto.X509");
    _klass = PyObject_GetAttrString(x509mod, "X509_Store_Context");
    _x509_store_ctx_swigptr = SWIG_NewPointerObj((void *)ctx, SWIGTYPE_p_X509_STORE_CTX, 0);
    _x509_store_ctx_obj = Py_BuildValue("(Oi)", _x509_store_ctx_swigptr, 0);

    _x509_store_ctx_inst = PyObject_CallObject(_klass, _x509_store_ctx_obj);

    argv = Py_BuildValue("(iO)", ok, _x509_store_ctx_inst);

    ret = PyObject_CallObject(x509_store_verify_cb_func, argv);
    if (!ret) {
        /* Got an exception in PyObject_CallObject(), let's fail verification
         * to be safe.
         */
        cret = 0;
    } else {
        cret = (int)PyInt_AsLong(ret);
    }

    Py_XDECREF(ret);
    Py_XDECREF(argv);
    Py_XDECREF(_x509_store_ctx_inst);
    Py_XDECREF(_x509_store_ctx_obj);
    Py_XDECREF(_x509_store_ctx_swigptr);
    Py_XDECREF(_klass);

    PyGILState_Release(gilstate);
    return cret;
}

void ssl_info_callback(const SSL *s, int where, int ret) {
    PyObject *argv, *retval, *_SSL;
    PyGILState_STATE gilstate;
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    gilstate = PyGILState_Ensure();

    _SSL = SWIG_NewPointerObj((void *)s, SWIGTYPE_p_SSL, 0);
    argv = Py_BuildValue("(iiO)", where, ret, _SSL);

    retval = PyObject_CallObject(ssl_info_cb_func, argv);

    Py_XDECREF(retval);
    Py_XDECREF(argv);
    Py_XDECREF(_SSL);

    PyGILState_Release(gilstate);
}

DH *ssl_set_tmp_dh_callback(SSL *ssl, int is_export, int keylength) {
    PyObject *argv, *ret, *_ssl;
    DH *dh;
    PyGILState_STATE gilstate;
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    gilstate = PyGILState_Ensure();

    _ssl = SWIG_NewPointerObj((void *)ssl, SWIGTYPE_p_SSL, 0);
    argv = Py_BuildValue("(Oii)", _ssl, is_export, keylength);

    ret = PyObject_CallObject(ssl_set_tmp_dh_cb_func, argv);

    if ((SWIG_ConvertPtr(ret, (void **)&dh, SWIGTYPE_p_DH, SWIG_POINTER_EXCEPTION | 0)) == -1)
      dh = NULL;
    Py_XDECREF(ret);
    Py_XDECREF(argv);
    Py_XDECREF(_ssl);

    PyGILState_Release(gilstate);

    return dh;
}

RSA *ssl_set_tmp_rsa_callback(SSL *ssl, int is_export, int keylength) {
    PyObject *argv, *ret, *_ssl;
    RSA *rsa;
    PyGILState_STATE gilstate;
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    gilstate = PyGILState_Ensure();

    _ssl = SWIG_NewPointerObj((void *)ssl, SWIGTYPE_p_SSL, 0);
    argv = Py_BuildValue("(Oii)", _ssl, is_export, keylength);

    ret = PyObject_CallObject(ssl_set_tmp_rsa_cb_func, argv);

    if ((SWIG_ConvertPtr(ret, (void **)&rsa, SWIGTYPE_p_RSA, SWIG_POINTER_EXCEPTION | 0)) == -1)
      rsa = NULL;
    Py_XDECREF(ret);
    Py_XDECREF(argv);
    Py_XDECREF(_ssl);

    PyGILState_Release(gilstate);

    return rsa;
}

/* Universal callback for dh_generate_parameters,
 * dsa_generate_parameters, and rsa_generate_key */
int bn_gencb_callback(int p, int n, BN_GENCB *gencb) {
    PyObject *argv, *ret, *cbfunc;

    cbfunc = (PyObject *)BN_GENCB_get_arg(gencb);
    argv = Py_BuildValue("(ii)", p, n);
    ret = PyObject_CallObject(cbfunc, argv);
    PyErr_Clear();
    Py_DECREF(argv);
    Py_XDECREF(ret);
    return 1;
}

int passphrase_callback(char *buf, int num, int v, void *arg) {
    int i;
    Py_ssize_t len;
    char *str;
    PyObject *argv, *ret, *cbfunc;
    PyGILState_STATE gilstate;

    gilstate = PyGILState_Ensure();
    cbfunc = (PyObject *)arg;
    argv = Py_BuildValue("(i)", v);
    /* PyObject_CallObject sets exception, if needed. */
    ret = PyObject_CallObject(cbfunc, argv);
    Py_DECREF(argv);
    if (ret == NULL) {
        PyGILState_Release(gilstate);
        return -1;
    }

    if (!PyBytes_Check(ret)) {
        PyErr_SetString(PyExc_RuntimeError,
                        "Result of callback is not bytes().");
        Py_DECREF(ret);
        PyGILState_Release(gilstate);
        return -1;
    }
    if ((len = PyBytes_Size(ret)) > num)
        len = num;
    str = PyBytes_AsString(ret);

    for (i = 0; i < len; i++)
        buf[i] = str[i];
    Py_DECREF(ret);
    PyGILState_Release(gilstate);
    return len;
}
%}

%inline %{

void lib_init() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    SSLeay_add_all_algorithms();
    ERR_load_ERR_strings();
#endif
}

/* Bignum routines that aren't not numerous enough to
warrant a separate file. */

PyObject *bn_to_mpi(const BIGNUM *bn) {
    int len = 0;
    unsigned char *mpi;
    PyObject *pyo;

    len = BN_bn2mpi(bn, NULL);
    if (!(mpi=(unsigned char *)PyMem_Malloc(len))) {
        m2_PyErr_Msg(PyExc_MemoryError);
        return NULL;
    }
    len=BN_bn2mpi(bn, mpi);

    pyo=PyBytes_FromStringAndSize((const char *)mpi, len);

    PyMem_Free(mpi);
    return pyo;
}

const BIGNUM *mpi_to_bn(PyObject *value) {
    const void *vbuf = NULL;
    int vlen = 0;

    if (m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        return NULL;

    return BN_mpi2bn(vbuf, vlen, NULL);
}

PyObject *bn_to_bin(BIGNUM *bn) {
    int len = 0;
    unsigned char *bin;
    PyObject *pyo;

    len = BN_num_bytes(bn);
    if (!(bin=(unsigned char *)PyMem_Malloc(len))) {
      PyErr_SetString(PyExc_MemoryError, "bn_to_bin");
      return NULL;
    }
    BN_bn2bin(bn, bin);

    pyo=PyBytes_FromStringAndSize((const char *)bin, len);

    PyMem_Free(bin);
    return pyo;
}

const BIGNUM *bin_to_bn(PyObject *value) {
    const void *vbuf = NULL;
    int vlen = 0;

    if (m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        return NULL;

    return BN_bin2bn(vbuf, vlen, NULL);
}

PyObject *bn_to_hex(BIGNUM *bn) {
    char *hex;
    PyObject *pyo;
    Py_ssize_t len = 0;

    hex = BN_bn2hex(bn);
    if (!hex) {
        m2_PyErr_Msg(PyExc_RuntimeError);
        OPENSSL_free(hex);
        return NULL;
    }
    len = strlen(hex);

    pyo=PyBytes_FromStringAndSize(hex, len);

    OPENSSL_free(hex);
    return pyo;
}

BIGNUM *hex_to_bn(PyObject *value) {
    const void *vbuf = NULL;
    Py_ssize_t vlen = 0;
    BIGNUM *bn;
    Py_buffer view;

    if (PyObject_CheckBuffer(value)) {
        if (PyObject_GetBuffer(value, &view, PyBUF_SIMPLE) == 0) {
            vbuf = view.buf;
            vlen = view.len;
        }
    }
    else {
        if (m2_PyObject_AsReadBuffer(value, &vbuf, &vlen) == -1)
            return NULL;
    }

    if ((bn=BN_new())==NULL) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_MemoryError, "hex_to_bn");
        return NULL;
    }
    if (BN_hex2bn(&bn, (const char *)vbuf) <= 0) {
        PyBuffer_Release(&view);
        m2_PyErr_Msg(PyExc_RuntimeError);
        BN_free(bn);
        return NULL;
    }
    PyBuffer_Release(&view);
    return bn;
}

BIGNUM *dec_to_bn(PyObject *value) {
    const void *vbuf = NULL;
    Py_ssize_t vlen = 0;
    BIGNUM *bn;
    Py_buffer view;

    if (PyObject_CheckBuffer(value)) {
        if (PyObject_GetBuffer(value, &view, PyBUF_SIMPLE) == 0) {
            vbuf = view.buf;
            vlen = view.len;
        }
    }
    else {
        if (m2_PyObject_AsReadBuffer(value, &vbuf, &vlen) == -1)
            return NULL;
    }

    if ((bn=BN_new())==NULL) {
      PyBuffer_Release(&view);
      PyErr_SetString(PyExc_MemoryError, "dec_to_bn");
      return NULL;
    }
    if ((BN_dec2bn(&bn, (const char *)vbuf) <= 0)) {
      PyBuffer_Release(&view);
      m2_PyErr_Msg(PyExc_RuntimeError);
      BN_free(bn);
      return NULL;
    }
    PyBuffer_Release(&view);
    return bn;
}
%}


/* Various useful typemaps. */

%typemap(in) Blob * {
    Py_ssize_t len = 0;

    if (!PyBytes_Check($input)) {
        PyErr_SetString(PyExc_TypeError, "expected PyString");
        return NULL;
    }
    len=PyBytes_Size($input);

    if (len > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "object too large");
        return NULL;
    }
    $1=(Blob *)PyMem_Malloc(sizeof(Blob));
    if (!$1) {
        PyErr_SetString(PyExc_MemoryError, "malloc Blob");
        return NULL;
    }

    $1->data=(unsigned char *)PyBytes_AsString($input);

    $1->len=len;
}

%typemap(out) Blob * {
    if ($1==NULL) {
        Py_INCREF(Py_None);
        $result=Py_None;
    } else {

        $result=PyBytes_FromStringAndSize((const char *)$1->data, $1->len);

        PyMem_Free($1->data);
        PyMem_Free($1);
    }
}

%typemap(in) PyObject *pyfunc {
    if (!PyCallable_Check($input)) {
        PyErr_SetString(PyExc_TypeError, "expected PyCallable");
        return NULL;
    }
    $1=$input;
}

%typemap(in) PyObject *pyblob {
    if (!PyBytes_Check($input)) {

        PyErr_SetString(PyExc_TypeError, "expected PyString");
        return NULL;
    }
    $1=$input;
}

%typemap(in) PyObject * {
    $1=$input;
}

%typemap(out) PyObject * {
    $result=$1;
}

%typemap(out) int {
    $result=PyLong_FromLong($1);
    if (PyErr_Occurred()) SWIG_fail;
}

/* Pointer checks. */

%apply Pointer NONNULL { Blob * };


/* A bunch of "straight-thru" functions. */

%rename(err_print_errors) ERR_print_errors;
%threadallow ERR_print_errors;
extern void ERR_print_errors(BIO *);
%rename(err_clear_error) ERR_clear_error;
extern void ERR_clear_error(void);
%rename(err_get_error) ERR_get_error;
extern unsigned long ERR_get_error(void);
%rename(err_peek_error) ERR_peek_error;
extern unsigned long ERR_peek_error(void);
%rename(err_lib_error_string) ERR_lib_error_string;
extern const char *ERR_lib_error_string(unsigned long);
%rename(err_func_error_string) ERR_func_error_string;
extern const char *ERR_func_error_string(unsigned long);
%rename(err_reason_error_string) ERR_reason_error_string;
extern const char *ERR_reason_error_string(unsigned long);
