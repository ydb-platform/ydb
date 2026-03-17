/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.  */
/*
** Portions created by Open Source Applications Foundation (OSAF) are
** Copyright (C) 2004-2005 OSAF. All Rights Reserved.
**
** Copyright (c) 2009-2010 Heikki Toivonen. All rights reserved.
**
*/
/* $Id$   */

%{
#include <openssl/asn1.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <openssl/asn1t.h>

typedef STACK_OF(X509) SEQ_CERT;

ASN1_ITEM_TEMPLATE(SEQ_CERT) =
    ASN1_EX_TEMPLATE_TYPE(ASN1_TFLG_SEQUENCE_OF, 0, SeqCert, X509)
ASN1_ITEM_TEMPLATE_END(SEQ_CERT)

IMPLEMENT_ASN1_FUNCTIONS(SEQ_CERT)
%}

%include "x509_v_flag.h"

%apply Pointer NONNULL { BIO * };
%apply Pointer NONNULL { X509 * };
%apply Pointer NONNULL { X509_CRL * };
%apply Pointer NONNULL { X509_REQ * };
%apply Pointer NONNULL { X509_NAME * };
%apply Pointer NONNULL { X509_NAME_ENTRY * };
%apply Pointer NONNULL { EVP_PKEY * };

#if OPENSSL_VERSION_NUMBER >= 0x0090800fL
%rename(x509_check_ca) X509_check_ca;
extern int X509_check_ca(X509 *);
#endif

%rename(x509_new) X509_new;
extern X509 *X509_new( void );
%rename(x509_dup) X509_dup;
extern X509 *X509_dup(X509 *);
%rename(x509_free) X509_free;
extern void X509_free(X509 *);
%rename(x509_crl_free) X509_CRL_free;
extern void X509_CRL_free(X509_CRL *);
%rename(x509_crl_new) X509_CRL_new;
extern X509_CRL * X509_CRL_new();

%rename(x509_print) X509_print;
%threadallow X509_print;
extern int X509_print(BIO *, X509 *);
%rename(x509_crl_print) X509_CRL_print;
%threadallow X509_CRL_print;
extern int X509_CRL_print(BIO *, X509_CRL *);

%rename(x509_get_serial_number) X509_get_serialNumber;
extern ASN1_INTEGER *X509_get_serialNumber(X509 *);
%rename(x509_set_serial_number) X509_set_serialNumber;
extern int X509_set_serialNumber(X509 *, ASN1_INTEGER *);
%rename(x509_get_pubkey) X509_get_pubkey;
extern EVP_PKEY *X509_get_pubkey(X509 *);
%rename(x509_set_pubkey) X509_set_pubkey;
extern int X509_set_pubkey(X509 *, EVP_PKEY *);
%rename(x509_get_issuer_name) X509_get_issuer_name;
extern X509_NAME *X509_get_issuer_name(X509 *);
%rename(x509_set_issuer_name) X509_set_issuer_name;
extern int X509_set_issuer_name(X509 *, X509_NAME *);
%rename(x509_get_subject_name) X509_get_subject_name;
extern X509_NAME *X509_get_subject_name(X509 *);
%rename(x509_set_subject_name) X509_set_subject_name;
extern int X509_set_subject_name(X509 *, X509_NAME *);
%rename(x509_cmp_current_time) X509_cmp_current_time;
extern int X509_cmp_current_time(ASN1_TIME *);


/* From x509.h */
/* standard trust ids */
%constant int X509_TRUST_DEFAULT      = -1;
%constant int X509_TRUST_COMPAT       = 1;
%constant int X509_TRUST_SSL_CLIENT   = 2;
%constant int X509_TRUST_SSL_SERVER   = 3;
%constant int X509_TRUST_EMAIL        = 4;
%constant int X509_TRUST_OBJECT_SIGN  = 5;
%constant int X509_TRUST_OCSP_SIGN    = 6;
%constant int X509_TRUST_OCSP_REQUEST = 7;

/* trust_flags values */
%constant int X509_TRUST_DYNAMIC      = 1;
%constant int X509_TRUST_DYNAMIC_NAME = 2;

/* check_trust return codes */
%constant int X509_TRUST_TRUSTED      = 1;
%constant int X509_TRUST_REJECTED     = 2;
%constant int X509_TRUST_UNTRUSTED    = 3;

/* From x509v3.h */
%constant int X509_PURPOSE_SSL_CLIENT         = 1;
%constant int X509_PURPOSE_SSL_SERVER         = 2;
%constant int X509_PURPOSE_NS_SSL_SERVER      = 3;
%constant int X509_PURPOSE_SMIME_SIGN         = 4;
%constant int X509_PURPOSE_SMIME_ENCRYPT      = 5;
%constant int X509_PURPOSE_CRL_SIGN           = 6;
%constant int X509_PURPOSE_ANY                = 7;
%constant int X509_PURPOSE_OCSP_HELPER        = 8;

%rename(x509_check_purpose) X509_check_purpose;
extern int X509_check_purpose(X509 *, int, int);
%rename(x509_check_trust) X509_check_trust;
extern int X509_check_trust(X509 *, int, int);

%rename(x509_write_pem) PEM_write_bio_X509;
%threadallow PEM_write_bio_X509;
extern int PEM_write_bio_X509(BIO *, X509 *);
%rename(x509_write_pem_file) PEM_write_X509;
extern int PEM_write_X509(FILE *, X509 *);

%rename(x509_verify) X509_verify;
extern int X509_verify(X509 *a, EVP_PKEY *r);
%rename(x509_verify_cert) X509_verify_cert;
extern int X509_verify_cert(X509_STORE_CTX *ctx);
%rename(x509_get_verify_error) X509_verify_cert_error_string;
extern const char *X509_verify_cert_error_string(long);

%constant long X509V3_EXT_UNKNOWN_MASK         = (0xfL << 16);
%constant long X509V3_EXT_DEFAULT              = 0;
%constant long X509V3_EXT_ERROR_UNKNOWN        = (1L << 16);
%constant long X509V3_EXT_PARSE_UNKNOWN        = (2L << 16);
%constant long X509V3_EXT_DUMP_UNKNOWN         = (3L << 16);

%rename(x509_add_ext) X509_add_ext;
extern int X509_add_ext(X509 *, X509_EXTENSION *, int);
%rename(x509_get_ext_count) X509_get_ext_count;
extern int X509_get_ext_count(X509 *);
%rename(x509_get_ext) X509_get_ext;
extern X509_EXTENSION *X509_get_ext(X509 *, int);
%rename(x509_ext_print) X509V3_EXT_print;
%threadallow X509V3_EXT_print;
extern int X509V3_EXT_print(BIO *, X509_EXTENSION *, unsigned long, int);

%rename(x509_name_new) X509_NAME_new;
extern X509_NAME *X509_NAME_new( void );
%rename(x509_name_free) X509_NAME_free;
extern void X509_NAME_free(X509_NAME *);
%rename(x509_name_print) X509_NAME_print;
%threadallow X509_NAME_print;
extern int X509_NAME_print(BIO *, X509_NAME *, int);
%rename(x509_name_get_entry) X509_NAME_get_entry;
extern X509_NAME_ENTRY *X509_NAME_get_entry(X509_NAME *, int);
%rename(x509_name_entry_count) X509_NAME_entry_count;
extern int X509_NAME_entry_count(X509_NAME *);
%rename(x509_name_delete_entry) X509_NAME_delete_entry;
extern X509_NAME_ENTRY *X509_NAME_delete_entry(X509_NAME *, int);
%rename(x509_name_add_entry) X509_NAME_add_entry;
extern int X509_NAME_add_entry(X509_NAME *, X509_NAME_ENTRY *, int, int);
%rename(x509_name_add_entry_by_obj) X509_NAME_add_entry_by_OBJ;
extern int X509_NAME_add_entry_by_OBJ(X509_NAME *, ASN1_OBJECT *, int, unsigned char *, int, int, int );
%rename(x509_name_add_entry_by_nid) X509_NAME_add_entry_by_NID;
extern int X509_NAME_add_entry_by_NID(X509_NAME *, int, int, unsigned char *, int, int, int );
%rename(x509_name_print_ex) X509_NAME_print_ex;
%threadallow X509_NAME_print_ex;
extern int X509_NAME_print_ex(BIO *, X509_NAME *, int, unsigned long);

#if OPENSSL_VERSION_NUMBER >= 0x10000000L
%rename(x509_name_hash) X509_NAME_hash_old;
extern unsigned long X509_NAME_hash_old(X509_NAME *);
#else
%rename(x509_name_hash) X509_NAME_hash;
extern unsigned long X509_NAME_hash(X509_NAME *);
#endif

%rename(x509_name_get_index_by_nid) X509_NAME_get_index_by_NID;
extern int X509_NAME_get_index_by_NID(X509_NAME *, int, int);

%rename(x509_name_entry_new) X509_NAME_ENTRY_new;
extern X509_NAME_ENTRY *X509_NAME_ENTRY_new( void );
%rename(x509_name_entry_free) X509_NAME_ENTRY_free;
extern void X509_NAME_ENTRY_free( X509_NAME_ENTRY *);
/*XXX This is probably bogus:*/
%rename(x509_name_entry_create_by_nid) X509_NAME_ENTRY_create_by_NID;
extern X509_NAME_ENTRY *X509_NAME_ENTRY_create_by_NID( X509_NAME_ENTRY **, int, int, unsigned char *, int);
%rename(x509_name_entry_set_object) X509_NAME_ENTRY_set_object;
extern int X509_NAME_ENTRY_set_object( X509_NAME_ENTRY *, ASN1_OBJECT *);
%rename(x509_name_entry_get_object) X509_NAME_ENTRY_get_object;
extern ASN1_OBJECT *X509_NAME_ENTRY_get_object(X509_NAME_ENTRY *);
%rename(x509_name_entry_get_data) X509_NAME_ENTRY_get_data;
extern ASN1_STRING *X509_NAME_ENTRY_get_data(X509_NAME_ENTRY *);

%typemap(in) (const unsigned char *, int) {
#if PY_MAJOR_VERSION >= 3
    if (PyBytes_Check($input)) {
        Py_ssize_t len;

        $1 = PyBytes_AsString($input);
        len = PyBytes_Size($input);
#else
    if (PyString_Check($input)) {
        Py_ssize_t len;

        $1 = (unsigned char *)PyString_AsString($input);
        len = PyString_Size($input);
#endif // PY_MAJOR_VERSION >= 3

        if (len > INT_MAX) {
            PyErr_SetString(_x509_err, "object too large");
            return NULL;
        }
        $2 = len;
    } else {
        PyErr_SetString(PyExc_TypeError, "expected string");
        return NULL;
    }
}
%rename(x509_name_entry_set_data) X509_NAME_ENTRY_set_data;
extern int X509_NAME_ENTRY_set_data(X509_NAME_ENTRY *, int, const unsigned char *, int);
%typemap(in) (const unsigned char *, int);

%rename(x509_req_new) X509_REQ_new;
extern X509_REQ * X509_REQ_new();
%rename(x509_req_free) X509_REQ_free;
extern void X509_REQ_free(X509_REQ *);
%rename(x509_req_print) X509_REQ_print;
%threadallow X509_REQ_print;
extern int X509_REQ_print(BIO *, X509_REQ *);

%rename(x509_req_get_pubkey) X509_REQ_get_pubkey;
extern EVP_PKEY *X509_REQ_get_pubkey(X509_REQ *);
%rename(x509_req_set_pubkey) X509_REQ_set_pubkey;
extern int X509_REQ_set_pubkey(X509_REQ *, EVP_PKEY *);
%rename(x509_req_set_subject_name) X509_REQ_set_subject_name;
extern int X509_REQ_set_subject_name(X509_REQ *, X509_NAME *);

%rename(x509_req_verify) X509_REQ_verify;
extern int X509_REQ_verify(X509_REQ *, EVP_PKEY *);
%rename(x509_req_sign) X509_REQ_sign;
extern int X509_REQ_sign(X509_REQ *, EVP_PKEY *, const EVP_MD *);

%rename(i2d_x509_bio) i2d_X509_bio;
%threadallow i2d_X509_bio;
extern int i2d_X509_bio(BIO *, X509 *);
%rename(i2d_x509_req_bio) i2d_X509_REQ_bio;
%threadallow i2d_X509_REQ_bio;
extern int i2d_X509_REQ_bio(BIO *, X509_REQ *);

%rename(x509_store_new) X509_STORE_new;
extern X509_STORE *X509_STORE_new(void);
%rename(x509_store_free) X509_STORE_free;
extern void X509_STORE_free(X509_STORE *);
%rename(x509_store_add_cert) X509_STORE_add_cert;
extern int X509_STORE_add_cert(X509_STORE *, X509 *);
%rename(x509_store_set_verify_cb) X509_STORE_set_verify_cb;
extern void X509_STORE_set_verify_cb(X509_STORE *st,
                                     int (*verify_cb)(int ok, X509_STORE_CTX *ctx));

%rename(x509_store_ctx_get_current_cert) X509_STORE_CTX_get_current_cert;
extern X509 *X509_STORE_CTX_get_current_cert(X509_STORE_CTX *);
%rename(x509_store_ctx_get_error) X509_STORE_CTX_get_error;
extern int X509_STORE_CTX_get_error(X509_STORE_CTX *);
%rename(x509_store_ctx_get_error_depth) X509_STORE_CTX_get_error_depth;
extern int X509_STORE_CTX_get_error_depth(X509_STORE_CTX *);
%rename(x509_store_ctx_free) X509_STORE_CTX_free;
extern void X509_STORE_CTX_free(X509_STORE_CTX *);
%rename(x509_store_ctx_get1_chain) X509_STORE_CTX_get1_chain;
extern STACK_OF(X509) *X509_STORE_CTX_get1_chain(X509_STORE_CTX *);

%rename(x509_extension_get_critical) X509_EXTENSION_get_critical;
extern int X509_EXTENSION_get_critical(X509_EXTENSION *);
%rename(x509_extension_set_critical) X509_EXTENSION_set_critical;
extern int X509_EXTENSION_set_critical(X509_EXTENSION *, int);


%rename(x509_store_set_flags) X509_STORE_set_flags;
extern int X509_STORE_set_flags(X509_STORE *ctx, unsigned long flags);


%typemap(out) X509 * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        m2_PyErr_Msg(_x509_err);
        $result = NULL;
    }
}


/* Functions using m2_PyErr_Msg and thus using internal Python C API are
 * not thread safe, so if we want to have %threadallow here, error
 * handling must be done outside of these internal functions. */
%threadallow x509_read_pem;
%inline %{
X509 *x509_read_pem(BIO *bio) {
    return PEM_read_bio_X509(bio, NULL, NULL, NULL);
}
%}

%threadallow d2i_x509;
%inline %{
X509 *d2i_x509(BIO *bio) {
    return d2i_X509_bio(bio, NULL);
}
%}
%typemap(out) X509 *;

%constant int NID_commonName                  = 13;
%constant int NID_countryName                 = 14;
%constant int NID_localityName                = 15;
%constant int NID_stateOrProvinceName         = 16;
%constant int NID_organizationName            = 17;
%constant int NID_organizationalUnitName      = 18;
%constant int NID_serialNumber                = 105;
%constant int NID_surname                     = 100;
%constant int NID_givenName                   = 99;
%constant int NID_pkcs9_emailAddress          = 48;

/* Cribbed from x509_vfy.h. */
%constant int        X509_V_OK                                      = 0;
%constant int        X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT           = 2;
%constant int        X509_V_ERR_UNABLE_TO_GET_CRL                   = 3;
%constant int        X509_V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE    = 4;
%constant int        X509_V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE     = 5;
%constant int        X509_V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY  = 6;
%constant int        X509_V_ERR_CERT_SIGNATURE_FAILURE              = 7;
%constant int        X509_V_ERR_CRL_SIGNATURE_FAILURE               = 8;
%constant int        X509_V_ERR_CERT_NOT_YET_VALID                  = 9;
%constant int        X509_V_ERR_CERT_HAS_EXPIRED                    = 10;
%constant int        X509_V_ERR_CRL_NOT_YET_VALID                   = 11;
%constant int        X509_V_ERR_CRL_HAS_EXPIRED                     = 12;
%constant int        X509_V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD      = 13;
%constant int        X509_V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD       = 14;
%constant int        X509_V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD      = 15;
%constant int        X509_V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD      = 16;
%constant int        X509_V_ERR_OUT_OF_MEM                          = 17;
%constant int        X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT         = 18;
%constant int        X509_V_ERR_SELF_SIGNED_CERT_IN_CHAIN           = 19;
%constant int        X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY   = 20;
%constant int        X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE     = 21;
%constant int        X509_V_ERR_CERT_CHAIN_TOO_LONG                 = 22;
%constant int        X509_V_ERR_CERT_REVOKED                        = 23;
%constant int        X509_V_ERR_INVALID_CA                          = 24;
%constant int        X509_V_ERR_PATH_LENGTH_EXCEEDED                = 25;
%constant int        X509_V_ERR_INVALID_PURPOSE                     = 26;
%constant int        X509_V_ERR_CERT_UNTRUSTED                      = 27;
%constant int        X509_V_ERR_CERT_REJECTED                       = 28;
%constant int        X509_V_ERR_APPLICATION_VERIFICATION            = 50;

/* See man page of X509_VERIFY_PARAM_set_flags for definition of all these flags */

#ifdef X509_V_FLAG_ALLOW_PROXY_CERTS
%constant int VERIFY_ALLOW_PROXY_CERTS  = X509_V_FLAG_ALLOW_PROXY_CERTS;
#endif
#ifdef X509_V_FLAG_CB_ISSUER_CHECK
%constant int VERIFY_CB_ISSUER_CHECK  = X509_V_FLAG_CB_ISSUER_CHECK;
#endif
#ifdef X509_V_FLAG_CHECK_SS_SIGNATURE
%constant int VERIFY_CHECK_SS_SIGNATURE  = X509_V_FLAG_CHECK_SS_SIGNATURE;
#endif
/* note: X509_V_FLAG_CRL_CHECK is already defined in _ssl.i as VERIFY_CRL_CHECK_LEAF
However I add it here for consistency */
#ifdef X509_V_FLAG_CRL_CHECK
%constant int VERIFY_CRL_CHECK  = X509_V_FLAG_CRL_CHECK;
#endif
#ifdef X509_V_FLAG_CRL_CHECK_ALL
%constant int VERIFY_CRL_CHECK_ALL  = X509_V_FLAG_CRL_CHECK_ALL;
#endif
#ifdef X509_V_FLAG_EXPLICIT_POLICY
%constant int VERIFY_EXPLICIT_POLICY  = X509_V_FLAG_EXPLICIT_POLICY;
#endif
#ifdef X509_V_FLAG_EXTENDED_CRL_SUPPORT
%constant int VERIFY_EXTENDED_CRL_SUPPORT  = X509_V_FLAG_EXTENDED_CRL_SUPPORT;
#endif
#ifdef X509_V_FLAG_IGNORE_CRITICAL
%constant int VERIFY_IGNORE_CRITICAL  = X509_V_FLAG_IGNORE_CRITICAL;
#endif
#ifdef X509_V_FLAG_INHIBIT_ANY
%constant int VERIFY_INHIBIT_ANY  = X509_V_FLAG_INHIBIT_ANY;
#endif
#ifdef X509_V_FLAG_INHIBIT_MAP
%constant int VERIFY_INHIBIT_MAP  = X509_V_FLAG_INHIBIT_MAP;
#endif
#ifdef X509_V_FLAG_NO_ALT_CHAINS
%constant int VERIFY_NO_ALT_CHAINS  = X509_V_FLAG_NO_ALT_CHAINS;
#endif
#ifdef X509_V_FLAG_NO_CHECK_TIME
%constant int VERIFY_NO_CHECK_TIME  = X509_V_FLAG_NO_CHECK_TIME;
#endif
#ifdef X509_V_FLAG_NOTIFY_POLICY
%constant int VERIFY_NOTIFY_POLICY  = X509_V_FLAG_NOTIFY_POLICY;
#endif
#ifdef X509_V_FLAG_PARTIAL_CHAIN
%constant int VERIFY_PARTIAL_CHAIN  = X509_V_FLAG_PARTIAL_CHAIN;
#endif
#ifdef X509_V_FLAG_POLICY_CHECK
%constant int VERIFY_POLICY_CHECK  = X509_V_FLAG_POLICY_CHECK;
#endif
#ifdef X509_V_FLAG_TRUSTED_FIRST
%constant int VERIFY_TRUSTED_FIRST  = X509_V_FLAG_TRUSTED_FIRST;
#endif
#ifdef X509_V_FLAG_USE_DELTAS
%constant int VERIFY_USE_DELTAS  = X509_V_FLAG_USE_DELTAS;
#endif
#ifdef X509_V_FLAG_X509_STRICT
%constant int VERIFY_X509_STRICT  = X509_V_FLAG_X509_STRICT;
#endif



/* x509.h */
%constant int XN_FLAG_COMPAT = 0;
%constant int XN_FLAG_SEP_COMMA_PLUS = (1 << 16);
%constant int XN_FLAG_SEP_CPLUS_SPC = (2 << 16);
%constant int XN_FLAG_SEP_MULTILINE = (4 << 16);
%constant int XN_FLAG_DN_REV = (1 << 20);
%constant int XN_FLAG_FN_LN = (1 << 21);
%constant int XN_FLAG_SPC_EQ = (1 << 23);
%constant int XN_FLAG_DUMP_UNKNOWN_FIELDS = (1 << 24);
%constant int XN_FLAG_FN_ALIGN = (1 << 25);
%constant int XN_FLAG_ONELINE =(ASN1_STRFLGS_RFC2253 | \
            ASN1_STRFLGS_ESC_QUOTE | \
            XN_FLAG_SEP_CPLUS_SPC | \
            XN_FLAG_SPC_EQ);
%constant int XN_FLAG_MULTILINE = (ASN1_STRFLGS_ESC_CTRL | \
            ASN1_STRFLGS_ESC_MSB | \
            XN_FLAG_SEP_MULTILINE | \
            XN_FLAG_SPC_EQ | \
            XN_FLAG_FN_LN | \
            XN_FLAG_FN_ALIGN);
%constant int XN_FLAG_RFC2253 = (ASN1_STRFLGS_RFC2253 | \
            XN_FLAG_SEP_COMMA_PLUS | \
            XN_FLAG_DN_REV | \
            XN_FLAG_DUMP_UNKNOWN_FIELDS);

/* Cribbed from rsa.h. */
%constant int RSA_3                           = 0x3L;
%constant int RSA_F4                          = 0x10001L;

%warnfilter(454) _x509_err;
%inline %{
static PyObject *_x509_err;

void x509_init(PyObject *x509_err) {
    Py_INCREF(x509_err);
    _x509_err = x509_err;
}
%}

%typemap(out) X509_REQ * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        m2_PyErr_Msg(_x509_err);
        $result = NULL;
    }
}
%threadallow d2i_x509_req;
%inline %{
X509_REQ *d2i_x509_req(BIO *bio) {
    return d2i_X509_REQ_bio(bio, NULL);
}
%}

%threadallow x509_req_read_pem;
%inline %{
X509_REQ *x509_req_read_pem(BIO *bio) {
    return PEM_read_bio_X509_REQ(bio, NULL, NULL, NULL);
}
%}

%typemap(out) X509_REQ *;

%inline %{
PyObject *i2d_x509(X509 *x) {
    int len;
    PyObject *ret = NULL;
    unsigned char *buf = NULL;
    len = i2d_X509(x, &buf);
    if (len < 0) {
        m2_PyErr_Msg(_x509_err);
    }
    else {

        ret = PyBytes_FromStringAndSize((char*)buf, len);

        OPENSSL_free(buf);
    }
    return ret;
}
%}

%threadallow x509_req_write_pem;
%inline %{
int x509_req_write_pem(BIO *bio, X509_REQ *x) {
    return PEM_write_bio_X509_REQ(bio, x);
}
%}

%typemap(out) X509_CRL * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        m2_PyErr_Msg(_x509_err);
        $result = NULL;
    }
}
%threadallow x509_crl_read_pem;
%inline %{
X509_CRL *x509_crl_read_pem(BIO *bio) {
    return PEM_read_bio_X509_CRL(bio, NULL, NULL, NULL);
}
%}
%typemap(out) X509_CRL * ;

%inline %{
/* X509_set_version() is a macro. */
int x509_set_version(X509 *x, long version) {
    return X509_set_version(x, version);
}

/* X509_get_version() is a macro. */
long x509_get_version(X509 *x) {
    return X509_get_version(x);
}

/* X509_set_notBefore() is a macro. */
int x509_set_not_before(X509 *x, ASN1_TIME *tm) {
    return X509_set_notBefore(x, tm);
}

/* X509_get_notBefore() is a macro. */
ASN1_TIME *x509_get_not_before(X509 *x) {
    return X509_get_notBefore(x);
}

/* X509_set_notAfter() is a macro. */
int x509_set_not_after(X509 *x, ASN1_TIME *tm) {
    return X509_set_notAfter(x, tm);
}

/* X509_get_notAfter() is a macro. */
ASN1_TIME *x509_get_not_after(X509 *x) {
    return X509_get_notAfter(x);
}

int x509_sign(X509 *x, EVP_PKEY *pkey, EVP_MD *md) {
    return X509_sign(x, pkey, md);
}

/* x509_gmtime_adj() is a macro. */
ASN1_TIME *x509_gmtime_adj(ASN1_TIME *s, long adj) {
    return X509_gmtime_adj(s, adj);
}

PyObject *x509_name_by_nid(X509_NAME *name, int nid) {
    void *buf;
    int len, xlen;
    PyObject *ret;

    if ((len = X509_NAME_get_text_by_NID(name, nid, NULL, 0)) == -1) {
        Py_RETURN_NONE;
    }
    len++;
    if (!(buf = PyMem_Malloc(len))) {
        PyErr_SetString(PyExc_MemoryError, "x509_name_by_nid");
        return NULL;
    }
    xlen = X509_NAME_get_text_by_NID(name, nid, buf, len);

    ret = PyBytes_FromStringAndSize(buf, xlen);

    PyMem_Free(buf);
    return ret;
}

int x509_name_set_by_nid(X509_NAME *name, int nid, PyObject *obj) {
    return X509_NAME_add_entry_by_NID(name, nid, MBSTRING_ASC, (unsigned char *)PyBytes_AsString(obj), -1, -1, 0);
}

/* x509_name_add_entry_by_txt */
int x509_name_add_entry_by_txt(X509_NAME *name, char *field, int type, char *bytes, int len, int loc, int set) {
    return X509_NAME_add_entry_by_txt(name, field, type, (unsigned char *)bytes, len, loc, set);
}

PyObject *x509_name_get_der(X509_NAME *name) {
    const char* pder="";
    size_t pderlen;
    i2d_X509_NAME(name, 0);
    if (!X509_NAME_get0_der(name, (const unsigned char **)&pder, &pderlen)) {
        m2_PyErr_Msg(_x509_err);
        return NULL;
    }
    return PyBytes_FromStringAndSize(pder, pderlen);
}

/* sk_X509_free() is a macro. */
void sk_x509_free(STACK_OF(X509) *stack) {
    sk_X509_free(stack);
}

/* sk_X509_push() is a macro. */
int sk_x509_push(STACK_OF(X509) *stack, X509 *x509) {
    return sk_X509_push(stack, x509);
}

/* sk_X509_pop() is a macro. */
X509 *sk_x509_pop(STACK_OF(X509) *stack) {
    return sk_X509_pop(stack);
}
%}

%inline %{
int x509_store_load_locations(X509_STORE *store, const char *file) {
    int locations = 0;

    if ((locations = X509_STORE_load_locations(store, file, NULL)) < 1) {
        m2_PyErr_Msg(_x509_err);
    }
    return locations;
}

int x509_type_check(X509 *x509) {
    return 1;
}

int x509_name_type_check(X509_NAME *name) {
    return 1;
}

X509_NAME *x509_req_get_subject_name(X509_REQ *x) {
    return X509_REQ_get_subject_name(x);
}

long x509_req_get_version(X509_REQ *x) {
    return X509_REQ_get_version(x);
}

int x509_req_set_version(X509_REQ *x, long version) {
    return X509_REQ_set_version(x, version);
}

int x509_req_add_extensions(X509_REQ *req, STACK_OF(X509_EXTENSION) *exts) {
    return X509_REQ_add_extensions(req, exts);
}

X509_NAME_ENTRY *x509_name_entry_create_by_txt(X509_NAME_ENTRY **ne, char *field, int type, char *bytes, int len) {
    return X509_NAME_ENTRY_create_by_txt( ne, field, type, (unsigned char *)bytes, len);
}
%}

%typemap(out) X509V3_CTX * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        $result = NULL;
    }
}
%inline %{
X509V3_CTX *
x509v3_set_nconf(void) {
      X509V3_CTX * ctx;
      CONF *conf = NCONF_new(NULL);

      if (!(ctx=(X509V3_CTX *)PyMem_Malloc(sizeof(X509V3_CTX)))) {
          PyErr_SetString(PyExc_MemoryError, "x509v3_set_nconf");
          return NULL;
      }
      /* X509V3_set_nconf does not generate any error signs at all. */
      X509V3_set_nconf(ctx, conf);
      return ctx;
}
%}
%typemap(out) X509V3_CTX * ;

%typemap(out) X509_EXTENSION * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        m2_PyErr_Msg(_x509_err);
        $result = NULL;
    }
}
%inline %{
X509_EXTENSION *
x509v3_ext_conf(void *conf, X509V3_CTX *ctx, char *name, char *value) {
      X509_EXTENSION * ext = NULL;
      ext = X509V3_EXT_conf(conf, ctx, name, value);
      PyMem_Free(ctx);
      return ext;
}
%}
%typemap(out) X509_EXTENSION * ;

%inline %{
/* X509_EXTENSION_free() might be a macro, didn't find definition. */
void x509_extension_free(X509_EXTENSION *ext) {
    X509_EXTENSION_free(ext);
}

PyObject *x509_extension_get_name(X509_EXTENSION *ext) {
    PyObject * ext_name;
    const char * ext_name_str;
    ext_name_str = OBJ_nid2sn(OBJ_obj2nid(X509_EXTENSION_get_object(ext)));
    if (!ext_name_str) {
        m2_PyErr_Msg(_x509_err);
        return NULL;
    }
    ext_name = PyBytes_FromStringAndSize(ext_name_str, strlen(ext_name_str));
    return ext_name;
}

/* sk_X509_EXTENSION_new_null is a macro. */
STACK_OF(X509_EXTENSION) *sk_x509_extension_new_null(void) {
    return sk_X509_EXTENSION_new_null();
}

/* sk_X509_EXTENSION_free() is a macro. */
void sk_x509_extension_free(STACK_OF(X509_EXTENSION) *stack) {
    sk_X509_EXTENSION_free(stack);
}

/* sk_X509_EXTENSION_push() is a macro. */
int sk_x509_extension_push(STACK_OF(X509_EXTENSION) *stack, X509_EXTENSION *x509_ext) {
    return sk_X509_EXTENSION_push(stack, x509_ext);
}

/* sk_X509_EXTENSION_pop() is a macro. */
X509_EXTENSION *sk_x509_extension_pop(STACK_OF(X509_EXTENSION) *stack) {
    return sk_X509_EXTENSION_pop(stack);
}

/* sk_X509_EXTENSION_num() is a macro. */
int sk_x509_extension_num(STACK_OF(X509_EXTENSION) *stack) {
    return sk_X509_EXTENSION_num(stack);
}

/* sk_X509_EXTENSION_value() is a macro. */
X509_EXTENSION *sk_x509_extension_value(STACK_OF(X509_EXTENSION) *stack, int i) {
    return sk_X509_EXTENSION_value(stack, i);
}

/* X509_STORE_CTX_get_app_data is a macro. */
void *x509_store_ctx_get_app_data(X509_STORE_CTX *ctx) {
  return X509_STORE_CTX_get_app_data(ctx);
}

/* X509_STORE_CTX_get_app_data is a macro. */
void *x509_store_ctx_get_ex_data(X509_STORE_CTX *ctx, int idx) {
  return X509_STORE_CTX_get_ex_data(ctx, idx);
}

void x509_store_set_verify_cb(X509_STORE *store, PyObject *pyfunc) {
    Py_XDECREF(x509_store_verify_cb_func);
    Py_INCREF(pyfunc);
    x509_store_verify_cb_func = pyfunc;
    X509_STORE_set_verify_cb(store, x509_store_verify_callback);
}
%}

%typemap(out) STACK_OF(X509) * {
    PyObject *self = NULL; /* bug in SWIG_NewPointerObj as of 3.0.5 */

    if ($1 != NULL)
        $result = SWIG_NewPointerObj($1, $1_descriptor, 0);
    else {
        $result = NULL;
    }
}

%inline %{
STACK_OF(X509) *
make_stack_from_der_sequence(PyObject * pyEncodedString){
    STACK_OF(X509) *certs;
    Py_ssize_t encoded_string_len;
    char *encoded_string;
    const unsigned char *tmp_str;

    encoded_string_len = PyBytes_Size(pyEncodedString);

    if (encoded_string_len > INT_MAX) {
        PyErr_Format(_x509_err, "object too large");
        return NULL;
    }

    encoded_string = PyBytes_AsString(pyEncodedString);

    if (!encoded_string) {
        PyErr_SetString(_x509_err,
                        "Cannot convert Python Bytes to (char *).");
        return NULL;
    }

    tmp_str = (unsigned char *)encoded_string;
    certs = d2i_SEQ_CERT(NULL, &tmp_str, encoded_string_len);
    if (certs == NULL) {
        PyErr_SetString(_x509_err, "Generating STACK_OF(X509) failed.");
        return NULL;
    }
    return certs;
}

/* sk_X509_new_null() is a macro returning "STACK_OF(X509) *". */
STACK_OF(X509) *sk_x509_new_null(void) {
    return sk_X509_new_null();
}
%}

%typemap(out) STACK_OF(X509) *;

%inline %{
PyObject *
get_der_encoding_stack(STACK_OF(X509) *stack){
    PyObject * encodedString;

    unsigned char * encoding = NULL;
    int len;

    len = i2d_SEQ_CERT(stack, &encoding);
    if (!encoding) {
       m2_PyErr_Msg(_x509_err);
       return NULL;
    }

    encodedString = PyBytes_FromStringAndSize((const char *)encoding, len);

    if (encoding)
        OPENSSL_free(encoding);

    return encodedString;
}

%}

/* Free malloc'ed return value for x509_name_oneline */
%typemap(ret) char * {
    if ($1 != NULL)
        OPENSSL_free($1);
}
%inline %{
char *x509_name_oneline(X509_NAME *x) {
    return X509_NAME_oneline(x, NULL, 0);
}
%}
%typemap(ret) char *;
