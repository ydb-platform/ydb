/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 1999-2004 Ng Pheng Siong. All rights reserved.  */
/*
** Portions created by Open Source Applications Foundation (OSAF) are
** Copyright (C) 2004 OSAF. All Rights Reserved.
*/
/* $Id$ */

%{
#include <openssl/asn1.h>
#include <time.h>
%}

%apply Pointer NONNULL { BIO * };
%apply Pointer NONNULL { ASN1_OBJECT * };
%apply Pointer NONNULL { ASN1_STRING * };
%apply Pointer NONNULL { ASN1_INTEGER * };
%apply Pointer NONNULL { ASN1_TIME * };

%rename(asn1_object_new) ASN1_OBJECT_new;
extern ASN1_OBJECT *ASN1_OBJECT_new( void );
%rename(asn1_object_create) ASN1_OBJECT_create;
extern ASN1_OBJECT *ASN1_OBJECT_create( int, unsigned char *, int, const char *, const char *);
%rename(asn1_object_free) ASN1_OBJECT_free;
extern void ASN1_OBJECT_free( ASN1_OBJECT *);
%rename(i2d_asn1_object) i2d_ASN1_OBJECT;
extern int i2d_ASN1_OBJECT( ASN1_OBJECT *, unsigned char **);
%rename(d2i_asn1_object) d2i_ASN1_OBJECT;
extern ASN1_OBJECT *d2i_ASN1_OBJECT( ASN1_OBJECT **, const unsigned char **, long);

%rename(asn1_bit_string_new) ASN1_BIT_STRING_new;
extern ASN1_BIT_STRING *ASN1_BIT_STRING_new( void );

%rename(asn1_string_new) ASN1_STRING_new;
extern ASN1_STRING *ASN1_STRING_new( void );
%rename(asn1_string_free) ASN1_STRING_free;
extern void ASN1_STRING_free( ASN1_STRING *);

%typemap(in) (const void *, int) {
    if (PyBytes_Check($input)) {
        Py_ssize_t len;

        $1 = PyBytes_AsString($input);
        len = PyBytes_Size($input);

        if (len > INT_MAX) {
            PyErr_SetString(PyExc_ValueError, "object too large");
            return NULL;
        }
        $2 = len;
    }
    else {
        PyErr_SetString(PyExc_TypeError, "expected string");
        return NULL;
    }
}

%rename(asn1_string_set) ASN1_STRING_set;
extern int ASN1_STRING_set( ASN1_STRING *, const void *, int);

%typemap(in) (const void *, int);

%rename(asn1_string_print) ASN1_STRING_print;
%threadallow ASN1_STRING_print;
extern int ASN1_STRING_print(BIO *, ASN1_STRING *);
%threadallow ASN1_STRING_print_ex;
%rename(asn1_string_print_ex) ASN1_STRING_print_ex;
extern int ASN1_STRING_print_ex(BIO *, ASN1_STRING *, unsigned long);

%rename(asn1_time_new) ASN1_TIME_new;
extern ASN1_TIME *ASN1_TIME_new( void );
%rename(asn1_time_free) ASN1_TIME_free;
extern void ASN1_TIME_free(ASN1_TIME *);
%rename(asn1_time_check) ASN1_TIME_check;
extern int ASN1_TIME_check(ASN1_TIME *);
%rename(asn1_time_set_string) ASN1_TIME_set_string;
extern int ASN1_TIME_set_string(ASN1_TIME *, const char *);
%rename(asn1_time_print) ASN1_TIME_print;
%threadallow ASN1_TIME_print;
extern int ASN1_TIME_print(BIO *, ASN1_TIME *);

%rename(asn1_integer_new) ASN1_INTEGER_new;
extern ASN1_INTEGER *ASN1_INTEGER_new( void );
%rename(asn1_integer_free) ASN1_INTEGER_free;
extern void ASN1_INTEGER_free( ASN1_INTEGER *);
%rename(asn1_integer_cmp) ASN1_INTEGER_cmp;
extern int ASN1_INTEGER_cmp(ASN1_INTEGER *, ASN1_INTEGER *);

%constant int ASN1_STRFLGS_ESC_2253 = 1;
%constant int ASN1_STRFLGS_ESC_CTRL = 2;
%constant int ASN1_STRFLGS_ESC_MSB = 4;
%constant int ASN1_STRFLGS_ESC_QUOTE = 8;
%constant int ASN1_STRFLGS_UTF8_CONVERT = 0x10;
%constant int ASN1_STRFLGS_IGNORE_TYPE = 0x20;
%constant int ASN1_STRFLGS_SHOW_TYPE = 0x40;
%constant int ASN1_STRFLGS_DUMP_ALL = 0x80;
%constant int ASN1_STRFLGS_DUMP_UNKNOWN = 0x100;
%constant int ASN1_STRFLGS_DUMP_DER = 0x200;

%constant int ASN1_STRFLGS_RFC2253 = (ASN1_STRFLGS_ESC_2253 | \
                ASN1_STRFLGS_ESC_CTRL | \
                ASN1_STRFLGS_ESC_MSB | \
                ASN1_STRFLGS_UTF8_CONVERT | \
                ASN1_STRFLGS_DUMP_UNKNOWN | \
                ASN1_STRFLGS_DUMP_DER);

%inline %{
/* ASN1_TIME_set_string () is a macro */
int asn1_time_type_check(ASN1_TIME *ASN1_TIME) {
    return 1;
}

ASN1_TIME* asn1_time_set(ASN1_TIME *asn1_time, PyObject *time) {
    long long val;
    if ((val = PyLong_AsLongLong(time)) >= 0) {
        ERR_clear_error();
        asn1_time = ASN1_TIME_adj(asn1_time, (time_t)0,
                                  (int)(val / 86400LL),
                                  (long) (val % 86400LL));
        return asn1_time;
    } else {
        char *errstr = PyMem_Malloc(256);
        snprintf(errstr, 256, "Error in conversion of PyLong to long (val = %lld)", val);
        PyErr_SetString(PyExc_OverflowError, errstr);
        PyMem_Free(errstr);
        return NULL;
    }
}

PyObject *asn1_integer_get(ASN1_INTEGER *asn1) {
    BIGNUM *bn;
    PyObject *ret;
    char *hex;

    bn = ASN1_INTEGER_to_BN(asn1, NULL);

    if (!bn){
        m2_PyErr_Msg(PyExc_RuntimeError);
        return NULL;
    }

    hex = BN_bn2hex(bn);

    if (!hex){
        m2_PyErr_Msg(PyExc_RuntimeError);
        BN_free(bn);
        return NULL;
    }

    BN_free(bn);

    ret = PyLong_FromString(hex, NULL, 16);

    OPENSSL_free(hex);

    return ret;
}

int asn1_integer_set(ASN1_INTEGER *asn1, PyObject *value) {
    BIGNUM *bn = NULL;
    PyObject *fmt, *args, *hex;

/* Despite all hopes to the contrary, we cannot survive here with
 * PyLong_AsLong shims as provided in
 * /usr/include/python2.7/longobject.h.
 */
    long val = PyLong_AsLong(value);
    if (val >= 0) {
        return ASN1_INTEGER_set(asn1, val);
    } else {
        PyErr_Clear();
    }

    if (!PyLong_Check(value)){
        PyErr_SetString(PyExc_TypeError, "expected int or long");
        return 0;
    }

    fmt = PyUnicode_FromString("%x");

    if (!fmt)
        return 0;

    args = PyTuple_New(1);

    if (!args){
        Py_DECREF(fmt);
        PyErr_SetString(PyExc_RuntimeError, "PyTuple_New() failed");
        return 0;
    }

    Py_INCREF(value);
    PyTuple_SET_ITEM(args, 0, value);
    hex = PyUnicode_Format(fmt, args);

    if (!hex){
        PyErr_SetString(PyExc_RuntimeError, "PyString_Format() failed");
        Py_DECREF(fmt);
        Py_DECREF(args);
        return 0;
    }

    Py_DECREF(fmt);
    Py_DECREF(args);

    if (BN_hex2bn(&bn, PyUnicode_AsUTF8(hex)) <= 0){
        m2_PyErr_Msg(PyExc_RuntimeError);
        Py_DECREF(hex);
        return 0;
    }

    Py_DECREF(hex);

    if (!BN_to_ASN1_INTEGER(bn, asn1)){
        m2_PyErr_Msg(PyExc_RuntimeError);
        BN_free(bn);
        return 0;
    }

    BN_free(bn);

    return 1;
}

%}
