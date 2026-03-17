/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* Copyright (c) 1999-2002 Ng Pheng Siong. All rights reserved.
 *
 * Portions created by Open Source Applications Foundation (OSAF) are
 * Copyright (C) 2004-2006 OSAF. All Rights Reserved.
 *
 * Copyright (c) 2009-2010 Heikki Toivonen. All rights reserved.
 *
 */

%module(threads=1) m2crypto
/* We really don't need threadblock (PyGILState_Ensure() etc.) anywhere.
   Disable threadallow as well, only enable it for operations likely to
   block. */
%nothreadblock;
%nothreadallow;

#if SWIG_VERSION >= 0x030000
#define __WCHAR_MAX__ __WCHAR_MAX
#define __WCHAR_MIN__ __WCHAR_MIN
#endif
/* https://todo.sr.ht/~mcepl/m2crypto/246 */
%ignore WCHAR_MAX;
%ignore WCHAR_MIN;
/* http://swig.10945.n7.nabble.com/SWIG-AsVal-wchar-t-error-td2264.html */
%{
int SWIG_AsVal_wchar_t(PyObject *p, wchar_t *c) { return SWIG_OK; }
PyObject *SWIG_From_wchar_t(wchar_t c) { return SWIG_Py_Void(); }
%}

%{
#ifdef _WIN32
#define _WINSOCKAPI_
#include <WinSock2.h>
#include <Windows.h>
#pragma comment(lib, "Ws2_32")
typedef unsigned __int64 uint64_t;
#endif
%}

%{
#if defined __GNUC__ && __GNUC__ < 5
#pragma GCC diagnostic ignored "-Wunused-label"
#pragma GCC diagnostic warning "-Wstrict-prototypes"
#endif

#include <openssl/err.h>
#include <openssl/rand.h>
#include <_lib.h>
#include <libcrypto-compat.h>
#include <py3k_compat.h>

#include "compile.h"

static PyObject *ssl_verify_cb_func;
static PyObject *ssl_info_cb_func;
static PyObject *ssl_set_tmp_dh_cb_func;
static PyObject *ssl_set_tmp_rsa_cb_func;
static PyObject *x509_store_verify_cb_func;
%}

%include <openssl/opensslv.h>

/* Bring in STACK_OF macro definition */
#ifdef _WIN32
%include <windows.i>
#endif
%include <openssl/safestack.h>

/* Bring in LHASH_OF macro definition */
/* XXX Can't include lhash.h where LHASH_OF is defined, because it includes
   XXX stdio.h etc. which we fail to include. So we have to (re)define
   XXX LHASH_OF here instead.
%include <openssl/lhash.h>
*/
#if OPENSSL_VERSION_NUMBER >= 0x10000000L
#define LHASH_OF(type) struct lhash_st_##type
#endif


%include constraints.i
%include _threads.i
%include _lib.i
%include _bio.i
%include _bn.i
%include _rand.i
%include _evp.i
%include _aes.i
%include _rc4.i
%include _dh.i
%include _rsa.i
%include _dsa.i
%include _ssl.i
%include _x509.i
%include _asn1.i
%include _pkcs7.i
%include _util.i
%include _ec.i
%include _objects.i

#ifdef SWIG_VERSION
%constant int encrypt = 1;
%constant int decrypt = 0;
#endif

