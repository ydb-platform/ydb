/* Copyright (c) 1999 Ng Pheng Siong. All rights reserved. */
/* $Id$ */

%{
#include <openssl/bn.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/dh.h>
%}

%apply Pointer NONNULL { DH * };

%rename(dh_new) DH_new;
extern DH *DH_new(void);
%rename(dh_free) DH_free;
extern void DH_free(DH *);
%rename(dh_size) DH_size;
extern int DH_size(const DH *);
%rename(dh_generate_key) DH_generate_key;
extern int DH_generate_key(DH *);
%rename(dhparams_print) DHparams_print;
%threadallow DHparams_print;
extern int DHparams_print(BIO *, const DH *);

%constant int dh_check_ok             = 0;
%constant int dh_check_p_not_prime    = DH_CHECK_P_NOT_PRIME;
%constant int dh_check_p_not_strong   = DH_CHECK_P_NOT_STRONG_PRIME;
%constant int dh_check_g_failed       = DH_UNABLE_TO_CHECK_GENERATOR;
%constant int dh_check_bad_g          = DH_NOT_SUITABLE_GENERATOR;

%constant DH_GENERATOR_2          = 2;
%constant DH_GENERATOR_5          = 5;

%warnfilter(454) _dh_err;
%inline %{
static PyObject *_dh_err;

void dh_init(PyObject *dh_err) {
    Py_INCREF(dh_err);
    _dh_err = dh_err;
}

int dh_type_check(DH *dh) {
    /* Our getting here means we passed Swig's type checking,
    XXX Still need to check the pointer for sanity? */
    return 1;
}
%}

%threadallow dh_read_parameters;
%inline %{
DH *dh_read_parameters(BIO *bio) {
    return PEM_read_bio_DHparams(bio, NULL, NULL, NULL);
}

DH *dh_generate_parameters(int plen, int g, PyObject *pyfunc) {
    DH *dh;
    BN_GENCB *gencb;
    int ret;

    if ((gencb=BN_GENCB_new()) == NULL) {
        m2_PyErr_Msg(_dh_err);
        return NULL;
    }

    if ((dh=DH_new()) == NULL) {
        m2_PyErr_Msg(_dh_err);
        BN_GENCB_free(gencb);
        return NULL;
    }

    BN_GENCB_set(gencb, bn_gencb_callback, (void *)pyfunc);

    Py_INCREF(pyfunc);
    ret = DH_generate_parameters_ex(dh, plen, g, gencb);
    Py_DECREF(pyfunc);
    BN_GENCB_free(gencb);

    if (ret)
        return dh;

    m2_PyErr_Msg(_dh_err);
    DH_free(dh);
    return NULL;
}

/* Note return value shenanigan. */
int dh_check(DH *dh) {
    int err;

    return (DH_check(dh, &err)) ? 0 : err;
}

PyObject *dh_compute_key(DH *dh, PyObject *pubkey) {
    const void *pkbuf;
    int pklen = 0, klen;
    void *key;
    BIGNUM *pk;
    PyObject *ret;

    if (m2_PyObject_AsReadBufferInt(pubkey, &pkbuf, &pklen) == -1)
        return NULL;

    if (!(pk = BN_mpi2bn((unsigned char *)pkbuf, pklen, NULL))) {
        m2_PyErr_Msg(_dh_err);
        return NULL;
    }
    if (!(key = PyMem_Malloc(DH_size(dh)))) {
        BN_free(pk);
        PyErr_SetString(PyExc_MemoryError, "dh_compute_key");
        return NULL;
    }
    if ((klen = DH_compute_key((unsigned char *)key, pk, dh)) == -1) {
        BN_free(pk);
        PyMem_Free(key);
        m2_PyErr_Msg(_dh_err);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize((const char *)key, klen);

    BN_free(pk);
    PyMem_Free(key);
    return ret;
}

PyObject *dh_get_p(DH *dh) {
    const BIGNUM* p = NULL;
    DH_get0_pqg(dh, &p, NULL, NULL);
    if (!p) {
        PyErr_SetString(_dh_err, "'p' is unset");
        return NULL;
    }
    return bn_to_mpi(p);
}

PyObject *dh_get_g(DH *dh) {
    const BIGNUM* g = NULL;
    DH_get0_pqg(dh, NULL, NULL, &g);
    if (!g) {
        PyErr_SetString(_dh_err, "'g' is unset");
        return NULL;
    }
    return bn_to_mpi(g);
}

PyObject *dh_get_pub(DH *dh) {
    const BIGNUM* pub_key = NULL;
    DH_get0_key(dh, &pub_key, NULL);
    if (!pub_key) {
        PyErr_SetString(_dh_err, "'pub' is unset");
        return NULL;
    }
    return bn_to_mpi(pub_key);
}

PyObject *dh_get_priv(DH *dh) {
    const BIGNUM* priv_key = NULL;
    DH_get0_key(dh, NULL, &priv_key);
    if (!priv_key) {
        PyErr_SetString(_dh_err, "'priv' is unset");
        return NULL;
    }
    return bn_to_mpi(priv_key);
}

PyObject *dh_set_pg(DH *dh, PyObject *pval, PyObject* gval) {
    BIGNUM* p, *g;

    if (!(p = m2_PyObject_AsBIGNUM(pval, _dh_err))
        || !(g = m2_PyObject_AsBIGNUM(gval, _dh_err)))
        return NULL;

    if (!DH_set0_pqg(dh, p, NULL, g)) {
        PyErr_SetString(_dh_err,
            "Cannot set prime number or generator of Z_p for DH.");
        BN_free(p);
        BN_free(g);
        return NULL;
        }

    Py_RETURN_NONE;
}
%}

