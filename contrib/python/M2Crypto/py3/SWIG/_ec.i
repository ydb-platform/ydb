/* Copyright (c) 1999-2000 Ng Pheng Siong. All rights reserved.
 Portions copyright (c) 2005-2006 Vrije Universiteit Amsterdam. All rights reserved.

 Most code originally from _dsa.i, _rsa.i and _dh.i and adjusted for EC use.
*/

%include <openssl/opensslconf.h>

#if OPENSSL_VERSION_NUMBER < 0x0090800fL || defined(OPENSSL_NO_EC)
#undef OPENSSL_NO_EC
%constant OPENSSL_NO_EC = 1;
#else
%constant OPENSSL_NO_EC = 0;

%{
#include <openssl/bn.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/ecdsa.h>
#include <openssl/ecdh.h>
%}

%apply Pointer NONNULL { EC_KEY * };

%rename(ec_key_new) EC_KEY_new;
extern EC_KEY *EC_KEY_new(void);
%rename(ec_key_free) EC_KEY_free;
extern void EC_KEY_free(EC_KEY *);
%rename(ec_key_size) ECDSA_size;
extern int ECDSA_size(const EC_KEY *);
%rename(ec_key_gen_key) EC_KEY_generate_key;
extern int EC_KEY_generate_key(EC_KEY *);
%rename(ec_key_check_key) EC_KEY_check_key;
extern int EC_KEY_check_key(const EC_KEY *);

/* Curve identifier constants from OpenSSL */
%constant int NID_secp112r1 = NID_secp112r1;
%constant int NID_secp112r2 = NID_secp112r2;
%constant int NID_secp128r1 = NID_secp128r1;
%constant int NID_secp128r2 = NID_secp128r2;
%constant int NID_secp160k1 = NID_secp160k1;
%constant int NID_secp160r1 = NID_secp160r1;
%constant int NID_secp160r2 = NID_secp160r2;
%constant int NID_secp192k1 = NID_secp192k1;
%constant int NID_secp224k1 = NID_secp224k1;
%constant int NID_secp224r1 = NID_secp224r1;
%constant int NID_secp256k1 = NID_secp256k1;
%constant int NID_secp384r1 = NID_secp384r1;
%constant int NID_secp521r1 = NID_secp521r1;
%constant int NID_sect113r1 = NID_sect113r1;
%constant int NID_sect113r2 = NID_sect113r2;
%constant int NID_sect131r1 = NID_sect131r1;
%constant int NID_sect131r2 = NID_sect131r2;
%constant int NID_sect163k1 = NID_sect163k1;
%constant int NID_sect163r1 = NID_sect163r1;
%constant int NID_sect163r2 = NID_sect163r2;
%constant int NID_sect193r1 = NID_sect193r1;
%constant int NID_sect193r2 = NID_sect193r2;
%constant int NID_sect233k1 = NID_sect233k1;
%constant int NID_sect233r1 = NID_sect233r1;
%constant int NID_sect239k1 = NID_sect239k1;
%constant int NID_sect283k1 = NID_sect283k1;
%constant int NID_sect283r1 = NID_sect283r1;
%constant int NID_sect409k1 = NID_sect409k1;
%constant int NID_sect409r1 = NID_sect409r1;
%constant int NID_sect571k1 = NID_sect571k1;
%constant int NID_sect571r1 = NID_sect571r1;

%constant int NID_X9_62_prime192v1 = NID_X9_62_prime192v1;
%constant int NID_X9_62_prime192v2 = NID_X9_62_prime192v2;
%constant int NID_X9_62_prime192v3 = NID_X9_62_prime192v3;
%constant int NID_X9_62_prime239v1 = NID_X9_62_prime239v1;
%constant int NID_X9_62_prime239v2 = NID_X9_62_prime239v2;
%constant int NID_X9_62_prime239v3 = NID_X9_62_prime239v3;
%constant int NID_X9_62_prime256v1 = NID_X9_62_prime256v1;
%constant int NID_X9_62_c2pnb163v1 = NID_X9_62_c2pnb163v1;
%constant int NID_X9_62_c2pnb163v2 = NID_X9_62_c2pnb163v2;
%constant int NID_X9_62_c2pnb163v3 = NID_X9_62_c2pnb163v3;
%constant int NID_X9_62_c2pnb176v1 = NID_X9_62_c2pnb176v1;
%constant int NID_X9_62_c2tnb191v1 = NID_X9_62_c2tnb191v1;
%constant int NID_X9_62_c2tnb191v2 = NID_X9_62_c2tnb191v2;
%constant int NID_X9_62_c2tnb191v3 = NID_X9_62_c2tnb191v3;
%constant int NID_X9_62_c2pnb208w1 = NID_X9_62_c2pnb208w1;
%constant int NID_X9_62_c2tnb239v1 = NID_X9_62_c2tnb239v1;
%constant int NID_X9_62_c2tnb239v2 = NID_X9_62_c2tnb239v2;
%constant int NID_X9_62_c2tnb239v3 = NID_X9_62_c2tnb239v3;
%constant int NID_X9_62_c2pnb272w1 = NID_X9_62_c2pnb272w1;
%constant int NID_X9_62_c2pnb304w1 = NID_X9_62_c2pnb304w1;
%constant int NID_X9_62_c2tnb359v1 = NID_X9_62_c2tnb359v1;
%constant int NID_X9_62_c2pnb368w1 = NID_X9_62_c2pnb368w1;
%constant int NID_X9_62_c2tnb431r1 = NID_X9_62_c2tnb431r1;

%constant int NID_wap_wsg_idm_ecid_wtls1  = NID_wap_wsg_idm_ecid_wtls1;
%constant int NID_wap_wsg_idm_ecid_wtls3  = NID_wap_wsg_idm_ecid_wtls3;
%constant int NID_wap_wsg_idm_ecid_wtls4  = NID_wap_wsg_idm_ecid_wtls4;
%constant int NID_wap_wsg_idm_ecid_wtls5  = NID_wap_wsg_idm_ecid_wtls5;
%constant int NID_wap_wsg_idm_ecid_wtls6  = NID_wap_wsg_idm_ecid_wtls6;
%constant int NID_wap_wsg_idm_ecid_wtls7  = NID_wap_wsg_idm_ecid_wtls7;
%constant int NID_wap_wsg_idm_ecid_wtls8  = NID_wap_wsg_idm_ecid_wtls8;
%constant int NID_wap_wsg_idm_ecid_wtls9  = NID_wap_wsg_idm_ecid_wtls9;
%constant int NID_wap_wsg_idm_ecid_wtls10 = NID_wap_wsg_idm_ecid_wtls10;
%constant int NID_wap_wsg_idm_ecid_wtls11 = NID_wap_wsg_idm_ecid_wtls11;
%constant int NID_wap_wsg_idm_ecid_wtls12 = NID_wap_wsg_idm_ecid_wtls12;

%constant int NID_ipsec3 = NID_ipsec3;
%constant int NID_ipsec4 = NID_ipsec4;


%warnfilter(454) _ec_err;
%inline %{
static PyObject *_ec_err;

void ec_init(PyObject *ec_err) {
    Py_INCREF(ec_err);
    _ec_err = ec_err;
}

PyObject *ec_get_builtin_curves(void) {
    /*  size_t EC_get_builtin_curves(EC_builtin_curve *r, size_t
     *  nitems); */
  EC_builtin_curve *curves;
  Py_ssize_t ret_curves = 0;
  size_t num_curves =  EC_get_builtin_curves(NULL, 0);
  PyObject *ret_tuple = NULL;
  PyObject *ret_dict = NULL;
  Py_ssize_t i;
  const char *comment;
  const char *sname;

  if (!(curves = PyMem_Malloc(num_curves * sizeof(EC_builtin_curve)))) {
      PyErr_SetString(PyExc_MemoryError, "ec_get_builtin_curves");
      return NULL;
  }

  ret_curves = (Py_ssize_t)EC_get_builtin_curves(curves, num_curves);

  if (!(ret_tuple = PyTuple_New(ret_curves))) {
      PyErr_SetString(PyExc_MemoryError, "ec_get_builtin_curves");
      return NULL;
  }

  for (i = 0; i < ret_curves; i++) {
    if (!(ret_dict = PyDict_New())) {
        PyErr_SetString(PyExc_MemoryError, "ec_get_builtin_curves");
        return NULL;
    }

    comment = curves[i].comment;
    sname = OBJ_nid2sn(curves[i].nid);
    if (sname == NULL)
        sname = "";

    PyDict_SetItemString(ret_dict, "NID",
                         PyLong_FromLong((long)curves[i].nid));
    PyDict_SetItemString(ret_dict, "sname",
                         PyString_FromString(sname));
    PyDict_SetItemString(ret_dict, "comment",
                         PyString_FromString(comment));

    PyTuple_SET_ITEM(ret_tuple, i, ret_dict);

  }

  PyMem_Free(curves);

  return ret_tuple;
}

EC_KEY* ec_key_new_by_curve_name(int nid)
{
    EC_KEY   *key;
    EC_GROUP *group;
    int ret  =0;
    point_conversion_form_t form = POINT_CONVERSION_UNCOMPRESSED;
    int      asn1_flag = OPENSSL_EC_NAMED_CURVE;

    /* If I simply do "return EC_KEY_new_by_curve_name(nid);"
     * I get large public keys (222 vs 84 bytes for sect233k1 curve).
     * I don't know why that is, but 'openssl ecparam -genkey ...' sets
     * the ASN.1 flag and the point conversion form, and gets the
     * small pub keys. So let's do that too.
     */
    key = EC_KEY_new();
    if (!key) {
        PyErr_SetString(PyExc_MemoryError, "ec_key_new_by_curve_name");
        return NULL;
    }
    group = EC_GROUP_new_by_curve_name(nid);
    if (!group) {
        m2_PyErr_Msg(_ec_err);
        EC_KEY_free(key);
        return NULL;
    }
    EC_GROUP_set_asn1_flag(group, asn1_flag);
    EC_GROUP_set_point_conversion_form(group, form);
    ret = EC_KEY_set_group(key, group);
    EC_GROUP_free(group);
    if (ret == 0)
    {
        /* EC_KEY_set_group only returns 0 or 1, and does not set error. */
        PyErr_SetString(_ec_err, "cannot set key's group");
        EC_KEY_free(key);
        return NULL;
    }

    return key;
}

PyObject *ec_key_get_public_der(EC_KEY *key) {
    char *src=NULL;
    int src_len=0;
    PyObject *pyo=NULL;

    /* Convert to binary */
    src_len = i2d_EC_PUBKEY( key, (unsigned char**)&src );
    if (src_len < 0)
    {
        m2_PyErr_Msg(_ec_err);
        return NULL;
    }
    /* Create a PyBuffer containing a copy of the binary,
     * to simplify memory deallocation
     */
    pyo = PyBytes_FromStringAndSize( src, src_len );

    OPENSSL_free(src);

    return pyo;
}

PyObject *ec_key_get_public_key(EC_KEY *key) {
    char *src=NULL;
    int src_len=0;
    PyObject *pyo=NULL;

    /* Convert to binary */
    src_len = i2o_ECPublicKey(key, (unsigned char**)&src);
    if (src_len < 0)
    {
        m2_PyErr_Msg(_ec_err);
        return NULL;
    }

    pyo = PyBytes_FromStringAndSize( src, src_len );

    OPENSSL_free(src);

    return pyo;
}

%}

%threadallow ec_key_read_pubkey;
%inline %{
EC_KEY *ec_key_read_pubkey(BIO *f) {
    return PEM_read_bio_EC_PUBKEY(f, NULL, NULL, NULL);
}
%}

%threadallow ec_key_write_pubkey;
%inline %{
int ec_key_write_pubkey(EC_KEY *key, BIO *f) {
    return PEM_write_bio_EC_PUBKEY(f, key );
}
%}

%inline %{
EC_KEY *ec_key_read_bio(BIO *f, PyObject *pyfunc) {
    EC_KEY *ret;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    ret = PEM_read_bio_ECPrivateKey(f, NULL, passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);
    return ret;
}
%}

%inline %{
int ec_key_write_bio(EC_KEY *key, BIO *f, EVP_CIPHER *cipher, PyObject *pyfunc) {
    int ret;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    ret = PEM_write_bio_ECPrivateKey(f, key, cipher, NULL, 0,
        passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);
    return ret;
}
%}

%inline %{
int ec_key_write_bio_no_cipher(EC_KEY *key, BIO *f, PyObject *pyfunc) {
    int ret;

    Py_INCREF(pyfunc);
    Py_BEGIN_ALLOW_THREADS
    ret = PEM_write_bio_ECPrivateKey(f, key, NULL, NULL, 0,
                      passphrase_callback, (void *)pyfunc);
    Py_END_ALLOW_THREADS
    Py_DECREF(pyfunc);
    return ret;
}


PyObject *ecdsa_sig_get_r(ECDSA_SIG *ecdsa_sig) {
    const BIGNUM* pr;
    ECDSA_SIG_get0(ecdsa_sig, &pr, NULL);
    return bn_to_mpi(pr);
}

PyObject *ecdsa_sig_get_s(ECDSA_SIG *ecdsa_sig) {
    const BIGNUM* ps;
    ECDSA_SIG_get0(ecdsa_sig, NULL, &ps);
    return bn_to_mpi(ps);
}

PyObject *ecdsa_sign(EC_KEY *key, PyObject *value) {
    const void *vbuf;
    int vlen = 0;
    PyObject *tuple;
    ECDSA_SIG *sig;

    if (m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        return NULL;

    if (!(sig = ECDSA_do_sign(vbuf, vlen, key))) {
        m2_PyErr_Msg(_ec_err);
        return NULL;
    }
    if (!(tuple = PyTuple_New(2))) {
        ECDSA_SIG_free(sig);
        PyErr_SetString(PyExc_RuntimeError, "PyTuple_New() fails");
        return NULL;
    }
    PyTuple_SET_ITEM(tuple, 0, ecdsa_sig_get_r(sig));
    PyTuple_SET_ITEM(tuple, 1, ecdsa_sig_get_s(sig));
    ECDSA_SIG_free(sig);
    return tuple;
}

int ecdsa_verify(EC_KEY *key, PyObject *value, PyObject *r, PyObject *s) {
    const void *vbuf, *rbuf, *sbuf;
    int vlen = 0, rlen = 0, slen = 0;
    ECDSA_SIG *sig;
    int ret;
    BIGNUM* pr, *ps;

    if ((m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        || (m2_PyObject_AsReadBufferInt(r, &rbuf, &rlen) == -1)
        || (m2_PyObject_AsReadBufferInt(s, &sbuf, &slen) == -1))
        return -1;

    if (!(pr = BN_mpi2bn((unsigned char *)rbuf, rlen, NULL))) {
        m2_PyErr_Msg(_ec_err);
        return -1;
    }
    if (!(ps = BN_mpi2bn((unsigned char *)sbuf, slen, NULL))) {
        m2_PyErr_Msg(_ec_err);
        BN_free(pr);
        return -1;
    }

    if (!(sig = ECDSA_SIG_new())) {
        m2_PyErr_Msg(_ec_err);
        BN_free(pr);
        BN_free(ps);
        return -1;
    }
    if (!ECDSA_SIG_set0(sig, pr, ps)) {
        PyErr_SetString(_ec_err, "Cannot set r and s fields of ECDSA_SIG.");
        ECDSA_SIG_free(sig);
        BN_free(pr);
        BN_free(ps);
        return -1;
    }
    ret = ECDSA_do_verify(vbuf, vlen, sig, key);
    ECDSA_SIG_free(sig);
    if (ret == -1)
        m2_PyErr_Msg(_ec_err);
    return ret;
}


PyObject *ecdsa_sign_asn1(EC_KEY *key, PyObject *value) {
    const void *vbuf;
    int vlen = 0;
    void *sigbuf;
    unsigned int siglen;
    PyObject *ret;

    if (m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        return NULL;

    if (!(sigbuf = PyMem_Malloc(ECDSA_size(key)))) {
        PyErr_SetString(PyExc_MemoryError, "ecdsa_sign_asn1");
        return NULL;
    }
    if (!ECDSA_sign(0, vbuf, vlen, (unsigned char *)sigbuf, &siglen, key)) {
        m2_PyErr_Msg(_ec_err);
        PyMem_Free(sigbuf);
        return NULL;
    }
    ret = PyBytes_FromStringAndSize(sigbuf, siglen);

    PyMem_Free(sigbuf);
    return ret;
}


int ecdsa_verify_asn1(EC_KEY *key, PyObject *value, PyObject *sig) {
    const void *vbuf;
    void *sbuf;
    int vlen = 0, slen = 0, ret;

    if ((m2_PyObject_AsReadBufferInt(value, &vbuf, &vlen) == -1)
        || (m2_PyObject_AsReadBufferInt(sig, (const void **)&sbuf, &slen)
        == -1))
        return -1;

    if ((ret = ECDSA_verify(0, vbuf, vlen, sbuf, slen, key)) == -1)
        m2_PyErr_Msg(_ec_err);
    return ret;
}

PyObject *ecdh_compute_key(EC_KEY *keypairA, EC_KEY *pubkeyB) {
    int sharedkeylen;
    void *sharedkey;
    const EC_POINT *pkpointB;
    PyObject *ret;
    const EC_GROUP* groupA;

    if ((pkpointB = EC_KEY_get0_public_key(pubkeyB)) == NULL)
    {
        PyErr_SetString(_ec_err, "Cannot get the public key of EC_KEY object.");
        return NULL;
    }

    groupA = EC_KEY_get0_group(keypairA);
    sharedkeylen = (EC_GROUP_get_degree(groupA) + 7)/8;

    if (!(sharedkey = PyMem_Malloc(sharedkeylen))) {
        PyErr_SetString(PyExc_MemoryError, "ecdh_compute_key");
        return NULL;
    }
    if ((sharedkeylen = ECDH_compute_key((unsigned char *)sharedkey, sharedkeylen, pkpointB, keypairA, NULL)) == -1) {
        m2_PyErr_Msg(_ec_err);
        PyMem_Free(sharedkey);
        return NULL;
    }

    ret = PyBytes_FromStringAndSize((const char *)sharedkey, sharedkeylen);

    PyMem_Free(sharedkey);

    return ret;
}


EC_KEY* ec_key_from_pubkey_der(PyObject *pubkey) {
    const void *keypairbuf;
    Py_ssize_t keypairbuflen;
    const unsigned char *tempBuf;
    EC_KEY *keypair;

    if (m2_PyObject_AsReadBuffer(pubkey, &keypairbuf, &keypairbuflen) == -1)
    {
        return NULL;
    }

    tempBuf = (const unsigned char *)keypairbuf;
    if ((keypair = d2i_EC_PUBKEY( NULL, &tempBuf, keypairbuflen)) == 0)
    {
        m2_PyErr_Msg(_ec_err);
        return NULL;
    }
    return keypair;
}

EC_KEY* ec_key_from_pubkey_params(int nid, PyObject *pubkey) {
    const void *keypairbuf;
    Py_ssize_t keypairbuflen;
    const unsigned char *tempBuf;
    EC_KEY *keypair;

    if (m2_PyObject_AsReadBuffer(pubkey, &keypairbuf, &keypairbuflen) == -1)
    {
        return NULL;
    }

    keypair = ec_key_new_by_curve_name(nid);
    if (!keypair) {
        m2_PyErr_Msg(_ec_err);
        return NULL;
    }

    tempBuf = (const unsigned char *)keypairbuf;
    if ((o2i_ECPublicKey( &keypair, &tempBuf, keypairbuflen)) == 0)
    {
        m2_PyErr_Msg(_ec_err);
        return NULL;
    }
    return keypair;
}


// According to [SEC2] the degree of the group is defined as EC key length
int ec_key_keylen(EC_KEY *key) {
    const EC_GROUP *group = EC_KEY_get0_group(key);
    return EC_GROUP_get_degree(group);
}

int ec_key_type_check(EC_KEY *key) {
    return 1;
}
%}
#endif // if OpenSSL version with EC support

