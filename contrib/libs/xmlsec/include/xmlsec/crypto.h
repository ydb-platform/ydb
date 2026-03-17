/*
 * XML Security Library (http://www.aleksey.com/xmlsec).
 *
 * Crypto engine selection.
 *
 * This is free software; see Copyright file in the source
 * distribution for preciese wording.
 *
 * Copyright (C) 2002-2022 Aleksey Sanin <aleksey@aleksey.com>. All Rights Reserved.
 */
#ifndef __XMLSEC_CRYPTO_H__
#define __XMLSEC_CRYPTO_H__

#include <xmlsec/xmlsec.h>

/* include nothing if we compile xmlsec library itself */
#ifndef IN_XMLSEC
#ifndef IN_XMLSEC_CRYPTO

#if defined(XMLSEC_NO_CRYPTO_DYNAMIC_LOADING) && defined(XMLSEC_CRYPTO_DYNAMIC_LOADING)
#error Dynamic loading for xmlsec-crypto libraries is disabled during library compilation
#endif /* defined(XMLSEC_NO_CRYPTO_DYNAMIC_LOADING) && defined(XMLSEC_CRYPTO_DYNAMIC_LOADING) */

#ifdef XMLSEC_CRYPTO_DYNAMIC_LOADING
#include <xmlsec/app.h>
#else /* XMLSEC_CRYPTO_DYNAMIC_LOADING */
#ifdef XMLSEC_CRYPTO_OPENSSL
#include <xmlsec/openssl/app.h>
#include <xmlsec/openssl/crypto.h>
#include <xmlsec/openssl/x509.h>
#include <xmlsec/openssl/symbols.h>
#else /* XMLSEC_CRYPTO_OPENSSL */
#ifdef XMLSEC_CRYPTO_MSCRYPTO
#error #include <xmlsec/mscrypto/app.h>
#error #include <xmlsec/mscrypto/crypto.h>
#error #include <xmlsec/mscrypto/x509.h>
#error #include <xmlsec/mscrypto/symbols.h>
#else /* XMLSEC_CRYPTO_MSCRYPTO */
#ifdef XMLSEC_CRYPTO_MSCNG
#error #include <xmlsec/mscng/app.h>
#error #include <xmlsec/mscng/crypto.h>
#error #include <xmlsec/mscng/x509.h>
#error #include <xmlsec/mscng/symbols.h>
#else /* XMLSEC_CRYPTO_MSCNG */
#ifdef XMLSEC_CRYPTO_NSS
#error #include <xmlsec/nss/app.h>
#error #include <xmlsec/nss/crypto.h>
#error #include <xmlsec/nss/x509.h>
#error #include <xmlsec/nss/symbols.h>
#else /* XMLSEC_CRYPTO_NSS */
#ifdef XMLSEC_CRYPTO_GNUTLS
#error #include <xmlsec/gnutls/app.h>
#error #include <xmlsec/gnutls/crypto.h>
#error #include <xmlsec/gnutls/symbols.h>
#else /* XMLSEC_CRYPTO_GNUTLS */
#ifdef XMLSEC_CRYPTO_GCRYPT
#error #include <xmlsec/gcrypt/app.h>
#error #include <xmlsec/gcrypt/crypto.h>
#error #include <xmlsec/gcrypt/symbols.h>
#else /* XMLSEC_CRYPTO_GCRYPT */
#error No crypto library defined
#endif /* XMLSEC_CRYPTO_GCRYPT */
#endif /* XMLSEC_CRYPTO_GNUTLS */
#endif /* XMLSEC_CRYPTO_NSS */
#endif /* XMLSEC_CRYPTO_MSCNG */
#endif /* XMLSEC_CRYPTO_MSCRYPTO */
#endif /* XMLSEC_CRYPTO_OPENSSL */
#endif /* XMLSEC_CRYPTO_DYNAMIC_LOADING */

#endif /* IN_XMLSEC_CRYPTO */
#endif /* IN_XMLSEC */

#endif /* __XMLSEC_CRYPTO_H__ */

