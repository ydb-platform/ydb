/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2001, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * Some parts of this code originally written by Adam Stubblefield,
 * -- astubble@rice.edu.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/hmac.h"

#ifdef HAVE_CRYPTO_IPP
#include <ippcp.h>
#endif

static void __aes_err __P((ENV *, int));
static int __aes_derivekeys __P((ENV *, DB_CIPHER *, u_int8_t *, size_t));

/*
 * __aes_setup --
 *	Setup AES functions.
 *
 * PUBLIC: int __aes_setup __P((ENV *, DB_CIPHER *));
 */
int
__aes_setup(env, db_cipher)
	ENV *env;
	DB_CIPHER *db_cipher;
{
	AES_CIPHER *aes_cipher;
	int ret;
#ifdef	HAVE_CRYPTO_IPP
	int ctx_size = 0;
	IppStatus ipp_ret;
#endif

	db_cipher->adj_size = __aes_adj_size;
	db_cipher->close = __aes_close;
	db_cipher->decrypt = __aes_decrypt;
	db_cipher->encrypt = __aes_encrypt;
	db_cipher->init = __aes_init;
	if ((ret = __os_calloc(env, 1, sizeof(AES_CIPHER), &aes_cipher)) != 0)
		return (ret);
#ifdef	HAVE_CRYPTO_IPP
	/*
	 * IPP AES encryption context size can only be obtained through this
	 * function call, cannot directly declare IppsRijndael128Spec within
	 * AES_CIPHER struct.
	 */
	if ((ipp_ret = ippsRijndael128GetSize(&ctx_size)) != ippStsNoErr) {
		__aes_err(env, (int)ipp_ret);
		return (EAGAIN);
	}
	if ((ret = __os_malloc(env, ctx_size, &aes_cipher->ipp_ctx)) != 0) {
		__os_free(env, aes_cipher);
		return (ret);
	}
#endif
	db_cipher->data = aes_cipher;
	return (0);
}

/*
 * __aes_adj_size --
 *	Given a size, return an addition amount needed to meet the
 *	"chunk" needs of the algorithm.
 *
 * PUBLIC: u_int __aes_adj_size __P((size_t));
 */
u_int
__aes_adj_size(len)
	size_t len;
{
	if (len % DB_AES_CHUNK == 0)
		return (0);
	return (DB_AES_CHUNK - (u_int)(len % DB_AES_CHUNK));
}

/*
 * __aes_close --
 *	Destroy the AES encryption instantiation.
 *
 * PUBLIC: int __aes_close __P((ENV *, void *));
 */
int
__aes_close(env, data)
	ENV *env;
	void *data;
{
#ifdef	HAVE_CRYPTO_IPP
	AES_CIPHER *aes_cipher = (AES_CIPHER *)data;
	__os_free(env, aes_cipher->ipp_ctx);
#endif
	__os_free(env, data);
	return (0);
}

/*
 * __aes_decrypt --
 *	Decrypt data with AES.
 *
 * PUBLIC: int __aes_decrypt __P((ENV *, void *, void *,
 * PUBLIC:     u_int8_t *, size_t));
 */
int
__aes_decrypt(env, aes_data, iv, cipher, cipher_len)
	ENV *env;
	void *aes_data;
	void *iv;
	u_int8_t *cipher;
	size_t cipher_len;
{
	AES_CIPHER *aes;
#ifdef	HAVE_CRYPTO_IPP
	IppStatus ipp_ret;
#else
	cipherInstance c;
#endif
	int ret;

	aes = (AES_CIPHER *)aes_data;
	if (iv == NULL || cipher == NULL)
		return (EINVAL);
	if ((cipher_len % DB_AES_CHUNK) != 0)
		return (EINVAL);

#ifdef	HAVE_CRYPTO_IPP
	if ((ipp_ret = ippsRijndael128DecryptCBC((const Ipp8u *)cipher,
	    (Ipp8u *)cipher, cipher_len, (IppsRijndael128Spec *)aes->ipp_ctx,
	    (const Ipp8u *)iv, 0)) != ippStsNoErr) {
		__aes_err(env, (int)ipp_ret);
		return (EAGAIN);
	}
#else
	/*
	 * Initialize the cipher
	 */
	if ((ret = __db_cipherInit(&c, MODE_CBC, iv)) < 0) {
		__aes_err(env, ret);
		return (EAGAIN);
	}

	/* Do the decryption */
	if ((ret = __db_blockDecrypt(&c, &aes->decrypt_ki, cipher,
	    cipher_len * 8, cipher)) < 0) {
		__aes_err(env, ret);
		return (EAGAIN);
	}
#endif
	return (0);
}

/*
 * __aes_encrypt --
 *	Encrypt data with AES.
 *
 * PUBLIC: int __aes_encrypt __P((ENV *, void *, void *,
 * PUBLIC:     u_int8_t *, size_t));
 */
int
__aes_encrypt(env, aes_data, iv, data, data_len)
	ENV *env;
	void *aes_data;
	void *iv;
	u_int8_t *data;
	size_t data_len;
{
	AES_CIPHER *aes;
#ifdef	HAVE_CRYPTO_IPP
	IppStatus ipp_ret;
#else
	cipherInstance c;
#endif
	u_int32_t tmp_iv[DB_IV_BYTES/4];
	int ret;

	aes = (AES_CIPHER *)aes_data;
	if (aes == NULL || data == NULL)
		return (EINVAL);
	if ((data_len % DB_AES_CHUNK) != 0)
		return (EINVAL);
	/*
	 * Generate the IV here.  We store it in a tmp IV because
	 * the IV might be stored within the data we are encrypting
	 * and so we will copy it over to the given location after
	 * encryption is done.
	 * We don't do this outside of there because some encryption
	 * algorithms someone might add may not use IV's and we always
	 * want on here.
	 */
	if ((ret = __db_generate_iv(env, tmp_iv)) != 0)
		return (ret);

#ifdef	HAVE_CRYPTO_IPP
	if ((ipp_ret = ippsRijndael128EncryptCBC((const Ipp8u *)data,
	    (Ipp8u *)data, data_len, (IppsRijndael128Spec *)aes->ipp_ctx,
	    (const Ipp8u *)tmp_iv, 0)) != ippStsNoErr) {
		__aes_err(env, (int)ipp_ret);
		return (EAGAIN);
	}
#else
	/*
	 * Initialize the cipher
	 */
	if ((ret = __db_cipherInit(&c, MODE_CBC, (char *)tmp_iv)) < 0) {
		__aes_err(env, ret);
		return (EAGAIN);
	}

	/* Do the encryption */
	if ((ret = __db_blockEncrypt(&c, &aes->encrypt_ki, data, data_len * 8,
	    data)) < 0) {
		__aes_err(env, ret);
		return (EAGAIN);
	}
#endif
	memcpy(iv, tmp_iv, DB_IV_BYTES);
	return (0);
}

/*
 * __aes_init --
 *	Initialize the AES encryption instantiation.
 *
 * PUBLIC: int __aes_init __P((ENV *, DB_CIPHER *));
 */
int
__aes_init(env, db_cipher)
	ENV *env;
	DB_CIPHER *db_cipher;
{
	DB_ENV *dbenv;

	dbenv = env->dbenv;

	return (__aes_derivekeys(
	    env, db_cipher, (u_int8_t *)dbenv->passwd, dbenv->passwd_len));
}

static int
__aes_derivekeys(env, db_cipher, passwd, plen)
	ENV *env;
	DB_CIPHER *db_cipher;
	u_int8_t *passwd;
	size_t plen;
{
	AES_CIPHER *aes;
	SHA1_CTX ctx;
#ifdef	HAVE_CRYPTO_IPP
	IppStatus ipp_ret;
#else
	int ret;
#endif
	u_int32_t temp[DB_MAC_KEY/4];

	if (passwd == NULL)
		return (EINVAL);

	aes = (AES_CIPHER *)db_cipher->data;

	/* Derive the crypto keys */
	__db_SHA1Init(&ctx);
	__db_SHA1Update(&ctx, passwd, plen);
	__db_SHA1Update(&ctx, (u_int8_t *)DB_ENC_MAGIC, strlen(DB_ENC_MAGIC));
	__db_SHA1Update(&ctx, passwd, plen);
	__db_SHA1Final((u_int8_t *)temp, &ctx);

#ifdef	HAVE_CRYPTO_IPP
	if ((ipp_ret = ippsRijndael128Init((const Ipp8u *)temp,
	    IppsRijndaelKey128, (IppsRijndael128Spec *)aes->ipp_ctx))
	    != ippStsNoErr) {
		__aes_err(env, (int)ipp_ret);
		return (EAGAIN);
	}
#else
	if ((ret = __db_makeKey(&aes->encrypt_ki, DIR_ENCRYPT,
	    DB_AES_KEYLEN, (char *)temp)) != TRUE) {
		__aes_err(env, ret);
		return (EAGAIN);
	}
	if ((ret = __db_makeKey(&aes->decrypt_ki, DIR_DECRYPT,
	    DB_AES_KEYLEN, (char *)temp)) != TRUE) {
		__aes_err(env, ret);
		return (EAGAIN);
	}
#endif
	return (0);
}

/*
 * __aes_err --
 *	Handle AES-specific errors.  Codes and messages derived from
 *	rijndael/rijndael-api-fst.h.
 */
static void
__aes_err(env, err)
	ENV *env;
	int err;
{
	char *errstr;

	switch (err) {
#ifdef	HAVE_CRYPTO_IPP
	case ippStsNullPtrErr:
		errstr = DB_STR("0182", "IPP AES NULL pointer error");
		break;
	case ippStsLengthErr:
		errstr = DB_STR("0183", "IPP AES length error");
		break;
	case ippStsContextMatchErr:
		errstr = DB_STR("0184",
		    "IPP AES context does not match operation");
		break;
	case ippStsUnderRunErr:
		errstr = DB_STR("0185", "IPP AES srclen size error");
		break;
#else
	case BAD_KEY_DIR:
		errstr = DB_STR("0186", "AES key direction is invalid");
		break;
	case BAD_KEY_MAT:
		errstr = DB_STR("0187",
		    "AES key material not of correct length");
		break;
	case BAD_KEY_INSTANCE:
		errstr = DB_STR("0188", "AES key passwd not valid");
		break;
	case BAD_CIPHER_MODE:
		errstr = DB_STR("0189",
		    "AES cipher in wrong state (not initialized)");
		break;
	case BAD_BLOCK_LENGTH:
		errstr = DB_STR("0190", "AES bad block length");
		break;
	case BAD_CIPHER_INSTANCE:
		errstr = DB_STR("0191", "AES cipher instance is invalid");
		break;
	case BAD_DATA:
		errstr = DB_STR("0192", "AES data contents are invalid");
		break;
	case BAD_OTHER:
		errstr = DB_STR("0193", "AES unknown error");
		break;
#endif
	default:
		errstr = DB_STR("0194", "AES error unrecognized");
		break;
	}
	__db_errx(env, "%s", errstr);
	return;
}
