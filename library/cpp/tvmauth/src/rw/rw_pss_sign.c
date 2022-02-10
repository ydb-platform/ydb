#include "rw.h" 
 
#include <contrib/libs/openssl/include/openssl/evp.h> 
 
//#define DBG_FUZZING

int RwApply(const int flen, const unsigned char* from, unsigned char* to, const TRwKey* rw) {
    int i, j, num, k, r = -1; 
    BN_CTX* ctx = NULL; 
    BIGNUM *f = NULL, *ret = NULL; 
 
    if ((ctx = BN_CTX_secure_new()) == NULL)
        goto err; 
    BN_CTX_start(ctx); 
 
    f = BN_CTX_get(ctx); 
    ret = BN_CTX_get(ctx); 
 
    num = BN_num_bytes(rw->N);
 
    if (num <= 0)
        goto err;

    if (!f || !ret) 
        goto err; 
 
    if (BN_bin2bn(from, flen, f) == NULL) 
        goto err; 
    if (BN_ucmp(f, rw->N) >= 0)
        goto err; 
 
    if (!rw->Meth->RwApply(ret, f, ctx, rw))
        goto err; 
 
    j = BN_num_bytes(ret); 
    if (num < j || j < 0)
        goto err;

    i = BN_bn2bin(ret, to + num - j);
    if (i < 0 || i > num)
        goto err;

    for (k = 0; k < (num - i); k++) 
        to[k] = 0; 
    r = num; 
 
err: 
    if (ctx != NULL) { 
        BN_CTX_end(ctx); 
        BN_CTX_free(ctx); 
    } 
    return r; 
} 
 
int RwPssrSignHash(const unsigned char* from, unsigned char* to, TRwKey* rw, const EVP_MD* md) {
    unsigned char* padding = NULL;
    int result = 0; 
 
    if (from == NULL || to == NULL || rw == NULL || md == NULL) 
        return 0; 
 
    int digest_size = EVP_MD_size(md);
    int sig_size = RwModSize(rw);
 
    if (digest_size <= 0 || sig_size <= 0)
        return 0;

    int tries = 50;
    do {
        if (padding != NULL) {
            free(padding);
#ifdef DBG_FUZZING
            fprintf(stderr, "Padding regenerating required\n");
#endif
        }
 
        padding = malloc(sig_size);
        if (padding == NULL)
            return 0;
 
        if (!RwPaddingAddPssr(rw, padding, from, md, digest_size))
            goto err;
    } while (padding[0] == 0x00 && tries-- > 0);

    result = RwNoPaddingSign(sig_size, padding, to, rw);
 
err: 
    if (padding != NULL) 
        free(padding);
 
    return result; 
} 
 
int RwPssrSignMsg(const int msgLen, const unsigned char* msg, unsigned char* to, TRwKey* rw, const EVP_MD* md) {
    EVP_MD_CTX* mdctx = NULL; 
    unsigned char* digest = NULL; 
    unsigned int digestLen; 
    int result = 0; 
 
    if (msg == NULL || to == NULL || rw == NULL || md == NULL) 
        goto err; 
 
    if (rw->P == NULL || rw->Q == NULL)
        goto err; 
 
    if ((mdctx = EVP_MD_CTX_create()) == NULL) 
        goto err; 
 
    if (1 != EVP_DigestInit_ex(mdctx, md, NULL)) 
        goto err; 
 
    if (1 != EVP_DigestUpdate(mdctx, msg, msgLen)) 
        goto err; 
 
    if ((digest = (unsigned char*)malloc(EVP_MD_size(md))) == NULL)
        goto err; 
 
    if (1 != EVP_DigestFinal_ex(mdctx, digest, &digestLen)) 
        goto err; 
 
    result = RwPssrSignHash(digest, to, rw, md);
 
err: 
    if (mdctx != NULL) 
        EVP_MD_CTX_destroy(mdctx); 
    if (digest != NULL) 
        free(digest);
 
    return result; 
} 
 
int RwPssrVerifyHash(const unsigned char* from, const unsigned char* sig, const int sig_len, const TRwKey* rw, const EVP_MD* md) {
    unsigned char* buffer = NULL; 
    int buffer_len; 
    int salt_size;
    int result = 0; 
 
    if (from == NULL || sig == NULL || rw == NULL || md == NULL) 
        return 0; 
 
    if (rw->N == NULL || rw->Meth == NULL)
        return 0; 
 
    salt_size = EVP_MD_size(md); 
    if (salt_size <= 0)
        return 0;

    buffer_len = RwModSize(rw);
    if (buffer_len <= 0)
        return 0;

    buffer = (unsigned char*)malloc(buffer_len);
    if (buffer == NULL)
        return 0;
 
    if (RwApply(sig_len, sig, buffer, rw) <= 0)
        goto err; 
 
    if (RwVerifyPssr(rw, from, md, buffer, salt_size) <= 0)
        goto err; 
 
    result = 1; 
 
err: 
    if (buffer != NULL) 
        free(buffer);
 
    return result; 
} 
 
int RwPssrVerifyMsg(const int msgLen, const unsigned char* msg, const unsigned char* sig, const int sig_len, const TRwKey* rw, const EVP_MD* md) {
    EVP_MD_CTX* mdctx = NULL; 
    unsigned char* digest = NULL; 
    unsigned int digestLen = 0; 
    int result = 0; 
 
    if (msg == NULL || msgLen == 0 || sig == NULL || rw == NULL || md == NULL) 
        goto err; 
 
    if (rw->N == NULL)
        goto err; 
 
    if ((mdctx = EVP_MD_CTX_create()) == NULL) 
        goto err; 
 
    if (1 != EVP_DigestInit_ex(mdctx, md, NULL)) 
        goto err; 
 
    int size_to_alloc = EVP_MD_size(md);
    if (size_to_alloc <= 0)
        goto err; 
 
    if ((digest = (unsigned char*)malloc(size_to_alloc)) == NULL)
        goto err;

    if (1 != EVP_DigestUpdate(mdctx, msg, msgLen)) 
        goto err; 
 
    if (1 != EVP_DigestFinal_ex(mdctx, digest, &digestLen)) 
        goto err; 
 
    result = RwPssrVerifyHash(digest, sig, sig_len, rw, md);
 
err: 
    if (mdctx != NULL) 
        EVP_MD_CTX_destroy(mdctx); 
    if (digest != NULL) 
        free(digest);
 
    return result; 
} 
