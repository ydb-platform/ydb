#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
#define MD5_DIGEST_SIZE 16

int MD5_init(void **shaState);
int MD5_destroy(void *shaState);
int MD5_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int MD5_digest(const void *shaState,
                  uint8_t digest[MD5_DIGEST_SIZE]);
int MD5_copy(const void *src, void *dst);

int MD5_pbkdf2_hmac_assist(const void *inner,
                    const void *outer,
                    const uint8_t first_digest[MD5_DIGEST_SIZE],
                    uint8_t final_digest[MD5_DIGEST_SIZE],
                    size_t iterations);
}

BEGIN_SYMS("Crypto.Hash._MD5")
SYM(MD5_init)
SYM(MD5_destroy)
SYM(MD5_update)
SYM(MD5_digest)
SYM(MD5_copy)
SYM(MD5_pbkdf2_hmac_assist)
END_SYMS()
