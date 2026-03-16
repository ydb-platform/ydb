#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
#define SHA1_DIGEST_SIZE 20

int SHA1_init(void **shaState);
int SHA1_destroy(void *shaState);
int SHA1_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int SHA1_digest(const void *shaState,
                  uint8_t digest[SHA1_DIGEST_SIZE]);
int SHA1_copy(const void *src, void *dst);

int SHA1_pbkdf2_hmac_assist(const void *inner,
                    const void *outer,
                    const uint8_t first_digest[SHA1_DIGEST_SIZE],
                    uint8_t final_digest[SHA1_DIGEST_SIZE],
                    size_t iterations);
}

BEGIN_SYMS("Crypto.Hash._SHA1")
SYM(SHA1_init)
SYM(SHA1_destroy)
SYM(SHA1_update)
SYM(SHA1_digest)
SYM(SHA1_copy)
SYM(SHA1_pbkdf2_hmac_assist)
END_SYMS()
