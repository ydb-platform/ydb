#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int SHA256_init(void **shaState);
int SHA256_destroy(void *shaState);
int SHA256_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int SHA256_digest(const void *shaState,
                  uint8_t *digest,
                  size_t digest_size);
int SHA256_copy(const void *src, void *dst);

int SHA256_pbkdf2_hmac_assist(const void *inner,
                    const void *outer,
                    const uint8_t *first_digest,
                    uint8_t *final_digest,
                    size_t iterations,
                    size_t digest_size);
}

BEGIN_SYMS("Crypto.Hash._SHA256")
SYM(SHA256_init)
SYM(SHA256_destroy)
SYM(SHA256_update)
SYM(SHA256_digest)
SYM(SHA256_copy)
SYM(SHA256_pbkdf2_hmac_assist)
END_SYMS()
