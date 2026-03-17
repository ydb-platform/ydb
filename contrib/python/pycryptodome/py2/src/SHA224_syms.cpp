#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int SHA224_init(void **shaState);
int SHA224_destroy(void *shaState);
int SHA224_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int SHA224_digest(const void *shaState,
                  uint8_t *digest,
                  size_t digest_size);
int SHA224_copy(const void *src, void *dst);

int SHA224_pbkdf2_hmac_assist(const void *inner,
                    const void *outer,
                    const uint8_t *first_digest,
                    uint8_t *final_digest,
                    size_t iterations,
                    size_t digest_size);
}

BEGIN_SYMS("Crypto.Hash._SHA224")
SYM(SHA224_init)
SYM(SHA224_destroy)
SYM(SHA224_update)
SYM(SHA224_digest)
SYM(SHA224_copy)
SYM(SHA224_pbkdf2_hmac_assist)
END_SYMS()
