#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int SHA512_init(void **shaState,
                size_t digest_size);
int SHA512_destroy(void *shaState);
int SHA512_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int SHA512_digest(const void *shaState,
                  uint8_t *digest,
                  size_t digest_size);
int SHA512_copy(const void *src, void *dst);

int SHA512_pbkdf2_hmac_assist(const void *inner,
                    const void *outer,
                    const uint8_t *first_digest,
                    uint8_t *final_digest,
                    size_t iterations,
                    size_t digest_size);
}

BEGIN_SYMS("Crypto.Hash._SHA512")
SYM(SHA512_init)
SYM(SHA512_destroy)
SYM(SHA512_update)
SYM(SHA512_digest)
SYM(SHA512_copy)
SYM(SHA512_pbkdf2_hmac_assist)
END_SYMS()
