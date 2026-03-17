#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int SHA384_init(void **shaState);
int SHA384_destroy(void *shaState);
int SHA384_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int SHA384_digest(const void *shaState,
                  uint8_t *digest,
                  size_t digest_size);
int SHA384_copy(const void *src, void *dst);

int SHA384_pbkdf2_hmac_assist(const void *inner,
                    const void *outer,
                    const uint8_t *first_digest,
                    uint8_t *final_digest,
                    size_t iterations,
                    size_t digest_size);
}

BEGIN_SYMS("Crypto.Hash._SHA384")
SYM(SHA384_init)
SYM(SHA384_destroy)
SYM(SHA384_update)
SYM(SHA384_digest)
SYM(SHA384_copy)
SYM(SHA384_pbkdf2_hmac_assist)
END_SYMS()
