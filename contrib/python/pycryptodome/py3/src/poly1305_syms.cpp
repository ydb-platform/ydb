#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int pycryptodome_poly1305_init(void **state,
                  const uint8_t *r,
                  size_t r_len,
                  const uint8_t *s,
                  size_t s_len);
int poly1305_destroy(void *state);
int poly1305_update(void *state,
                    const uint8_t *in,
                    size_t len);
int poly1305_digest(const void *state,
                    uint8_t *digest,
                    size_t len);
}

BEGIN_SYMS("Crypto.Hash._poly1305")
SYM(pycryptodome_poly1305_init)
SYM(poly1305_destroy)
SYM(poly1305_update)
SYM(poly1305_digest)
END_SYMS()
