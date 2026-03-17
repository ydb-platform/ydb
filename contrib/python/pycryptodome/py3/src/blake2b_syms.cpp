#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int blake2b_init(void **state,
                 const uint8_t *key,
                 size_t key_size,
                 size_t digest_size);
int blake2b_destroy(void *state);
int blake2b_update(void *state,
                   const uint8_t *buf,
                   size_t len);
int blake2b_digest(const void *state,
                   uint8_t digest[64]);
int blake2b_copy(const void *src, void *dst);
}

BEGIN_SYMS("Crypto.Hash._BLAKE2b")
SYM(blake2b_init)
SYM(blake2b_destroy)
SYM(blake2b_update)
SYM(blake2b_digest)
SYM(blake2b_copy)
END_SYMS()
