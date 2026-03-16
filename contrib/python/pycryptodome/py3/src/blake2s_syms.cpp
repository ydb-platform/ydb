#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int blake2s_init(void **state,
                 const uint8_t *key,
                 size_t key_size,
                 size_t digest_size);
int blake2s_destroy(void *state);
int blake2s_update(void *state,
                   const uint8_t *buf,
                   size_t len);
int blake2s_digest(const void *state,
                   uint8_t digest[32]);
int blake2s_copy(const void *src, void *dst);
}

BEGIN_SYMS("Crypto.Hash._BLAKE2s")
SYM(blake2s_init)
SYM(blake2s_destroy)
SYM(blake2s_update)
SYM(blake2s_digest)
SYM(blake2s_copy)
END_SYMS()
