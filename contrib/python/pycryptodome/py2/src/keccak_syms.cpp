#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int keccak_init(void **state,
                size_t capacity_bytes,
                uint8_t rounds);
int keccak_destroy(void *state);
int keccak_absorb(void *state,
                  const uint8_t *in,
                  size_t len);
int keccak_squeeze(const void *state,
                   uint8_t *out,
                   size_t len,
                   uint8_t padding);
int keccak_digest(void *state,
                  uint8_t *digest,
                  size_t len,
                  uint8_t padding);
int keccak_copy(const void *src, void *dst);
int keccak_reset(void *state);
}

BEGIN_SYMS("Crypto.Hash._keccak")
SYM(keccak_init)
SYM(keccak_destroy)
SYM(keccak_absorb)
SYM(keccak_squeeze)
SYM(keccak_digest)
SYM(keccak_copy)
SYM(keccak_reset)
END_SYMS()
