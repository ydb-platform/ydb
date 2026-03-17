#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int ripemd160_init(void **shaState);
int ripemd160_destroy(void *shaState);
int ripemd160_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int ripemd160_digest(const void *shaState,
                  uint8_t digest[20]);
int ripemd160_copy(const void *src, void *dst);
}

BEGIN_SYMS("Crypto.Hash._RIPEMD160")
SYM(ripemd160_init)
SYM(ripemd160_destroy)
SYM(ripemd160_update)
SYM(ripemd160_digest)
SYM(ripemd160_copy)
END_SYMS()
