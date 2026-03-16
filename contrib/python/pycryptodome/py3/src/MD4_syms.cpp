#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int md4_init(void **shaState);
int md4_destroy(void *shaState);
int md4_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int md4_digest(const void *shaState,
                  uint8_t digest[20]);
int md4_copy(const void *src, void *dst);
}

BEGIN_SYMS("Crypto.Hash._MD4")
SYM(md4_init)
SYM(md4_destroy)
SYM(md4_update)
SYM(md4_digest)
SYM(md4_copy)
END_SYMS()
