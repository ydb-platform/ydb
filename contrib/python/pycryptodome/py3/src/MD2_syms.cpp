#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int md2_init(void **shaState);
int md2_destroy(void *shaState);
int md2_update(void *hs,
                  const uint8_t *buf,
                  size_t len);
int md2_digest(const void *shaState,
                  uint8_t digest[20]);
int md2_copy(const void *src, void *dst);
}

BEGIN_SYMS("Crypto.Hash._MD2")
SYM(md2_init)
SYM(md2_destroy)
SYM(md2_update)
SYM(md2_digest)
SYM(md2_copy)
END_SYMS()
