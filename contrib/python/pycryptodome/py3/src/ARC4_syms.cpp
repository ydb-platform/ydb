#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int ARC4_stream_encrypt(void *rc4State, const uint8_t in[],
                        uint8_t out[], size_t len);
int ARC4_stream_init(uint8_t *key, size_t keylen,
                     void **pRc4State);
int ARC4_stream_destroy(void *rc4State);
}

BEGIN_SYMS("Crypto.Cipher._ARC4")
SYM(ARC4_stream_encrypt)
SYM(ARC4_stream_init)
SYM(ARC4_stream_destroy)
END_SYMS()
