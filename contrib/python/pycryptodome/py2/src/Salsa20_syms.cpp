#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int Salsa20_8_core(const uint8_t *x, const uint8_t *y,
                   uint8_t *out);
}

BEGIN_SYMS("Crypto.Cipher._Salsa20")
SYM(Salsa20_8_core)
END_SYMS()
