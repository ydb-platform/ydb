#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int monty_pow(const uint8_t *base,
               const uint8_t *exp,
               const uint8_t *modulus,
               uint8_t       *out,
               size_t len,
               uint64_t seed);
}

BEGIN_SYMS("Crypto.Math._modexp")
SYM(monty_pow)
END_SYMS()
