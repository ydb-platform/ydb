#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
void strxor(const uint8_t *in1,
            const uint8_t *in2,
            uint8_t *out, size_t len);
void strxor_c(const uint8_t *in,
              uint8_t c,
              uint8_t *out,
              size_t len);
}

BEGIN_SYMS("Crypto.Util._strxor")
SYM(strxor)
SYM(strxor_c)
END_SYMS()
