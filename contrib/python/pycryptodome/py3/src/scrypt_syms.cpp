#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
typedef int (core_t)(const uint8_t [64], const uint8_t [64], uint8_t [64]);
int scryptROMix(const uint8_t *data_in, uint8_t *data_out,
       size_t data_len, unsigned N, core_t *core);
}

BEGIN_SYMS("Crypto.Protocol._scrypt")
SYM(scryptROMix)
END_SYMS()
