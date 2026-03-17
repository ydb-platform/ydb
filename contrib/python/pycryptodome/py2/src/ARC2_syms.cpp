#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int pycryptodome_ARC2_start_operation(const uint8_t key[],
                         size_t key_len,
                         size_t effective_key_len,
                         void **pResult);
int pycryptodome_ARC2_encrypt(const void *state,
                 const uint8_t *in,
                 uint8_t *out,
                 size_t data_len);
int pycryptodome_ARC2_decrypt(const void *state,
                 const uint8_t *in,
                 uint8_t *out,
                 size_t data_len);
int pycryptodome_ARC2_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_arc2")
SYM(pycryptodome_ARC2_start_operation)
SYM(pycryptodome_ARC2_encrypt)
SYM(pycryptodome_ARC2_decrypt)
SYM(pycryptodome_ARC2_stop_operation)
END_SYMS()
