#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int pycryptodome_Blowfish_start_operation(const uint8_t key[],
                             size_t key_len,
                             void **pResult);
int pycryptodome_Blowfish_encrypt(const void *state,
                     const uint8_t *in,
                     uint8_t *out,
                     size_t data_len);
int pycryptodome_Blowfish_decrypt(const void *state,
                     const uint8_t *in,
                     uint8_t *out,
                     size_t data_len);
int pycryptodome_Blowfish_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_blowfish")
SYM(pycryptodome_Blowfish_start_operation)
SYM(pycryptodome_Blowfish_encrypt)
SYM(pycryptodome_Blowfish_decrypt)
SYM(pycryptodome_Blowfish_stop_operation)
END_SYMS()
