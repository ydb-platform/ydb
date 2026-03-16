#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int pycryptodome_DES_start_operation(const uint8_t key[],
                         size_t key_len,
                         void **pResult);
int pycryptodome_DES_encrypt(const void *state,
                 const uint8_t *in,
                 uint8_t *out,
                 size_t data_len);
int pycryptodome_DES_decrypt(const void *state,
                 const uint8_t *in,
                 uint8_t *out,
                 size_t data_len);
int pycryptodome_DES_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_des")
SYM(pycryptodome_DES_start_operation)
SYM(pycryptodome_DES_encrypt)
SYM(pycryptodome_DES_decrypt)
SYM(pycryptodome_DES_stop_operation)
END_SYMS()
