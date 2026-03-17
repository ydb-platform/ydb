#include <library/python/symbols/registry/syms.h>

#include <stdlib.h>
#include "block_base.h"

extern "C" {
int pycryptodome_AES_start_operation(const uint8_t key[],
                        size_t key_len,
                        void **pResult);
int pycryptodome_AES_encrypt(const void *state,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int pycryptodome_AES_decrypt(const void *state,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int pycryptodome_AES_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_aes")
SYM(pycryptodome_AES_start_operation)
SYM(pycryptodome_AES_encrypt)
SYM(pycryptodome_AES_decrypt)
SYM(pycryptodome_AES_stop_operation)
END_SYMS()
