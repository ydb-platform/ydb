#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int AESNI_start_operation(const uint8_t key[],
                        size_t key_len,
                        void **pResult);
int AESNI_encrypt(const void *state,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int AESNI_decrypt(const void *state,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int AESNI_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_aesni")
SYM(AESNI_start_operation)
SYM(AESNI_encrypt)
SYM(AESNI_decrypt)
SYM(AESNI_stop_operation)
END_SYMS()
