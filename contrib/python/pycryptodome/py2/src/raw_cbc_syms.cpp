#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int CBC_start_operation(void *cipher,
                        const uint8_t iv[],
                        size_t iv_len,
                        void **pResult);
int CBC_encrypt(void *cbcState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int CBC_decrypt(void *cbcState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int CBC_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_cbc")
SYM(CBC_start_operation)
SYM(CBC_encrypt)
SYM(CBC_decrypt)
SYM(CBC_stop_operation)
END_SYMS()
