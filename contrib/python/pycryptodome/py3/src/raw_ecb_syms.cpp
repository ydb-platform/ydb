#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int ECB_start_operation(void *cipher,
                        void **pResult);
int ECB_encrypt(void *ecbState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int ECB_decrypt(void *ecbState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int ECB_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_ecb")
SYM(ECB_start_operation)
SYM(ECB_encrypt)
SYM(ECB_decrypt)
SYM(ECB_stop_operation)
END_SYMS()
