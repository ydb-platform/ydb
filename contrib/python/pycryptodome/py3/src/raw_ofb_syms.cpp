#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int OFB_start_operation(void *cipher,
                        const uint8_t iv[],
                        size_t iv_len,
                        void **pResult);
int OFB_encrypt(void *ofbState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int OFB_decrypt(void *ofbState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int OFB_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_ofb")
SYM(OFB_start_operation)
SYM(OFB_encrypt)
SYM(OFB_decrypt)
SYM(OFB_stop_operation)
END_SYMS()
