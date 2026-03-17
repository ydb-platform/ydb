#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int CFB_start_operation(void *cipher,
                        const uint8_t iv[],
                        size_t iv_len,
                        size_t segment_len, /* In bytes */
                        void **pResult);
int CFB_encrypt(void *cfbState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int CFB_decrypt(void *cfbState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int CFB_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_cfb")
SYM(CFB_start_operation)
SYM(CFB_encrypt)
SYM(CFB_decrypt)
SYM(CFB_stop_operation)
END_SYMS()
