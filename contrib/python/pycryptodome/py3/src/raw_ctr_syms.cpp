#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int CTR_start_operation(void *cipher,
                        uint8_t   initialCounterBlock[],
                        size_t    initialCounterBlock_len,
                        size_t    prefix_len,
                        unsigned  counter_len,
                        unsigned  littleEndian,
                        void **pResult);
int CTR_encrypt(void *ctrState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int CTR_decrypt(void *ctrState,
                const uint8_t *in,
                uint8_t *out,
                size_t data_len);
int CTR_stop_operation(void *ctrState);
}

BEGIN_SYMS("Crypto.Cipher._raw_ctr")
SYM(CTR_start_operation)
SYM(CTR_encrypt)
SYM(CTR_decrypt)
SYM(CTR_stop_operation)
END_SYMS()
