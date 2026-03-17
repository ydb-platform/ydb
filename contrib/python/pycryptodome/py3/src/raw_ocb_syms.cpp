#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int OCB_start_operation(void *cipher,
    const uint8_t *offset_0,
    size_t offset_0_len,
    void **pState);
int OCB_encrypt(void *state,
    const uint8_t *in,
    uint8_t *out,
    size_t data_len);
int OCB_decrypt(void *state,
    const uint8_t *in,
    uint8_t *out,
    size_t data_len);
int OCB_update(void *state,
    const uint8_t *in,
    size_t data_len);
int OCB_digest(void *state,
    uint8_t *tag,
    size_t tag_len);
int OCB_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_ocb")
SYM(OCB_start_operation)
SYM(OCB_encrypt)
SYM(OCB_decrypt)
SYM(OCB_update)
SYM(OCB_digest)
SYM(OCB_stop_operation)
END_SYMS()
