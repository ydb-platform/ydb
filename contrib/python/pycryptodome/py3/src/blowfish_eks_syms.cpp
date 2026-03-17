#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int pycryptodome_EKSBlowfish_start_operation(const uint8_t key[],
                                size_t key_len,
                                const uint8_t salt[16],
                                size_t salt_len,
                                unsigned cost,
                                unsigned invert,
                                void **pResult);
int pycryptodome_EKSBlowfish_encrypt(const void *state,
                        const uint8_t *in,
                        uint8_t *out,
                        size_t data_len);
int pycryptodome_EKSBlowfish_decrypt(const void *state,
                        const uint8_t *in,
                        uint8_t *out,
                        size_t data_len);
int pycryptodome_EKSBlowfish_stop_operation(void *state);
}

BEGIN_SYMS("Crypto.Cipher._raw_eksblowfish")
SYM(pycryptodome_EKSBlowfish_start_operation)
SYM(pycryptodome_EKSBlowfish_encrypt)
SYM(pycryptodome_EKSBlowfish_decrypt)
SYM(pycryptodome_EKSBlowfish_stop_operation)
END_SYMS()
