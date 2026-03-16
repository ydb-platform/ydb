#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int pkcs1_decode(const uint8_t *em, size_t len_em,
                 const uint8_t *sentinel, size_t len_sentinel,
                 size_t expected_pt_len,
                 uint8_t *output);

int oaep_decode(const uint8_t *em,
                size_t em_len,
                const uint8_t *lHash,
                size_t hLen,
                const uint8_t *db,
                size_t db_len);
}

BEGIN_SYMS("Crypto.Cipher._pkcs1_decode")
SYM(pkcs1_decode)
SYM(oaep_decode)
END_SYMS()
