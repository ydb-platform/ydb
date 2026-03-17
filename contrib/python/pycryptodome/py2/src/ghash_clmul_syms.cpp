#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int ghash_clmul(uint8_t y_out[16],
                const uint8_t block_data[],
                size_t len,
                const uint8_t y_in[16],
                const void *exp_key);
int ghash_expand_clmul(const uint8_t h[16],
                       void **ghash_tables);
int ghash_destroy_clmul(void *ghash_tables);
}

BEGIN_SYMS("Crypto.Hash._ghash_clmul")
SYM(ghash_clmul)
SYM(ghash_expand_clmul)
SYM(ghash_destroy_clmul)
END_SYMS()
