#include <library/python/symbols/registry/syms.h>

#include "common.h"

extern "C" {
int have_aes_ni(void);
int have_clmul(void);
}

BEGIN_SYMS("Crypto.Util._cpuid_c")
SYM(have_aes_ni)
SYM(have_clmul)
END_SYMS()
