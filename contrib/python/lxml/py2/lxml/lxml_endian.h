#ifndef PY_BIG_ENDIAN

#ifdef _MSC_VER
typedef unsigned __int32 uint32_t;
#else
#include <stdint.h>
#endif

static CYTHON_INLINE int _lx__is_big_endian(void) {
    union {uint32_t i; char c[4];} x = {0x01020304};
    return x.c[0] == 1;
}
#define PY_BIG_ENDIAN _lx__is_big_endian()
#endif
