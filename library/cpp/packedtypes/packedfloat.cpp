#include "packedfloat.h"

#include <util/stream/output.h>

#define OUT_IMPL(T)                                                   \
    template <>                                                       \
    void Out<T>(IOutputStream & os, TTypeTraits<T>::TFuncParam val) { \
        os << (float)val;                                             \
    }

OUT_IMPL(f16)
OUT_IMPL(uf16)
OUT_IMPL(f8)
OUT_IMPL(uf8)
OUT_IMPL(f8d)
OUT_IMPL(uf8d)

#undef OUT_IMPL
