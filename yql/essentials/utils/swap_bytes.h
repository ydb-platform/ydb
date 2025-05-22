#pragma once
#include <util/system/compiler.h>
#include <util/system/defaults.h>

namespace NYql {

// clang generates bswap for ui32 and ui64
template <typename TUnsigned>
Y_FORCE_INLINE
TUnsigned SwapBytes(TUnsigned value) {
    TUnsigned result;
    auto* from = (ui8*)&value + sizeof(TUnsigned) - 1;
    auto* to = (ui8*)&result;
    for (size_t i = 0; i < sizeof(TUnsigned); ++i) {
        *to++ = *from--;
    }
    return result;
}

}
