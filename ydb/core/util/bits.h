#pragma once

#include <util/system/compiler.h>
#include <util/generic/ylimits.h>
#include <util/system/yassert.h>

namespace NKikimr {

    // naive implementation of clz
    inline unsigned int NaiveClz(unsigned int v) {
        Y_ASSERT(v);
        unsigned int cntr = 0;
        const static unsigned mask = 1u << (std::numeric_limits<unsigned int>::digits - 1);
        while ((v & mask) == 0) {
            v <<= 1;
            cntr++;
        }
        return cntr;
    }

    // from gcc manual:
    // Returns the number of leading 0-bits in x, starting at the most significant bit position.
    // If x is 0, the result is undefined.
    Y_FORCE_INLINE unsigned int Clz(unsigned int v) {
#ifdef __GNUC__
        return __builtin_clz(v);
#elif defined(_MSC_VER) && defined(_64_)
        // FIXME: use _BitScanReverse
        return NaiveClz(v);
#else
        return NaiveClz(v);
#endif
    }

} // NKikimr

