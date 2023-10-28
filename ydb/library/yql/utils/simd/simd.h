#pragma once

#include <util/system/cpu_id.h>
#include <util/system/types.h>

#include <stdlib.h>

#include "simd_avx2.h"
#include "simd_sse42.h"
#include "simd_fallback.h"

namespace NSimd {

template<int RegisterSize, typename TBaseRegister, template<typename> typename TSimd>
struct TSimdTraits {
    using TRegister = TBaseRegister;
    template<typename T>
    using TSimd8 = TSimd<T>;

    static constexpr int Size = RegisterSize;
};

using TSimdAVX2Traits = TSimdTraits<32, __m256i, NSimd::NAVX2::TSimd8>;
using TSimdSSE42Traits = TSimdTraits<16, __m128i, NSimd::NSSE42::TSimd8>;
using TSimdFallbackTraits = TSimdTraits<8, ui64, NSimd::NFallback::TSimd8>;


template<typename TFactory>
auto SelectSimdTraits(const TFactory& factory) {
    if (NX86::HaveAVX2()) {
        return factory.template Create<TSimdAVX2Traits>();
    } else if (NX86::HaveSSE42()) {
        return factory.template Create<TSimdSSE42Traits>();
    } else {
        return factory.template Create<TSimdFallbackTraits>();   
    }
}

// Creates unpack mask for Simd register content. dataSize - value in bytes to unpack, stripeSize - distance between content parts.
// when needOffset is true, first data part starts at stipeSize bytes in result register  
template<typename TTraits>
auto CreateUnpackMask(ui32 dataSize, ui32 stripeSize, bool needOffset) {

    using TSimdI8 = typename TTraits::template TSimd8<i8>;
    i8 indexes[TTraits::Size];

    bool insideStripe = needOffset;
    ui32 stripeOffset = 0;
    ui32 currOffset = 0;
    ui32 dataOffset = 0;
    ui32 currDataSize = 0;

    while ( currOffset < TTraits::Size) {
        if (insideStripe) {
            if (stripeOffset >= stripeSize) {
                insideStripe = false;
                currDataSize = 0;
                stripeOffset = 0;
            } else {
                indexes[currOffset++] = -1;
                stripeOffset++;
            }
        } else {
            indexes[currOffset++] = dataOffset++;
            currDataSize++;
            if (currDataSize >= dataSize) {
                insideStripe = true;
                currDataSize = 0;
                stripeOffset = 0;
            }
        }
    }

    return TSimdI8(indexes);
}

}