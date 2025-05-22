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
    using TSimdI8 = TSimd8<i8>;
    static constexpr int Size = RegisterSize;
};

using TSimdAVX2Traits = TSimdTraits<32, __m256i, NSimd::NAVX2::TSimd8>;
using TSimdSSE42Traits = TSimdTraits<16, __m128i, NSimd::NSSE42::TSimd8>;
using TSimdFallbackTraits = TSimdTraits<8, ui64, NSimd::NFallback::TSimd8>;


template<typename TFactory>
auto SelectSimdTraits(const TFactory& factory) {
    if (NX86::HaveAVX2()) {
        return factory.template Create<TSimdAVX2Traits>();
    } else {
        return factory.template Create<TSimdSSE42Traits>();
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

template
__attribute__((target("avx2")))
auto CreateUnpackMask<NSimd::TSimdAVX2Traits>(ui32, ui32, bool);

template
__attribute__((target("sse4.2")))
auto CreateUnpackMask<NSimd::TSimdSSE42Traits>(ui32, ui32, bool);


// Creates mask to advance register content for N bytes. When N is negative, move data to lower bytes.
template<typename TTraits> auto AdvanceBytesMask(const int N) {
    i8 positions[TTraits::Size];
    if (N < 0) {
        for (int i = 0; i < TTraits::Size; i += 1) {
            positions[i] = -N + i > (TTraits::Size - 1) ? -1 : -N + i;
        }
    } else {
        for (int i = 0; i < TTraits::Size; i += 1) {
            positions[i] = -N + i < 0 ? -1 : -N + i;
        }
    }
    return typename TTraits::TSimdI8(positions);
}


template
__attribute__((target("avx2")))
auto AdvanceBytesMask<NSimd::TSimdAVX2Traits>(const int);


template
__attribute__((target("sse4.2")))
auto AdvanceBytesMask<NSimd::TSimdSSE42Traits>(const int);


// Prepare unpack mask to merge two columns in one register. col1Bytes, col2Bytes - size of data in columns.
template<typename TTraits>
void PrepareMergeMasks( ui32 col1Bytes, ui32 col2Bytes, typename TTraits::TSimdI8& unpackMask1, typename TTraits::TSimdI8& unpackMask2) {
    unpackMask1 = CreateUnpackMask<TTraits>(col1Bytes, col2Bytes, false);
    unpackMask2 = CreateUnpackMask<TTraits>(col2Bytes, col1Bytes, true);
}


template
__attribute__((target("avx2")))
void PrepareMergeMasks<NSimd::TSimdAVX2Traits>(ui32, ui32, NSimd::TSimdAVX2Traits::TSimdI8 &, NSimd::TSimdAVX2Traits::TSimdI8 &);


template
__attribute__((target("sse4.2")))
void PrepareMergeMasks<NSimd::TSimdSSE42Traits>(ui32, ui32, NSimd::TSimdSSE42Traits::TSimdI8 &, NSimd::TSimdSSE42Traits::TSimdI8 &);


}
