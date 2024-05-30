#pragma once

#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {



template <typename TTraits>
inline ui32 CalculateCRC32(const ui8 * data, ui32 size, ui32 hash = 0 ) {

    using TSimdI8 = typename TTraits::TSimdI8;

    while (size >= 8) {
        hash = TSimdI8::CRC32u64(hash, ReadUnaligned<ui64>(data));
        size -= 8;
        data += 8;
    }

    switch(size) {
        case 7:
            hash = TSimdI8::CRC32u32(hash, ReadUnaligned<ui32>(data));
            data += 4;
            [[fallthrough]];
        case 3:
            hash = TSimdI8::CRC32u16(hash, ReadUnaligned<ui16>(data));
            data += 2;
            [[fallthrough]];
        case 1:
            hash = TSimdI8::CRC32u8(hash, ReadUnaligned<ui8>(data));
            break;
        case 6:
            hash = TSimdI8::CRC32u32(hash, ReadUnaligned<ui32>(data));
            data += 4;
            [[fallthrough]];
        case 2:
            hash = TSimdI8::CRC32u16(hash, ReadUnaligned<ui16>(data));
            break;
        case 5:
            hash = TSimdI8::CRC32u32(hash, ReadUnaligned<ui32>(data));
            data += 4;
            hash = TSimdI8::CRC32u8(hash, ReadUnaligned<ui8>(data));
            break;
        case 4:
            hash = TSimdI8::CRC32u32(hash, ReadUnaligned<ui32>(data));
            break;
        case 0:
            break;
    }
    return hash;

}
template
__attribute__((target("avx2")))
ui32 CalculateCRC32<NSimd::TSimdAVX2Traits>(const ui8 * data, ui32 size, ui32 hash = 0 );
template
__attribute__((target("sse4.2")))
ui32 CalculateCRC32<NSimd::TSimdSSE42Traits>(const ui8 * data, ui32 size, ui32 hash = 0 );
}

}

}
