#pragma once

#include <ydb/library/yql/utils/simd/simd.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {



// Calculates CRC32 of data using hardware acceleration instruction (Size <= 16)
template<typename TTraits, ui32 Size> ui32 CalculateCRC32(const ui8 * data, ui32 initHash = 0) {
    static_assert(Size <= 16, "Size for template CRC32 calculation should be <= 16 !");

    using TSimdI8 = TTraits::TSimdI8;

    ui32 hash = initHash;

    if constexpr (Size == 1 ) {
        hash = TSimdI8::CRC32u8(hash, *(ui8*) data);
    }

    if constexpr (Size == 2 ) {
        hash = TSimdI8::CRC32u16(hash, *(ui16*) data);
    }

    if constexpr (Size == 3 ) {

        hash = TSimdI8::CRC32u16(hash, *(ui16*) data);
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+2));
    }

    if constexpr (Size == 4 ) {
        hash = TSimdI8::CRC32u32(hash, *(ui32*) data);
    }

    if constexpr (Size == 5 ) {
        hash = TSimdI8::CRC32u32(hash, *(ui32*) data);
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+4));
    }

    if constexpr (Size == 6 ) {
        hash = TSimdI8::CRC32u32(hash, *(ui32*) data);
        hash = TSimdI8::CRC32u16(hash, *(ui16*) (data+4));
    }

    if constexpr (Size == 7 ) {
        hash = TSimdI8::CRC32u32(hash, *(ui32*) data);
        hash = TSimdI8::CRC32u16(hash, *(ui16*) (data+4));
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+6));
    }

    if constexpr (Size == 8 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
    }

    if constexpr (Size == 9 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+8));
    }

    if constexpr (Size == 10 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u16(hash, *(ui16*) (data+8));
    }

    if constexpr (Size == 11 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u16(hash, *(ui16*) (data+8));
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+10));
    }

    if constexpr (Size == 12 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u32(hash, *(ui32*) (data+8));
    }

    if constexpr (Size == 13 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u32(hash, *(ui32*) (data+8));
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+12));
    }

    if constexpr (Size == 14 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u32(hash, *(ui32*) (data+8));
        hash = TSimdI8::CRC32u16(hash, *(ui16*) (data+12));
    }

    if constexpr (Size == 15 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u32(hash, *(ui32*) (data+8));
        hash = TSimdI8::CRC32u16(hash, *(ui16*) (data+12));
        hash = TSimdI8::CRC32u8(hash, *(ui8*) (data+14));
    }

    if constexpr (Size == 16 ) {
        hash = TSimdI8::CRC32u64(hash, *(ui64*) data);
        hash = TSimdI8::CRC32u64(hash, *(ui64*) (data+8));
    }

    return hash;

}


template <typename TTraits>
inline ui32 CalculateCRC32(const ui8 * data, ui32 size, ui32 hash = 0 ) {

    using TSimdI8 = TTraits::TSimdI8;

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
}

}

}
