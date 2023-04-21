#pragma once

#include <util/system/compiler.h>
#include <util/system/defaults.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {

constexpr size_t MAX_PACKED32_SIZE = 5;
constexpr size_t MAX_PACKED64_SIZE = 9;

Y_FORCE_INLINE size_t GetPack32Length(ui32 value) {
    if (value < 0x80) {
        return 1;
    }
    else if (value < 0x4000) {
        return 2;
    }
    else if (value < 0x200000) {
        return 3;
    }
    else {
        return 5;
    }
}

[[nodiscard]] Y_FORCE_INLINE size_t Pack32(ui32 value, char buffer[MAX_PACKED32_SIZE]) {
    if (value < 0x80) {
        buffer[0] = value << 0x1;
        return 1;
    }
    else if (value < 0x4000) {
        WriteUnaligned<ui16>(((ui16*)(buffer)), ui16((value << 2) | 0x1));
        return 2;
    }
    else if (value < 0x200000) {
        WriteUnaligned<ui32>(((ui32*)(buffer)), ui32((value << 3) | 0x3));
        return 3;
    }
    else {
        buffer[0] = 0x7;
        WriteUnaligned<ui32>((ui32*)(buffer + 1), ui32(value));
        return 5;
    }
}

[[nodiscard]] Y_FORCE_INLINE size_t Unpack32(const char* buffer, size_t length, ui32& value) {
    if (Y_UNLIKELY(!length)) {
        return 0;
    }

    size_t count;
    if (0 == (buffer[0] & 0x1)) {
        count = 1;
        value = ((const ui8*)buffer)[0] >> 1;
    } else if (0 == (buffer[0] & 0x2)) {
        count = 2;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = ReadUnaligned<ui16>((const ui16*)buffer) >> 2;
    } else if (0 == (buffer[0] & 0x4)) {
        count = 3;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = (ReadUnaligned<ui16>((const ui16*)buffer) >> 3) | (((const ui8*)buffer)[2] << 13);
    } else {
        count = 5;
        if (Y_UNLIKELY(length < count || buffer[0] != 0x7)) {
            return 0;
        }
        value = ReadUnaligned<ui32>((const ui32*)(buffer + 1));
    }

    return count;
}

Y_FORCE_INLINE size_t GetPack64Length(ui64 value) {
    if (value < 0x80) {
        return 1;
    }
    else if (value < 0x4000) {
        return 2;
    }
    else if (value < 0x200000) {
        return 3;
    }
    else if (value < 0x10000000) {
        return 4;
    }
    else if (value < 0x800000000) {
        return 5;
    }
    else if (value < 0x40000000000) {
        return 6;
    }
    else if (value < 0x2000000000000) {
        return 7;
    }
    else {
        return 9;
    }
}

[[nodiscard]] Y_FORCE_INLINE size_t Pack64(ui64 value, char buffer[MAX_PACKED64_SIZE]) {
    if (value < 0x80) {
        buffer[0] = value << 1;
        return 1;
    }
    else if (value < 0x4000) {
        WriteUnaligned<ui16>((ui16*)(buffer), ui16((value << 2) | 0x1));
        return 2;
    }
    else if (value < 0x200000) {
        WriteUnaligned<ui32>((ui32*)(buffer), ui32((value << 3) | 0x3));
        return 3;
    }
    else if (value < 0x10000000) {
        WriteUnaligned<ui32>((ui32*)(buffer), ui32((value << 4) | 0x7));
        return 4;
    }
    else if (value < 0x800000000) {
        WriteUnaligned<ui64>((ui64*)(buffer), ui64((value << 5) | 0xF));
        return 5;
    }
    else if (value < 0x40000000000) {
        WriteUnaligned<ui64>((ui64*)(buffer), ui64((value << 6) | 0x1F));
        return 6;
    }
    else if (value < 0x2000000000000) {
        WriteUnaligned<ui64>((ui64*)(buffer), ui64((value << 7) | 0x3F));
        return 7;
    }
    else {
        buffer[0] = 0x7F;
        WriteUnaligned<ui64>((ui64*)(buffer + 1), value);
        return 9;
    }
}

[[nodiscard]] Y_FORCE_INLINE size_t Unpack64(const char* buffer, size_t length, ui64& value) {
    if (Y_UNLIKELY(!length)) {
        return 0;
    }

    size_t count;
    if (0 == (buffer[0] & 0x1)) {
        count = 1;
        value = ((const ui8*)buffer)[0] >> 1;
    } else if (0 == (buffer[0] & 0x2)) {
        count = 2;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = ReadUnaligned<ui16>((const ui16*)buffer) >> 2;
    } else if (0 == (buffer[0] & 0x4)) {
        count = 3;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = (ui64(ReadUnaligned<ui16>((const ui16*)buffer)) >> 3) | (ui64(((const ui8*)buffer)[2]) << 13);
    } else if (0 == (buffer[0] & 0x8)) {
        count = 4;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = ReadUnaligned<ui32>((const ui32*)buffer) >> 4;
    } else if (0 == (buffer[0] & 0x10)) {
        count = 5;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = (ui64(ReadUnaligned<ui32>((const ui32*)buffer)) >> 5) | (ui64(((const ui8*)buffer)[4]) << 27);
    } else if (0 == (buffer[0] & 0x20)) {
        count = 6;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = (ui64(ReadUnaligned<ui32>((const ui32*)buffer)) >> 6) | (ui64(ReadUnaligned<ui16>(((const ui16*)buffer) + 2)) << 26);
    } else if (0 == (buffer[0] & 0x40)) {
        count = 7;
        if (Y_UNLIKELY(length < count)) {
            return 0;
        }
        value = (ui64(ReadUnaligned<ui32>((const ui32*)buffer)) >> 7) | (ui64(ReadUnaligned<ui16>(((const ui16*)buffer) + 2)) << 25) | (ui64(((const ui8*)buffer)[6]) << 41);
    } else {
        count = 9;
        if (Y_UNLIKELY(length < count || buffer[0] != 0x7F)) {
            return 0;
        }
        value = ReadUnaligned<ui64>((const ui64*)(buffer + 1));
    }

    return count;
}

} // NKikimr
