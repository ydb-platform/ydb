#pragma once

#include <yql/essentials/types/uuid/uuid.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>

#include <array>
#include <cstring>

namespace NYql::NUuidKeyGen {

static constexpr ui32 PrefixBits = 10;
static constexpr ui32 PrefixMask10 = (1U << PrefixBits) - 1U;

static constexpr ui32 TimestampBits = 30;
static constexpr ui32 TimestampFieldLowBit = 33;
static constexpr ui32 MaskPos = PrefixBits - 1;

// Precomputed for the default 10-bit prefix (maskPos = 9).
static constexpr ui64 V8PrefixMask = 0xFFC0000000000000ULL;
static constexpr ui64 V8TimestampMask = 0x003FFFFFFF000000ULL;

static constexpr ui64 V7TimestampPrefixMask = static_cast<ui64>(PrefixMask10) << (48 - PrefixBits);
static constexpr ui32 V8PrefixShift = 64 - PrefixBits;

// UDFs return 16 raw bytes in YDB/YQL Uuid internal storage layout (Microsoft GUID
// mixed-endian, the same representation as table cells and RandomUuid()).
// YQL applies ParseUuidToArray when a UUID literal string is parsed; UDFs must emit
// that internal layout directly.
//
// Field placement below uses RFC 4122 octet indices as a convenient intermediate
// layout while embedding version, variant, prefix and timestamp. The final step
// converts those octets to internal storage bytes via the same rules as
// ParseUuidToArray. This is not the Java reorder() helper used for external
// client-side UUID construction.

inline ui64 NormalizePrefixBits(ui64 prefix) {
    return prefix & PrefixMask10;
}

inline ui64 PrefixBitsForV8Msb(ui64 prefix) {
    return NormalizePrefixBits(prefix) << V8PrefixShift;
}

inline ui64 ReadBe64(const ui8* data) {
    ui64 value = 0;
    for (ui32 i = 0; i < 8; ++i) {
        value = (value << 8) | data[i];
    }
    return value;
}

inline void WriteBe64(ui64 value, ui8* data) {
    for (int i = 7; i >= 0; --i) {
        data[i] = static_cast<ui8>(value & 0xff);
        value >>= 8;
    }
}

inline void FillRandomBytes(ui8* data, size_t size) {
    for (size_t offset = 0; offset < size; offset += sizeof(ui64)) {
        const ui64 random = RandomNumber<ui64>();
        const size_t chunk = Min(size - offset, sizeof(ui64));
        for (size_t i = 0; i < chunk; ++i) {
            data[offset + i] = static_cast<ui8>((random >> (8 * (sizeof(ui64) - 1 - i))) & 0xff);
        }
    }
}

inline void RfcLayoutToYdbInternalBytes(const ui8* rfcLayout, ui8* internalBytes) {
    std::array<ui16, 8> dw{};
    for (ui32 i = 0; i < 8; ++i) {
        dw[i] = static_cast<ui16>((rfcLayout[i * 2] << 8) | rfcLayout[i * 2 + 1]);
    }
    std::swap(dw[0], dw[1]);
    for (ui32 i = 4; i < 8; ++i) {
        dw[i] = static_cast<ui16>(((dw[i] >> 8) & 0xff) | ((dw[i] & 0xff) << 8));
    }
    std::memcpy(internalBytes, dw.data(), NKikimr::NUuid::UUID_LEN);
}

inline ui64 GetPrefixMask() {
    return V8PrefixMask;
}

inline ui64 GetTimestampMask() {
    return V8TimestampMask;
}

inline ui32 GetTimestampCode(ui64 epochSeconds) {
    const ui64 diff = epochSeconds % (1ULL << TimestampBits);
    return static_cast<ui32>(diff);
}

inline ui64 UpdateMsb(ui64 msb, ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    const ui64 tsMask = GetTimestampMask();
    const ui64 prefixMask = GetPrefixMask();
    const ui64 tsCode = static_cast<ui64>(GetTimestampCode(epochSeconds))
        << (TimestampFieldLowBit - MaskPos);

    if (!hasPrefix) {
        return (msb & ~tsMask) | (tsCode & tsMask);
    }
    return (msb & ~(prefixMask | tsMask))
        | (PrefixBitsForV8Msb(prefix) | (tsCode & tsMask));
}

inline ui64 ApplyV7Prefix(ui64 timestampMs, ui64 prefix) {
    return (timestampMs & ~V7TimestampPrefixMask)
        | (NormalizePrefixBits(prefix) << (48 - PrefixBits));
}

inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV7Bytes(ui64 timestampMs) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfcLayout{};
    FillRandomBytes(rfcLayout.data(), rfcLayout.size());

    rfcLayout[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    rfcLayout[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    rfcLayout[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    rfcLayout[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    rfcLayout[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    rfcLayout[5] = static_cast<ui8>(timestampMs & 0xff);
    rfcLayout[6] = static_cast<ui8>((rfcLayout[6] & 0x0f) | 0x70);
    rfcLayout[8] = static_cast<ui8>((rfcLayout[8] & 0x3f) | 0x80);

    std::array<ui8, NKikimr::NUuid::UUID_LEN> internalBytes{};
    RfcLayoutToYdbInternalBytes(rfcLayout.data(), internalBytes.data());
    return internalBytes;
}

inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV8Bytes(ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfcLayout{};
    FillRandomBytes(rfcLayout.data(), rfcLayout.size());

    rfcLayout[6] = static_cast<ui8>((rfcLayout[6] & 0x0f) | 0x80);
    rfcLayout[8] = static_cast<ui8>((rfcLayout[8] & 0x3f) | 0x80);

    ui64 msb = ReadBe64(rfcLayout.data());
    msb = UpdateMsb(msb, prefix, epochSeconds, hasPrefix);
    WriteBe64(msb, rfcLayout.data());

    std::array<ui8, NKikimr::NUuid::UUID_LEN> internalBytes{};
    RfcLayoutToYdbInternalBytes(rfcLayout.data(), internalBytes.data());
    return internalBytes;
}

} // namespace NYql::NUuidKeyGen
