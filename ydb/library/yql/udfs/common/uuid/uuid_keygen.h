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

// Precomputed for the default 10-bit prefix (maskPos = 9), matching UuidKeyGen.java.
static constexpr ui64 V8PrefixMask = 0xFFC0000000000000ULL;
static constexpr ui64 V8TimestampMask = 0x003FFFFFFF000000ULL;

static constexpr ui64 V7TimestampPrefixMask = static_cast<ui64>(PrefixMask10) << (48 - PrefixBits);

inline ui64 ReorderMsb(ui64 v) {
    const ui64 b0 = (v >> 56) & 0xff;
    const ui64 b1 = (v >> 48) & 0xff;
    const ui64 b2 = (v >> 40) & 0xff;
    const ui64 b3 = (v >> 32) & 0xff;
    const ui64 b4 = (v >> 24) & 0xff;
    const ui64 b5 = (v >> 16) & 0xff;
    const ui64 b6 = (v >> 8) & 0xff;
    const ui64 b7 = v & 0xff;
    return (b3 << 56) | (b2 << 48) | (b1 << 40) | (b0 << 32)
        | (b5 << 24) | (b4 << 16) | (b7 << 8) | b6;
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

inline void RfcBytesToYdbInternal(const ui8* rfc, ui8* ydb) {
    std::array<ui16, 8> dw{};
    for (ui32 i = 0; i < 8; ++i) {
        dw[i] = static_cast<ui16>((rfc[i * 2] << 8) | rfc[i * 2 + 1]);
    }
    std::swap(dw[0], dw[1]);
    for (ui32 i = 4; i < 8; ++i) {
        dw[i] = static_cast<ui16>(((dw[i] >> 8) & 0xff) | ((dw[i] & 0xff) << 8));
    }
    std::memcpy(ydb, dw.data(), NKikimr::NUuid::UUID_LEN);
}

inline void TransformRfcLsbToYdb(ui8* bytes) {
    for (ui32 i = 0; i < 4; ++i) {
        std::swap(bytes[i * 2], bytes[i * 2 + 1]);
    }
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
        | ((prefix & prefixMask) | (tsCode & tsMask));
}

inline ui64 ApplyV7Prefix(ui64 timestampMs, ui64 prefix) {
    return (timestampMs & ~V7TimestampPrefixMask)
        | ((prefix & PrefixMask10) << (48 - PrefixBits));
}

inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV7Bytes(ui64 timestampMs) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfc{};
    FillRandomBytes(rfc.data(), rfc.size());

    rfc[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    rfc[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    rfc[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    rfc[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    rfc[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    rfc[5] = static_cast<ui8>(timestampMs & 0xff);
    rfc[6] = static_cast<ui8>((rfc[6] & 0x0f) | 0x70);
    rfc[8] = static_cast<ui8>((rfc[8] & 0x3f) | 0x80);

    std::array<ui8, NKikimr::NUuid::UUID_LEN> ydb{};
    RfcBytesToYdbInternal(rfc.data(), ydb.data());
    return ydb;
}

inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV8Bytes(ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfc{};
    FillRandomBytes(rfc.data(), rfc.size());

    rfc[6] = static_cast<ui8>((rfc[6] & 0x0f) | 0x80);
    rfc[8] = static_cast<ui8>((rfc[8] & 0x3f) | 0x80);

    ui64 msb = ReadBe64(rfc.data());
    const ui64 lsb = ReadBe64(rfc.data() + 8);
    msb = UpdateMsb(msb, prefix, epochSeconds, hasPrefix);
    msb = ReorderMsb(msb);

    std::array<ui8, NKikimr::NUuid::UUID_LEN> ydb{};
    WriteBe64(msb, ydb.data());
    WriteBe64(lsb, ydb.data() + 8);
    TransformRfcLsbToYdb(ydb.data() + 8);
    return ydb;
}

inline ui64 NewPrefixValue() {
    std::array<ui8, 8> data{};
    FillRandomBytes(data.data(), data.size());
    return ReadBe64(data.data());
}

} // namespace NYql::NUuidKeyGen
