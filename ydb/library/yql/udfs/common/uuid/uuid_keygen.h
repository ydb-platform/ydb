#pragma once

#include <yql/essentials/types/uuid/uuid.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>

#include <array>
#include <cstring>

namespace NYql::NUuidKeyGen {

static constexpr ui32 PrefixBits = 10;
static constexpr ui32 MaskPos = PrefixBits - 1;
static constexpr ui32 PrefixMask32 = (1U << PrefixBits) - 1U;
static constexpr ui64 PrefixMask64 = 0xFFC0000000000000ULL;
static constexpr ui64 V8TimestampMask = 0x003FFFFFFF000000ULL;
static constexpr ui64 V7TimestampPrefixMask = static_cast<ui64>(PrefixMask32) << (48 - PrefixBits);
static constexpr ui32 V8TimestampBits = 30;
static constexpr ui32 V8TimestampFieldLowBit = 33;

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

inline ui64 ByteSwap64(ui64 value) {
    ui64 result = 0;
    for (ui32 i = 0; i < 8; ++i) {
        result = (result << 8) | ((value >> (8 * i)) & 0xff);
    }
    return result;
}

inline ui64 JavaUuidMsbToLow128(ui64 javaMsb) {
    return ((javaMsb & 0xffffffff00000000ULL) >> 32)
        | ((javaMsb & 0x00000000ffff0000ULL) << 16)
        | ((javaMsb & 0x000000000000ffffULL) << 48);
}

inline void FillRandomBytes(ui8* data, size_t size) {
    for (size_t offset = 0; offset < size; offset += sizeof(ui64)) {
        const ui64 random = RandomNumber<ui64>();
        std::memcpy(data + offset, &random, std::min(size - offset, sizeof(ui64)));
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

inline void JavaUuidToYdbStorageBytes(ui64 javaMsb, ui64 javaLsb, ui8* internalBytes) {
    const ui64 low128 = JavaUuidMsbToLow128(javaMsb);
    const ui64 hi128 = ByteSwap64(javaLsb);
    NKikimr::NUuid::UuidHalfsToBytes(
        reinterpret_cast<char*>(internalBytes),
        NKikimr::NUuid::UUID_LEN,
        hi128,
        low128);
}

inline ui64 GetTimestampCode(ui64 epochSeconds) {
    return (epochSeconds % (1ULL << V8TimestampBits))
        << (V8TimestampFieldLowBit - MaskPos);
}

inline ui64 UpdateMsbV8(ui64 msb, ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    const ui64 tsCode = GetTimestampCode(epochSeconds);
    if (hasPrefix) {
        return (msb & ~(PrefixMask64 | V8TimestampMask))
        | ((prefix & PrefixMask64) | (tsCode & V8TimestampMask));
    }
    return (msb & ~V8TimestampMask) | (tsCode & V8TimestampMask);
}

inline ui64 UpdateMsbV7(ui8* result, ui64 prefix) {
    ui64 msb = ReadBe64(result);
    msb = (msb & ~PrefixMask64) | (prefix & PrefixMask64);
    WriteBe64(msb, result);
    return msb;
}

// UUID V7 optimized for YDB internal layout
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV7Bytes(ui64 prefix, ui64 timestampMs, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    result[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    result[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    result[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    result[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    result[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    result[5] = static_cast<ui8>(timestampMs & 0xff);
    result[6] = static_cast<ui8>((result[6] & 0x0f) | 0x70);
    result[8] = static_cast<ui8>((result[8] & 0x3f) | 0x80);

    if (hasPrefix) {
        UpdateMsbV7(result.data(), prefix);
    }

    return result;
}

// UUID V8 (custom) optimized for YDB internal layout
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV8Bytes(ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    result[6] = static_cast<ui8>((result[6] & 0x0f) | 0x80);
    result[8] = static_cast<ui8>((result[8] & 0x3f) | 0x80);

    ui64 msb = ReadBe64(result.data());
    msb = UpdateMsbV8(msb, prefix, epochSeconds, hasPrefix);
    WriteBe64(msb, result.data());

    return result;
}

} // namespace NYql::NUuidKeyGen
