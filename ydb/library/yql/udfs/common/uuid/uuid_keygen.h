#pragma once

#include <yql/essentials/types/uuid/uuid.h>

#include <util/random/random.h>

#include <algorithm>
#include <array>
#include <cstring>

namespace NYql::NUuidKeyGen {

static constexpr ui32 PrefixBits = 10;
static constexpr ui32 MaskPos = PrefixBits - 1;
static constexpr ui64 PrefixMask64 = 0xFFC0000000000000ULL;
static constexpr ui64 V8TimestampMask = 0x003FFFFFFF000000ULL;
static constexpr ui32 V8TimestampBits = 30;
static constexpr ui32 V8TimestampFieldLowBit = 33;

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
        std::memcpy(data + offset, &random, std::min(size - offset, sizeof(ui64)));
    }
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
