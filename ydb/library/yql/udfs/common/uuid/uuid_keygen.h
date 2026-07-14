#pragma once

#include <yql/essentials/types/uuid/uuid.h>

#include <util/random/random.h>

#include <algorithm>
#include <array>
#include <cstring>

// UUID generators for YDB primary keys.
//
// YDB stores and compares Uuid values as 16 raw bytes in Microsoft GUID
// (mixed-endian) layout — the same order used by memcmp on table keys.
// These functions assemble that internal byte sequence directly; they do
// NOT produce RFC 9562 network-byte-order values.
//
// Consequently, byte order optimized for YDB key sorting differs from what
// you see when casting a Uuid to String in YQL (canonical GUID text) or when
// interpreting the value as a standard RFC v7/v8 UUID elsewhere.

namespace NYql::NUuidKeyGen {

// Number of high bits in the MSB used as a partition-spread prefix (default 10 → ~1024 buckets).
static constexpr ui32 PrefixBits = 10;
static constexpr ui32 V8TimestampBits = 30;
static constexpr ui64 PrefixMsbMask = ((1ULL << PrefixBits) - 1) << (64 - PrefixBits);
static constexpr ui64 PrefixParamMask = (1ULL << PrefixBits) - 1;
static constexpr ui32 V8TimestampShift = 64 - PrefixBits - V8TimestampBits;
static constexpr ui64 V8TimestampMask = ((1ULL << V8TimestampBits) - 1) << V8TimestampShift;

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

// Take the low PrefixBits of the caller-supplied Uint64 and place them
// into the high PrefixBits of the MSB (internal byte layout).
inline ui64 PrefixParamToMsb(ui64 prefix) {
    return (prefix & PrefixParamMask) << (64 - PrefixBits);
}

// Embed Unix epoch seconds (mod 2^30) into the MSB bit field that follows
// the prefix in YDB internal byte order. Field position shifts with PrefixBits
// so prefix and timestamp do not overlap.
inline ui64 GetTimestampCode(ui64 epochSeconds) {
    return (epochSeconds % (1ULL << V8TimestampBits)) << V8TimestampShift;
}

// Merge prefix and timestamp into random MSB bits; LSB stays fully random.
inline ui64 UpdateMsbV8(ui64 msb, ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    const ui64 tsCode = GetTimestampCode(epochSeconds);
    if (hasPrefix) {
        return (msb & ~(PrefixMsbMask | V8TimestampMask))
        | (PrefixParamToMsb(prefix) | (tsCode & V8TimestampMask));
    }
    return (msb & ~V8TimestampMask) | (tsCode & V8TimestampMask);
}

inline ui64 UpdateMsbV7(ui8* result, ui64 prefix) {
    ui64 msb = ReadBe64(result);
    msb = (msb & ~PrefixMsbMask) | PrefixParamToMsb(prefix);
    WriteBe64(msb, result);
    return msb;
}

// Build a time-ordered UUID v7 in YDB internal byte layout.
//
// Sort order (memcmp on stored bytes): timestamp (ms, 48 bits) first, then
// random suffix. Bytes result[0..5] hold the timestamp MSB→LSB so that newer
// keys compare greater when used as a primary key.
//
// This deliberately does NOT match RFC 9562 v7 field order: putting the
// timestamp first in *internal* bytes is what makes ORDER BY / range scans
// chronological in YDB. The canonical string from CAST(Uuid AS String) will
// therefore not show the v7 timestamp in RFC field positions.
//
// Optional prefix (newPrefixV7): overlays the top PrefixBits of the MSB via
// UpdateMsbV7 for pinned-partition batch writes.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV7Bytes(ui64 prefix, ui64 timestampMs, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    // Place timestamp directly into leading internal bytes (YDB mixed-endian MSB).
    // Do not reorder from RFC form — these positions are chosen for key sort order.
    result[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    result[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    result[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    result[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    result[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    result[5] = static_cast<ui8>(timestampMs & 0xff);
    result[7] = static_cast<ui8>((result[7] & 0x0f) | 0x70);
    result[8] = static_cast<ui8>((result[8] & 0x3f) | 0x80);

    if (hasPrefix) {
        UpdateMsbV7(result.data(), prefix);
    }

    return result;
}

// Build a custom "v8" UUID (non-standard version 8) in YDB internal layout.
//
// Sort order (memcmp on stored bytes): (1) short random prefix — top PrefixBits of MSB;
// (2) 30-bit second-granularity timestamp; (3) random suffix in remaining bits.
//
// newV8(): prefix bits are left random from FillRandomBytes → keys spread
// across ~2^PrefixBits partition ranges (horizontal scalability).
// newPrefixV8(p): prefix is fixed → keys from one transaction tend to land
// in the same partition; timestamp still groups rows written at nearby times.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV8Bytes(ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    result[7] = static_cast<ui8>((result[7] & 0x0f) | 0x80);
    result[8] = static_cast<ui8>((result[8] & 0x3f) | 0x80);

    ui64 msb = ReadBe64(result.data());
    msb = UpdateMsbV8(msb, prefix, epochSeconds, hasPrefix);
    WriteBe64(msb, result.data());

    return result;
}

} // namespace NYql::NUuidKeyGen
