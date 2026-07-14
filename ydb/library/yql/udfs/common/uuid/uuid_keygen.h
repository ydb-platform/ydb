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
// interpreting the value as a standard RFC UUID elsewhere.
//
// Both generators set UUID version 8 (implementation-specific per RFC 9562).

namespace NYql::NUuidKeyGen {

// Number of high bits in the MSB used as a partition-spread prefix (default 10 → ~1024 buckets).
static constexpr ui32 PrefixBits = 10;
static constexpr ui32 ShardedTimestampBits = 30;
static constexpr ui64 PrefixMsbMask = ((1ULL << PrefixBits) - 1) << (64 - PrefixBits);
static constexpr ui64 PrefixParamMask = (1ULL << PrefixBits) - 1;
static constexpr ui32 ShardedTimestampShift = 64 - PrefixBits - ShardedTimestampBits;
static constexpr ui64 ShardedTimestampMask = ((1ULL << ShardedTimestampBits) - 1) << ShardedTimestampShift;

static constexpr ui8 UuidVersionByte = 0x80;
static constexpr ui8 RfcV7VersionByte = 0x70;

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

inline void SetUuidVersionAndVariant(ui8* result) {
    result[7] = static_cast<ui8>((result[7] & 0x0f) | UuidVersionByte);
    result[8] = static_cast<ui8>((result[8] & 0x3f) | 0x80);
}

// Take the low PrefixBits of the caller-supplied Uint64 and place them
// into the high PrefixBits of the MSB (internal byte layout).
inline ui64 PrefixParamToMsb(ui64 prefix) {
    return (prefix & PrefixParamMask) << (64 - PrefixBits);
}

// Read the top PrefixBits of the UUID MSB (YDB internal byte layout).
inline ui64 ExtractPrefixFromUuidBytes(const ui8* data) {
    const ui64 msb = ReadBe64(data);
    return (msb & PrefixMsbMask) >> (64 - PrefixBits);
}

// Embed Unix epoch seconds (mod 2^30) into the MSB bit field that follows
// the prefix in YDB internal byte order. Field position shifts with PrefixBits
// so prefix and timestamp do not overlap.
inline ui64 GetTimestampCode(ui64 epochSeconds) {
    return (epochSeconds % (1ULL << ShardedTimestampBits)) << ShardedTimestampShift;
}

// Merge prefix and timestamp into random MSB bits; LSB stays fully random.
inline ui64 UpdateMsbSharded(ui64 msb, ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    const ui64 tsCode = GetTimestampCode(epochSeconds);
    if (hasPrefix) {
        return (msb & ~(PrefixMsbMask | ShardedTimestampMask))
        | (PrefixParamToMsb(prefix) | (tsCode & ShardedTimestampMask));
    }
    return (msb & ~ShardedTimestampMask) | (tsCode & ShardedTimestampMask);
}

inline ui64 UpdateMsbChrono(ui8* result, ui64 prefix) {
    ui64 msb = ReadBe64(result);
    msb = (msb & ~PrefixMsbMask) | PrefixParamToMsb(prefix);
    WriteBe64(msb, result);
    return msb;
}

// Build a chronological key UUID in YDB internal byte layout.
//
// Sort order (memcmp on stored bytes): timestamp (ms, 48 bits) first, then
// random suffix. Bytes result[0..5] hold the timestamp MSB→LSB so that newer
// keys compare greater when used as a primary key.
//
// Timestamp field positions are chosen for YDB key sort order, not for RFC
// field layout. The canonical string from CAST(Uuid AS String) will therefore
// not show the timestamp in standard UUID text positions.
//
// Optional prefix (newChronoPrefix): overlays the top PrefixBits of the MSB via
// UpdateMsbChrono for pinned-partition batch writes.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeChronoUuidBytes(ui64 prefix, ui64 timestampMs, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    // Place timestamp directly into leading internal bytes (YDB mixed-endian MSB).
    result[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    result[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    result[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    result[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    result[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    result[5] = static_cast<ui8>(timestampMs & 0xff);
    SetUuidVersionAndVariant(result.data());

    if (hasPrefix) {
        UpdateMsbChrono(result.data(), prefix);
    }

    return result;
}

// Build a sharded key UUID in YDB internal layout.
//
// Sort order (memcmp on stored bytes): (1) short random prefix — top PrefixBits of MSB;
// (2) 30-bit second-granularity timestamp; (3) random suffix in remaining bits.
//
// newSharded(): prefix bits are left random from FillRandomBytes → keys spread
// across ~2^PrefixBits partition ranges (horizontal scalability).
// newShardedPrefix(p): prefix is fixed → keys from one transaction tend to land
// in the same partition; timestamp still groups rows written at nearby times.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeShardedUuidBytes(ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    SetUuidVersionAndVariant(result.data());

    ui64 msb = ReadBe64(result.data());
    msb = UpdateMsbSharded(msb, prefix, epochSeconds, hasPrefix);
    WriteBe64(msb, result.data());

    return result;
}

// Reorder RFC MSB (big-endian) into YDB GUID (Microsoft mixed-endian) storage.
inline ui64 ReorderRfcMsbToYdb(ui64 msb) {
    const ui64 b0 = (msb >> 56) & 0xff;
    const ui64 b1 = (msb >> 48) & 0xff;
    const ui64 b2 = (msb >> 40) & 0xff;
    const ui64 b3 = (msb >> 32) & 0xff;
    const ui64 b4 = (msb >> 24) & 0xff;
    const ui64 b5 = (msb >> 16) & 0xff;
    const ui64 b6 = (msb >> 8) & 0xff;
    const ui64 b7 = msb & 0xff;
    return (b3 << 56) | (b2 << 48) | (b1 << 40) | (b0 << 32)
        | (b5 << 24) | (b4 << 16) | (b7 << 8) | b6;
}

// reorder() does not place version bits for YDB canonical layout; patch after reorder.
inline ui64 UpdateRfcV7MsbVersionForYdb(ui64 msb) {
    return (msb & ~0xF000ULL) | 0x7000ULL;
}

inline ui64 UpdateRfcLsbVariantForYdb(ui64 lsb) {
    return (lsb & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
}

// RFC 9562 (network byte order) → YDB internal storage.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> RfcUuidBytesToYdbInternal(const ui8* rfc) {
    ui64 msb = ReadBe64(rfc);
    ui64 lsb = ReadBe64(rfc + 8);
    msb = ReorderRfcMsbToYdb(msb);
    msb = UpdateRfcV7MsbVersionForYdb(msb);
    lsb = UpdateRfcLsbVariantForYdb(lsb);
    std::array<ui8, NKikimr::NUuid::UUID_LEN> ydb{};
    WriteBe64(msb, ydb.data());
    WriteBe64(lsb, ydb.data() + 8);
    return ydb;
}

// Build an RFC 9562 UUID v7 in network byte order.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeRfcV7Bytes(ui64 timestampMs) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> uuid{};
    FillRandomBytes(uuid.data(), uuid.size());

    uuid[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    uuid[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    uuid[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    uuid[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    uuid[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    uuid[5] = static_cast<ui8>(timestampMs & 0xff);
    uuid[6] = static_cast<ui8>((uuid[6] & 0x0f) | RfcV7VersionByte);
    uuid[8] = static_cast<ui8>((uuid[8] & 0x3f) | 0x80);

    return uuid;
}

// Build an RFC 9562 UUID v7 and convert it to YDB internal storage layout.
// Sort order in YDB keys will not follow creation time; use for external interoperability.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeRfcV7YdbBytes(ui64 timestampMs) {
    const auto rfc = MakeRfcV7Bytes(timestampMs);
    return RfcUuidBytesToYdbInternal(rfc.data());
}

} // namespace NYql::NUuidKeyGen
