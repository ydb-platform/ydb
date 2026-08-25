#pragma once

#include <yql/essentials/types/uuid/uuid.h>

#include <util/generic/maybe.h>
#include <util/random/random.h>

#include <algorithm>
#include <array>
#include <cstring>

// UUID generators for YDB primary keys.
//
// YDB stores and compares Uuid values as 16 raw bytes in Microsoft GUID
// (mixed-endian) layout — the same order used by memcmp on table keys.
// Key generators assemble that internal byte sequence directly; they do
// NOT produce RFC 9562 network-byte-order values (except newV7 helpers,
// which convert RFC → YDB storage after generation).
//
// Row/column key layouts use UUID version 8 (custom / user-defined).

namespace NYql::NUuidKeyGen {

// Row-key layout (YDB internal bytes), per pk_generation RFC:
//   [12 prefix][31 timestamp sec][5 random] + custom_b/ver/var/custom_c
static constexpr ui32 PrefixBits = 12;
static constexpr ui32 TimestampBits = 31;
static constexpr ui64 TimestampModulus = 1ULL << TimestampBits;
static constexpr ui64 PrefixMsbMask = ((1ULL << PrefixBits) - 1) << (64 - PrefixBits);
static constexpr ui64 PrefixParamMask = (1ULL << PrefixBits) - 1;
static constexpr ui32 RowKeyTimestampShift = 64 - PrefixBits - TimestampBits;
static constexpr ui64 RowKeyTimestampMask = ((1ULL << TimestampBits) - 1) << RowKeyTimestampShift;

// Column-key layout (YDB internal bytes):
//   [31 timestamp sec][1 random][16 random] + custom_b/ver/var/custom_c
static constexpr ui32 ColumnKeyTimestampShift = 64 - TimestampBits;
static constexpr ui64 ColumnKeyTimestampMask = ((1ULL << TimestampBits) - 1) << ColumnKeyTimestampShift;

static constexpr ui8 UuidV8VersionByte = 0x80;
static constexpr ui8 UuidV4VersionByte = 0x40;
static constexpr ui8 RfcV7VersionByte = 0x70;

static constexpr ui64 MaxRowGroupCount = 1'000'000;

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

inline void SetUuidVersionAndVariant(ui8* result, ui8 versionByte) {
    result[7] = static_cast<ui8>((result[7] & 0x0f) | versionByte);
    result[8] = static_cast<ui8>((result[8] & 0x3f) | 0x80);
}

inline void SetUuidV8VersionAndVariant(ui8* result) {
    SetUuidVersionAndVariant(result, UuidV8VersionByte);
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

// Embed Unix epoch seconds (mod 2^31) into the row-key MSB bit field that
// follows the prefix. Field position shifts with PrefixBits so prefix and
// timestamp do not overlap.
inline ui64 GetRowKeyTimestampCode(ui64 epochSeconds) {
    return (epochSeconds % TimestampModulus) << RowKeyTimestampShift;
}

inline ui64 GetColumnKeyTimestampCode(ui64 epochSeconds) {
    return (epochSeconds % TimestampModulus) << ColumnKeyTimestampShift;
}

// Merge prefix and timestamp into random MSB bits; remaining bits stay random.
inline ui64 UpdateMsbRowKey(ui64 msb, ui64 prefix, ui64 epochSeconds, bool hasPrefix) {
    const ui64 tsCode = GetRowKeyTimestampCode(epochSeconds);
    if (hasPrefix) {
        return (msb & ~(PrefixMsbMask | RowKeyTimestampMask))
            | (PrefixParamToMsb(prefix) | (tsCode & RowKeyTimestampMask));
    }
    return (msb & ~RowKeyTimestampMask) | (tsCode & RowKeyTimestampMask);
}

inline ui64 UpdateMsbColumnKey(ui64 msb, ui64 epochSeconds) {
    const ui64 tsCode = GetColumnKeyTimestampCode(epochSeconds);
    return (msb & ~ColumnKeyTimestampMask) | (tsCode & ColumnKeyTimestampMask);
}

// Build a row-table key UUID in YDB internal layout (UUIDv8).
//
// Sort order (memcmp on stored bytes): (1) 12-bit random prefix; (2) 31-bit
// second-granularity timestamp; (3) random suffix in remaining bits.
//
// Without an explicit prefix, prefix bits stay random → keys spread across
// ~2^12 partition ranges. With hasPrefix=true, the prefix is fixed (used by
// newV8RowGroup for multi-row transactions).
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeRowKeyUuidBytes(
    ui64 prefix, ui64 epochSeconds, bool hasPrefix)
{
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());
    SetUuidV8VersionAndVariant(result.data());

    ui64 msb = ReadBe64(result.data());
    msb = UpdateMsbRowKey(msb, prefix, epochSeconds, hasPrefix);
    WriteBe64(msb, result.data());

    return result;
}

// Build a column-table key UUID in YDB internal layout (UUIDv8).
//
// Sort order (memcmp on stored bytes): 31-bit second-granularity timestamp
// first, then random suffix. No partition prefix — column tables use hash
// partitioning.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeColumnKeyUuidBytes(ui64 epochSeconds) {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());

    ui64 msb = ReadBe64(result.data());
    msb = UpdateMsbColumnKey(msb, epochSeconds);
    WriteBe64(msb, result.data());
    SetUuidV8VersionAndVariant(result.data());

    return result;
}

// Build a random UUID version 4 in YDB internal storage layout
// (same layout as RandomUuid() / GenUuid4).
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> MakeV4UuidBytes() {
    std::array<ui8, NKikimr::NUuid::UUID_LEN> result{};
    FillRandomBytes(result.data(), result.size());
    SetUuidVersionAndVariant(result.data(), UuidV4VersionByte);
    return result;
}

// Reorder RFC MSB (big-endian) into YDB GUID (Microsoft mixed-endian) storage.
// Byte map: RFC [0 1 2 3 4 5 6 7] → YDB [3 2 1 0 5 4 7 6].
// Involutory: applying twice restores the original value.
// Version nibble (RFC byte 6 high) lands in YDB byte 7 high; LSB/variant
// bytes are not reordered and stay in place.
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

// RFC 9562 (network byte order) → YDB internal storage.
// Expects version/variant already set in the RFC layout (as in MakeRfcV7Bytes);
// only the first 8 bytes are reordered — no post-patch of version/variant.
inline std::array<ui8, NKikimr::NUuid::UUID_LEN> RfcUuidBytesToYdbInternal(const ui8* rfc) {
    ui64 msb = ReadBe64(rfc);
    ui64 lsb = ReadBe64(rfc + 8);
    msb = ReorderRfcMsbToYdb(msb);
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

// Extract the 48-bit Unix timestamp (milliseconds) from an RFC 9562 UUID v7 stored
// in YDB internal byte layout. Returns Nothing() when the version nibble is not 7.
inline TMaybe<ui64> ExtractV7TimestampMicrosFromYdbBytes(const ui8* ydb) {
    const ui64 msb = ReadBe64(ydb);
    const ui64 rfcMsb = ReorderRfcMsbToYdb(msb);
    if (((rfcMsb >> 12) & 0xF) != 7) {
        return Nothing();
    }
    const ui64 timestampMs = (rfcMsb >> 16) & ((1ULL << 48) - 1);
    return timestampMs * 1000;
}

} // namespace NYql::NUuidKeyGen
