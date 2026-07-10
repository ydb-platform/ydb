#include <ydb/library/yql/udfs/common/uuid/uuid_keygen.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>
#include <util/system/datetime.h>

#include <array>
#include <cstring>
#include <unordered_set>
#include <vector>

using namespace NYql::NUuidKeyGen;

namespace {

using TUuidBytes = std::array<ui8, NKikimr::NUuid::UUID_LEN>;

constexpr ui64 kTestPrefix = 0x2A5ULL;
constexpr ui64 kSmallPrefix = 3ULL;

int CompareUuidBytes(const TUuidBytes& lhs, const TUuidBytes& rhs) {
    return std::memcmp(lhs.data(), rhs.data(), rhs.size());
}

TString UuidBytesToHex(const TUuidBytes& bytes) {
    static const char hex[] = "0123456789abcdef";
    TString result;
    result.reserve(bytes.size() * 2);
    for (const ui8 byte : bytes) {
        result.append(hex[byte >> 4]);
        result.append(hex[byte & 0x0f]);
    }
    return result;
}

void AssertGenerationOrderIsSortOrder(const TVector<TUuidBytes>& generated) {
    UNIT_ASSERT(generated.size() >= 2);

    for (size_t i = 1; i < generated.size(); ++i) {
        UNIT_ASSERT_C(CompareUuidBytes(generated[i - 1], generated[i]) < 0,
            "UUID at index " << i - 1 << " must sort before UUID at index " << i
            << " in YDB byte order");
    }
}

void AssertAllDistinct(const TVector<TUuidBytes>& generated) {
    std::unordered_set<TString> seen;
    seen.reserve(generated.size());
    for (const auto& uuid : generated) {
        const TString hex = UuidBytesToHex(uuid);
        UNIT_ASSERT_C(seen.insert(hex).second,
            "Expected distinct UUID bytes, got duplicate: " << hex);
    }
}

TUuidBytes GenerateV7WithFixedRandom(ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeV7Bytes(timestampMs);
}

TUuidBytes GenerateV7WithPrefixAndFixedRandom(ui64 prefix, ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeV7Bytes(ApplyV7Prefix(timestampMs, prefix));
}

TUuidBytes GenerateV8WithFixedRandom(ui64 prefix, ui64 epochSeconds) {
    SetRandomSeed(42);
    return MakeV8Bytes(prefix, epochSeconds, true);
}

TUuidBytes BuildV7ViaParseUuidToArray(ui64 timestampMs, ui32 seed) {
    SetRandomSeed(seed);
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
    RfcLayoutToYdbInternalBytes(rfc.data(), ydb.data());
    return ydb;
}

TUuidBytes BuildV8ViaParseUuidToArray(ui64 prefix, ui64 epochSeconds, ui32 seed) {
    SetRandomSeed(seed);
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfc{};
    FillRandomBytes(rfc.data(), rfc.size());
    rfc[6] = static_cast<ui8>((rfc[6] & 0x0f) | 0x80);
    rfc[8] = static_cast<ui8>((rfc[8] & 0x3f) | 0x80);

    ui64 msb = ReadBe64(rfc.data());
    msb = UpdateMsb(msb, prefix, epochSeconds, true);
    WriteBe64(msb, rfc.data());

    std::array<ui8, NKikimr::NUuid::UUID_LEN> ydb{};
    RfcLayoutToYdbInternalBytes(rfc.data(), ydb.data());
    return ydb;
}

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(V7AndV8UseBottom10PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 timestampMs = 1'700'000'000'000ULL;
        const ui64 epochSeconds = timestampMs / 1000;

        UNIT_ASSERT_VALUES_EQUAL(
            ApplyV7Prefix(timestampMs, rawPrefix),
            ApplyV7Prefix(timestampMs, rawPrefix & PrefixMask10));

        SetRandomSeed(77);
        const auto v8FromRaw = MakeV8Bytes(rawPrefix, epochSeconds, true);
        SetRandomSeed(77);
        const auto v8FromBottomBits = MakeV8Bytes(rawPrefix & PrefixMask10, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(v8FromRaw, v8FromBottomBits);

        SetRandomSeed(88);
        const auto v8WithSmallPrefix = MakeV8Bytes(kSmallPrefix, epochSeconds, true);
        SetRandomSeed(88);
        const auto v8WithoutPrefix = MakeV8Bytes(0, epochSeconds, true);
        UNIT_ASSERT(CompareUuidBytes(v8WithSmallPrefix, v8WithoutPrefix) != 0);
    }

    Y_UNIT_TEST(V7MatchesYdbInternalLayout) {
        const ui64 timestampMs = 1'700'000'123'456ULL;
        SetRandomSeed(31415);
        const auto generated = MakeV7Bytes(timestampMs);
        const auto viaParse = BuildV7ViaParseUuidToArray(timestampMs, 31415);
        UNIT_ASSERT_VALUES_EQUAL(generated, viaParse);
    }

    Y_UNIT_TEST(V8MatchesParseUuidToArrayInternalLayout) {
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(27182);
        const auto generated = MakeV8Bytes(kTestPrefix, epochSeconds, true);
        const auto viaParse = BuildV8ViaParseUuidToArray(kTestPrefix, epochSeconds, 27182);
        UNIT_ASSERT_VALUES_EQUAL(generated, viaParse);
    }

    Y_UNIT_TEST(V7MixedEndianSortOrderMatchesParseUuidToArrayAtBoundary) {
        // At this timestamp boundary RFC chronological order and YDB memcmp order diverge.
        // Generators must follow ParseUuidToArray (mixed-endian), not RFC byte order.
        const ui64 earlierTimestampMs = 0x00FFFFFFULL;
        const ui64 laterTimestampMs = 0x01000000ULL;

        SetRandomSeed(16180);
        const auto earlierGenerated = MakeV7Bytes(earlierTimestampMs);
        SetRandomSeed(16180);
        const auto laterGenerated = MakeV7Bytes(laterTimestampMs);

        SetRandomSeed(16180);
        const auto earlierViaParse = BuildV7ViaParseUuidToArray(earlierTimestampMs, 16180);
        SetRandomSeed(16180);
        const auto laterViaParse = BuildV7ViaParseUuidToArray(laterTimestampMs, 16180);

        UNIT_ASSERT_VALUES_EQUAL(earlierGenerated, earlierViaParse);
        UNIT_ASSERT_VALUES_EQUAL(laterGenerated, laterViaParse);
        UNIT_ASSERT_VALUES_EQUAL(
            CompareUuidBytes(earlierGenerated, laterGenerated),
            CompareUuidBytes(earlierViaParse, laterViaParse));
    }

    Y_UNIT_TEST(V8MatchesYdbInternalLayout) {
        SetRandomSeed(4242);
        const auto v8 = MakeV8Bytes(kTestPrefix, 1'700'000'000ULL, true);

        SetRandomSeed(4242);
        std::array<ui8, NKikimr::NUuid::UUID_LEN> rfcLayout{};
        FillRandomBytes(rfcLayout.data(), rfcLayout.size());
        rfcLayout[6] = static_cast<ui8>((rfcLayout[6] & 0x0f) | 0x80);
        rfcLayout[8] = static_cast<ui8>((rfcLayout[8] & 0x3f) | 0x80);

        ui64 msb = ReadBe64(rfcLayout.data());
        msb = UpdateMsb(msb, kTestPrefix, 1'700'000'000ULL, true);
        WriteBe64(msb, rfcLayout.data());

        std::array<ui8, NKikimr::NUuid::UUID_LEN> expected{};
        RfcLayoutToYdbInternalBytes(rfcLayout.data(), expected.data());

        UNIT_ASSERT_VALUES_EQUAL(v8, expected);
    }

    Y_UNIT_TEST(V7SortOrderWithoutPrefixFixedRandom) {
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateV7WithFixedRandom(baseTimestampMs + i * 2));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateV7WithPrefixAndFixedRandom(kTestPrefix, baseTimestampMs + i * 2));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V8SortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(3);

        for (ui32 i = 0; i < 3; ++i) {
            generated.push_back(GenerateV8WithFixedRandom(kTestPrefix, baseEpochSeconds + i));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithoutPrefixVaryingRandom) {
        SetRandomSeed(12345);
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV7Bytes(baseTimestampMs + i * 2));
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithPrefixVaryingRandom) {
        SetRandomSeed(54321);
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV7Bytes(ApplyV7Prefix(baseTimestampMs + i * 2, kTestPrefix)));
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V7DistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(98765);
        const ui64 timestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeV7Bytes(timestampMs));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V7DistinctRandomSuffixAtSameTimestampWithPrefix) {
        SetRandomSeed(13579);
        const ui64 prefix = 0x1FFULL;
        const ui64 timestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeV7Bytes(ApplyV7Prefix(timestampMs, prefix)));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V8DistinctAcrossTimestampsWithVaryingRandom) {
        SetRandomSeed(24680);
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV8Bytes(kTestPrefix, baseEpochSeconds + i, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V8DistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(112233);
        const ui64 epochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeV8Bytes(kTestPrefix, epochSeconds, true));
        }

        AssertAllDistinct(generated);
    }
}
