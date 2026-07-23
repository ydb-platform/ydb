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

constexpr ui64 kTestChronoPrefix = 0x294ULL;
constexpr ui64 kTestShardedPrefix = 0x294ULL;
constexpr ui64 kSmallPrefix = 3ULL;

int CompareUuidBytes(const TUuidBytes& lhs, const TUuidBytes& rhs) {
    return std::memcmp(lhs.data(), rhs.data(), rhs.size());
}

TString UuidBytesToDisplayString(const TUuidBytes& bytes) {
    return NKikimr::NUuid::UuidBytesToString(TString(reinterpret_cast<const char*>(bytes.data()), bytes.size()));
}

void AssertUuidStringHasCanonicalDashes(TStringBuf uuidString) {
    UNIT_ASSERT_C(uuidString.size() == 36, "Expected canonical UUID string length 36, got: " << uuidString);
    UNIT_ASSERT_C(uuidString[8] == '-', "Expected dash at position 9, got: " << uuidString);
    UNIT_ASSERT_C(uuidString[13] == '-', "Expected dash at position 14, got: " << uuidString);
    UNIT_ASSERT_C(uuidString[18] == '-', "Expected dash at position 19, got: " << uuidString);
    UNIT_ASSERT_C(uuidString[23] == '-', "Expected dash at position 24, got: " << uuidString);
}

void AssertUuidStringFormat(TStringBuf uuidString) {
    AssertUuidStringHasCanonicalDashes(uuidString);
    UNIT_ASSERT_C(uuidString[14] == '8',
        "Expected version digit '8' at position 15, got '" << uuidString[14]
        << "' in " << uuidString);
    const char variantDigit = uuidString[19];
    UNIT_ASSERT_C(variantDigit == '8' || variantDigit == '9' || variantDigit == 'a' || variantDigit == 'b',
        "Expected RFC variant digit at position 20, got '" << variantDigit << "' in " << uuidString);
}

void AssertRfcV7StringFormat(TStringBuf uuidString) {
    AssertUuidStringHasCanonicalDashes(uuidString);
    UNIT_ASSERT_C(uuidString[14] == '7',
        "Expected version digit '7' at position 15, got '" << uuidString[14]
        << "' in " << uuidString);
    const char variantDigit = uuidString[19];
    UNIT_ASSERT_C(variantDigit == '8' || variantDigit == '9' || variantDigit == 'a' || variantDigit == 'b',
        "Expected RFC variant digit at position 20, got '" << variantDigit << "' in " << uuidString);
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

TUuidBytes GenerateChronoWithFixedRandom(ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeChronoUuidBytes(0, timestampMs, false);
}

TUuidBytes GenerateChronoWithPrefixAndFixedRandom(ui64 prefix, ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeChronoUuidBytes(prefix, timestampMs, true);
}

TUuidBytes GenerateShardedWithFixedRandom(ui64 prefix, ui64 epochSeconds) {
    SetRandomSeed(42);
    return MakeShardedUuidBytes(prefix, epochSeconds, true);
}

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(ChronoUsesBottom10PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 expectedParam = rawPrefix & PrefixParamMask;  // 0x11
        const ui64 timestampMs = 1'700'000'000'000ULL;
        SetRandomSeed(77);
        const auto chronoFromRaw = MakeChronoUuidBytes(rawPrefix, timestampMs, true);
        SetRandomSeed(77);
        const auto chronoFromBottomBits = MakeChronoUuidBytes(expectedParam, timestampMs, true);
        UNIT_ASSERT_VALUES_EQUAL(chronoFromRaw, chronoFromBottomBits);
    }

    Y_UNIT_TEST(ShardedUsesBottom10PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 expectedParam = rawPrefix & PrefixParamMask;  // 0x11
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(77);
        const auto shardedFromRaw = MakeShardedUuidBytes(rawPrefix, epochSeconds, true);
        SetRandomSeed(77);
        const auto shardedFromBottomBits = MakeShardedUuidBytes(expectedParam, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(shardedFromRaw, shardedFromBottomBits);
        SetRandomSeed(88);
        const auto shardedWithSmallPrefix = MakeShardedUuidBytes(kSmallPrefix, epochSeconds, true);
        SetRandomSeed(88);
        const auto shardedWithZeroPrefix = MakeShardedUuidBytes(0, epochSeconds, true);
        UNIT_ASSERT_VALUES_UNEQUAL(shardedWithSmallPrefix, shardedWithZeroPrefix);
    }

    Y_UNIT_TEST(ChronoSortOrderAtTimestampBoundary) {
        const ui64 earlierTimestampMs = 0x00FFFFFFULL;
        const ui64 laterTimestampMs = 0x01000000ULL;

        SetRandomSeed(16180);
        const auto earlierGenerated = MakeChronoUuidBytes(0, earlierTimestampMs, false);
        SetRandomSeed(16180);
        const auto laterGenerated = MakeChronoUuidBytes(0, laterTimestampMs, false);

        UNIT_ASSERT(CompareUuidBytes(earlierGenerated, laterGenerated) < 0);
    }

    Y_UNIT_TEST(ChronoSortOrderWithoutPrefixFixedRandom) {
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateChronoWithFixedRandom(baseTimestampMs + i * 2));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(ChronoSortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateChronoWithPrefixAndFixedRandom(kTestChronoPrefix, baseTimestampMs + i * 2));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(ShardedSortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(3);

        for (ui32 i = 0; i < 3; ++i) {
            generated.push_back(GenerateShardedWithFixedRandom(kTestShardedPrefix, baseEpochSeconds + i));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(ChronoSortOrderWithoutPrefixVaryingRandom) {
        SetRandomSeed(12345);
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeChronoUuidBytes(0, baseTimestampMs + i * 2, false));
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ChronoSortOrderWithPrefixVaryingRandom) {
        SetRandomSeed(54321);
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeChronoUuidBytes(kTestChronoPrefix, baseTimestampMs + i * 2, true));
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ChronoDistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(98765);
        const ui64 timestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeChronoUuidBytes(0, timestampMs, false));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ChronoDistinctRandomSuffixAtSameTimestampWithPrefix) {
        SetRandomSeed(13579);
        const ui64 prefix = 0x1FFULL;
        const ui64 timestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeChronoUuidBytes(prefix, timestampMs, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ShardedDistinctAcrossTimestampsWithVaryingRandom) {
        SetRandomSeed(24680);
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeShardedUuidBytes(kTestShardedPrefix, baseEpochSeconds + i, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ShardedDistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(112233);
        const ui64 epochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeShardedUuidBytes(kTestShardedPrefix, epochSeconds, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ChronoStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeChronoUuidBytes(0, MilliSeconds(), false);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(ChronoPrefixStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeChronoUuidBytes(kTestChronoPrefix, MilliSeconds(), true);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(ShardedStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeShardedUuidBytes(0, MilliSeconds() / 1000, false);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(ShardedPrefixStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeShardedUuidBytes(kTestShardedPrefix, MilliSeconds() / 1000, true);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(ExtractPrefixFromUuidBytesMatchesUint64Prefix) {
        const ui64 prefixParam = kTestChronoPrefix;
        const ui64 timestampMs = 1'700'000'000'000ULL;
        SetRandomSeed(42);
        const auto withUint64Prefix = MakeChronoUuidBytes(prefixParam, timestampMs, true);

        const ui64 extractedPrefix = ExtractPrefixFromUuidBytes(withUint64Prefix.data());
        UNIT_ASSERT_VALUES_EQUAL(extractedPrefix, prefixParam & PrefixParamMask);

        SetRandomSeed(42);
        const auto withUuidPrefix = MakeChronoUuidBytes(extractedPrefix, timestampMs, true);
        UNIT_ASSERT_VALUES_EQUAL(withUint64Prefix, withUuidPrefix);
    }

    Y_UNIT_TEST(ChronoUuidPrefixMatchesUint64Prefix) {
        const ui64 prefixParam = kTestChronoPrefix;
        const ui64 timestampMs = 1'700'000'000'000ULL;
        SetRandomSeed(55);
        const auto prefixUuid = MakeChronoUuidBytes(prefixParam, timestampMs, true);

        SetRandomSeed(55);
        const auto fromUint64 = MakeChronoUuidBytes(prefixParam, timestampMs, true);
        SetRandomSeed(55);
        const auto fromUuid = MakeChronoUuidBytes(
            ExtractPrefixFromUuidBytes(prefixUuid.data()), timestampMs, true);
        UNIT_ASSERT_VALUES_EQUAL(fromUint64, fromUuid);
    }

    Y_UNIT_TEST(ShardedUuidPrefixMatchesUint64Prefix) {
        const ui64 prefixParam = kTestShardedPrefix;
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(66);
        const auto prefixUuid = MakeShardedUuidBytes(prefixParam, epochSeconds, true);

        SetRandomSeed(66);
        const auto fromUint64 = MakeShardedUuidBytes(prefixParam, epochSeconds, true);
        SetRandomSeed(66);
        const auto fromUuid = MakeShardedUuidBytes(
            ExtractPrefixFromUuidBytes(prefixUuid.data()), epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(fromUint64, fromUuid);
    }

    Y_UNIT_TEST(RfcV7StringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeRfcV7YdbBytes(MilliSeconds());
        AssertRfcV7StringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(RfcV7AtUsesFixedTimestamp) {
        const ui64 timestampMs = 1'700'000'000'123ULL;
        SetRandomSeed(42);
        const auto first = MakeRfcV7YdbBytes(timestampMs);
        SetRandomSeed(42);
        const auto second = MakeRfcV7YdbBytes(timestampMs);
        UNIT_ASSERT_VALUES_EQUAL(first, second);
        AssertRfcV7StringFormat(UuidBytesToDisplayString(first));
    }

    Y_UNIT_TEST(RfcV7DiffersFromChronoForSameTimestamp) {
        const ui64 timestampMs = 1'700'000'000'123ULL;
        SetRandomSeed(42);
        const auto rfcV7 = MakeRfcV7YdbBytes(timestampMs);
        SetRandomSeed(42);
        const auto chrono = MakeChronoUuidBytes(0, timestampMs, false);
        UNIT_ASSERT_VALUES_UNEQUAL(rfcV7, chrono);
    }

    Y_UNIT_TEST(ReorderRfcMsbMatchesExpectations) {
        const ui64 msb = ReadBe64(std::array<ui8, 8>{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}.data());
        const ui64 reordered = ReorderRfcMsbToYdb(msb);
        std::array<ui8, 8> bytes{};
        WriteBe64(reordered, bytes.data());
        UNIT_ASSERT_VALUES_EQUAL(bytes[0], 0x04);
        UNIT_ASSERT_VALUES_EQUAL(bytes[1], 0x03);
        UNIT_ASSERT_VALUES_EQUAL(bytes[2], 0x02);
        UNIT_ASSERT_VALUES_EQUAL(bytes[3], 0x01);
        UNIT_ASSERT_VALUES_EQUAL(bytes[4], 0x06);
        UNIT_ASSERT_VALUES_EQUAL(bytes[5], 0x05);
        UNIT_ASSERT_VALUES_EQUAL(bytes[6], 0x08);
        UNIT_ASSERT_VALUES_EQUAL(bytes[7], 0x07);
    }

    Y_UNIT_TEST(ExtractV7TimestampRoundtrip) {
        const ui64 timestampMs = 1'700'000'000'123ULL;
        const ui64 expectedMicros = timestampMs * 1000;
        SetRandomSeed(42);
        const auto bytes = MakeRfcV7YdbBytes(timestampMs);
        const auto extracted = ExtractV7TimestampMicrosFromYdbBytes(bytes.data());
        UNIT_ASSERT(extracted.Defined());
        UNIT_ASSERT_VALUES_EQUAL(*extracted, expectedMicros);
    }

    Y_UNIT_TEST(ExtractV7TimestampReturnsNothingForChrono) {
        SetRandomSeed(42);
        const auto bytes = MakeChronoUuidBytes(0, MilliSeconds(), false);
        const auto extracted = ExtractV7TimestampMicrosFromYdbBytes(bytes.data());
        UNIT_ASSERT(!extracted.Defined());
    }

    Y_UNIT_TEST(ExtractV7TimestampReturnsNothingForSharded) {
        SetRandomSeed(42);
        const auto bytes = MakeShardedUuidBytes(0, MilliSeconds() / 1000, false);
        const auto extracted = ExtractV7TimestampMicrosFromYdbBytes(bytes.data());
        UNIT_ASSERT(!extracted.Defined());
    }

    Y_UNIT_TEST(ExtractV7TimestampFromParsedUuidString) {
        const ui64 timestampMs = 1'700'000'000'456ULL;
        SetRandomSeed(42);
        const auto bytes = MakeRfcV7YdbBytes(timestampMs);
        const TString uuidString = UuidBytesToDisplayString(bytes);

        ui16 dw[8] = {};
        UNIT_ASSERT(NKikimr::NUuid::ParseUuidToArray(uuidString, dw, false));
        std::array<ui8, NKikimr::NUuid::UUID_LEN> parsed{};
        std::memcpy(parsed.data(), dw, sizeof(dw));

        const auto extracted = ExtractV7TimestampMicrosFromYdbBytes(parsed.data());
        UNIT_ASSERT(extracted.Defined());
        UNIT_ASSERT_VALUES_EQUAL(*extracted, timestampMs * 1000);
    }
}
