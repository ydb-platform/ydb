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

constexpr ui64 kTestRowPrefix = 0xA94ULL; // fits in 12 bits
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

void AssertUuidV8StringFormat(TStringBuf uuidString) {
    AssertUuidStringHasCanonicalDashes(uuidString);
    UNIT_ASSERT_C(uuidString[14] == '8',
        "Expected version digit '8' at position 15, got '" << uuidString[14]
        << "' in " << uuidString);
    const char variantDigit = uuidString[19];
    UNIT_ASSERT_C(variantDigit == '8' || variantDigit == '9' || variantDigit == 'a' || variantDigit == 'b',
        "Expected RFC variant digit at position 20, got '" << variantDigit << "' in " << uuidString);
}

void AssertUuidV4StringFormat(TStringBuf uuidString) {
    AssertUuidStringHasCanonicalDashes(uuidString);
    UNIT_ASSERT_C(uuidString[14] == '4',
        "Expected version digit '4' at position 15, got '" << uuidString[14]
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

TUuidBytes GenerateColumnKeyWithFixedRandom(ui64 epochSeconds) {
    SetRandomSeed(42);
    return MakeColumnKeyUuidBytes(epochSeconds);
}

TUuidBytes GenerateRowKeyWithFixedRandom(ui64 prefix, ui64 epochSeconds) {
    SetRandomSeed(42);
    return MakeRowKeyUuidBytes(prefix, epochSeconds, true);
}

ui64 ExtractEmbeddedRowTimestamp(const TUuidBytes& bytes) {
    const ui64 msb = ReadBe64(bytes.data());
    return (msb & RowKeyTimestampMask) >> RowKeyTimestampShift;
}

ui64 ExtractEmbeddedColumnTimestamp(const TUuidBytes& bytes) {
    const ui64 msb = ReadBe64(bytes.data());
    return (msb & ColumnKeyTimestampMask) >> ColumnKeyTimestampShift;
}

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(RowKeyUsesBottom12PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0A94ULL;
        const ui64 expectedParam = rawPrefix & PrefixParamMask; // 0xA94
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(77);
        const auto fromRaw = MakeRowKeyUuidBytes(rawPrefix, epochSeconds, true);
        SetRandomSeed(77);
        const auto fromBottomBits = MakeRowKeyUuidBytes(expectedParam, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(fromRaw, fromBottomBits);
        UNIT_ASSERT_VALUES_EQUAL(ExtractPrefixFromUuidBytes(fromRaw.data()), expectedParam);

        SetRandomSeed(88);
        const auto withSmallPrefix = MakeRowKeyUuidBytes(kSmallPrefix, epochSeconds, true);
        SetRandomSeed(88);
        const auto withZeroPrefix = MakeRowKeyUuidBytes(0, epochSeconds, true);
        UNIT_ASSERT_VALUES_UNEQUAL(withSmallPrefix, withZeroPrefix);
    }

    Y_UNIT_TEST(RowKeyEmbeds31BitSecondTimestamp) {
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(42);
        const auto bytes = MakeRowKeyUuidBytes(kTestRowPrefix, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(
            ExtractEmbeddedRowTimestamp(bytes),
            epochSeconds % TimestampModulus);
    }

    Y_UNIT_TEST(ColumnKeyEmbeds31BitSecondTimestamp) {
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(42);
        const auto bytes = MakeColumnKeyUuidBytes(epochSeconds);
        UNIT_ASSERT_VALUES_EQUAL(
            ExtractEmbeddedColumnTimestamp(bytes),
            epochSeconds % TimestampModulus);
    }

    Y_UNIT_TEST(ColumnKeySortOrderAtTimestampBoundary) {
        const ui64 earlier = 0x00FFFFFFULL;
        const ui64 later = 0x01000000ULL;

        SetRandomSeed(16180);
        const auto earlierGenerated = MakeColumnKeyUuidBytes(earlier);
        SetRandomSeed(16180);
        const auto laterGenerated = MakeColumnKeyUuidBytes(later);

        UNIT_ASSERT(CompareUuidBytes(earlierGenerated, laterGenerated) < 0);
    }

    Y_UNIT_TEST(ColumnKeySortOrderFixedRandom) {
        const ui64 baseEpochSeconds = Seconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateColumnKeyWithFixedRandom(baseEpochSeconds + i));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(RowKeySortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 baseEpochSeconds = Seconds();
        TVector<TUuidBytes> generated;
        generated.reserve(3);

        for (ui32 i = 0; i < 3; ++i) {
            generated.push_back(GenerateRowKeyWithFixedRandom(kTestRowPrefix, baseEpochSeconds + i));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(ColumnKeySortOrderVaryingRandom) {
        SetRandomSeed(12345);
        const ui64 baseEpochSeconds = Seconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeColumnKeyUuidBytes(baseEpochSeconds + i));
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(RowKeySortOrderWithPrefixVaryingRandom) {
        SetRandomSeed(54321);
        const ui64 baseEpochSeconds = Seconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeRowKeyUuidBytes(kTestRowPrefix, baseEpochSeconds + i, true));
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(ColumnKeyDistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(98765);
        const ui64 epochSeconds = Seconds();
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeColumnKeyUuidBytes(epochSeconds));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(RowKeyDistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(112233);
        const ui64 epochSeconds = Seconds();
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeRowKeyUuidBytes(kTestRowPrefix, epochSeconds, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(RowKeyWithoutPrefixUsesRandomPrefixBits) {
        SetRandomSeed(24680);
        const ui64 epochSeconds = Seconds();
        std::unordered_set<ui64> prefixes;

        for (ui32 i = 0; i < 50; ++i) {
            const auto bytes = MakeRowKeyUuidBytes(0, epochSeconds, false);
            prefixes.insert(ExtractPrefixFromUuidBytes(bytes.data()));
        }

        UNIT_ASSERT_C(prefixes.size() > 1, "Expected varying random prefixes");
    }

    Y_UNIT_TEST(ColumnKeyStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeColumnKeyUuidBytes(Seconds());
        AssertUuidV8StringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(RowKeyStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeRowKeyUuidBytes(0, Seconds(), false);
        AssertUuidV8StringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(RowKeyPrefixStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeRowKeyUuidBytes(kTestRowPrefix, Seconds(), true);
        AssertUuidV8StringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(V4StringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeV4UuidBytes();
        AssertUuidV4StringFormat(UuidBytesToDisplayString(bytes));
    }

    Y_UNIT_TEST(ExtractPrefixFromUuidBytesMatchesUint64Prefix) {
        const ui64 prefixParam = kTestRowPrefix;
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(42);
        const auto withUint64Prefix = MakeRowKeyUuidBytes(prefixParam, epochSeconds, true);

        const ui64 extractedPrefix = ExtractPrefixFromUuidBytes(withUint64Prefix.data());
        UNIT_ASSERT_VALUES_EQUAL(extractedPrefix, prefixParam & PrefixParamMask);

        SetRandomSeed(42);
        const auto withUuidPrefix = MakeRowKeyUuidBytes(extractedPrefix, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(withUint64Prefix, withUuidPrefix);
    }

    Y_UNIT_TEST(RowGroupSharedPrefixMatchesUint64Prefix) {
        const ui64 prefixParam = kTestRowPrefix;
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(55);
        const auto prefixUuid = MakeRowKeyUuidBytes(prefixParam, epochSeconds, true);

        SetRandomSeed(55);
        const auto fromUint64 = MakeRowKeyUuidBytes(prefixParam, epochSeconds, true);
        SetRandomSeed(55);
        const auto fromUuid = MakeRowKeyUuidBytes(
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

    Y_UNIT_TEST(RfcV7DiffersFromColumnKeyForSameSecond) {
        const ui64 epochSeconds = 1'700'000'000ULL;
        const ui64 timestampMs = epochSeconds * 1000;
        SetRandomSeed(42);
        const auto rfcV7 = MakeRfcV7YdbBytes(timestampMs);
        SetRandomSeed(42);
        const auto columnKey = MakeColumnKeyUuidBytes(epochSeconds);
        UNIT_ASSERT_VALUES_UNEQUAL(rfcV7, columnKey);
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

    Y_UNIT_TEST(RfcToYdbPreservesVersionAndRandABits) {
        // RFC layout: version nibble in byte 6 high, rand_a continues through byte 7.
        std::array<ui8, NKikimr::NUuid::UUID_LEN> rfc{};
        rfc[0] = 0x01;
        rfc[1] = 0x02;
        rfc[2] = 0x03;
        rfc[3] = 0x04;
        rfc[4] = 0x05;
        rfc[5] = 0x06;
        rfc[6] = 0x7a; // version 7 + rand_a high nibble 0xa
        rfc[7] = 0xbc; // rand_a low byte
        rfc[8] = 0x8d; // variant 10 + random
        for (ui32 i = 9; i < rfc.size(); ++i) {
            rfc[i] = static_cast<ui8>(0x10 + i);
        }

        const auto ydb = RfcUuidBytesToYdbInternal(rfc.data());
        // Version nibble must land in YDB byte 7 high; rand_a must stay intact
        // (YDB byte 6 = RFC byte 7, YDB byte 7 = RFC byte 6).
        UNIT_ASSERT_VALUES_EQUAL(ydb[6], 0xbc);
        UNIT_ASSERT_VALUES_EQUAL(ydb[7], 0x7a);
        UNIT_ASSERT_VALUES_EQUAL(ydb[8], 0x8d);

        // Reverse reorder restores the original RFC MSB, including full rand_a.
        const ui64 restoredMsb = ReorderRfcMsbToYdb(ReadBe64(ydb.data()));
        UNIT_ASSERT_VALUES_EQUAL(restoredMsb, ReadBe64(rfc.data()));
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

    Y_UNIT_TEST(ExtractV7TimestampReturnsNothingForColumnKey) {
        SetRandomSeed(42);
        const auto bytes = MakeColumnKeyUuidBytes(Seconds());
        const auto extracted = ExtractV7TimestampMicrosFromYdbBytes(bytes.data());
        UNIT_ASSERT(!extracted.Defined());
    }

    Y_UNIT_TEST(ExtractV7TimestampReturnsNothingForRowKey) {
        SetRandomSeed(42);
        const auto bytes = MakeRowKeyUuidBytes(0, Seconds(), false);
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
