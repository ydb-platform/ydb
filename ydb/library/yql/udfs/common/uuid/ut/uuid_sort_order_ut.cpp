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

constexpr ui64 kTestV7Prefix = 0x294ULL;
constexpr ui64 kTestV8Prefix = 0x294ULL;
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

void AssertUuidStringFormat(TStringBuf uuidString, char versionDigit) {
    AssertUuidStringHasCanonicalDashes(uuidString);
    UNIT_ASSERT_C(uuidString[14] == versionDigit,
        "Expected version digit '" << versionDigit << "' at position 15, got '" << uuidString[14]
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

TUuidBytes GenerateV7WithFixedRandom(ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeV7Bytes(0, timestampMs, false);
}

TUuidBytes GenerateV7WithPrefixAndFixedRandom(ui64 prefix, ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeV7Bytes(prefix, timestampMs, true);
}

TUuidBytes GenerateV8WithFixedRandom(ui64 prefix, ui64 epochSeconds) {
    SetRandomSeed(42);
    return MakeV8Bytes(prefix, epochSeconds, true);
}

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(V7UsesBottom10PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 expectedParam = rawPrefix & PrefixParamMask;  // 0x11
        const ui64 timestampMs = 1'700'000'000'000ULL;
        SetRandomSeed(77);
        const auto v7FromRaw = MakeV7Bytes(rawPrefix, timestampMs, true);
        SetRandomSeed(77);
        const auto v7FromBottomBits = MakeV7Bytes(expectedParam, timestampMs, true);
        UNIT_ASSERT_VALUES_EQUAL(v7FromRaw, v7FromBottomBits);
    }

    Y_UNIT_TEST(V8UsesBottom10PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 expectedParam = rawPrefix & PrefixParamMask;  // 0x11
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(77);
        const auto v8FromRaw = MakeV8Bytes(rawPrefix, epochSeconds, true);
        SetRandomSeed(77);
        const auto v8FromBottomBits = MakeV8Bytes(expectedParam, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(v8FromRaw, v8FromBottomBits);
        // Было: small prefix 3 игнорировался (верхние 10 бит = 0).
        // Стало: prefix=3 реально кодируется.
        SetRandomSeed(88);
        const auto v8WithSmallPrefix = MakeV8Bytes(kSmallPrefix, epochSeconds, true);
        SetRandomSeed(88);
        const auto v8WithZeroPrefix = MakeV8Bytes(0, epochSeconds, true);
        UNIT_ASSERT_VALUES_UNEQUAL(v8WithSmallPrefix, v8WithZeroPrefix);
    }

    Y_UNIT_TEST(V7SortOrderAtTimestampBoundary) {
        const ui64 earlierTimestampMs = 0x00FFFFFFULL;
        const ui64 laterTimestampMs = 0x01000000ULL;

        SetRandomSeed(16180);
        const auto earlierGenerated = MakeV7Bytes(0, earlierTimestampMs, false);
        SetRandomSeed(16180);
        const auto laterGenerated = MakeV7Bytes(0, laterTimestampMs, false);

        UNIT_ASSERT(CompareUuidBytes(earlierGenerated, laterGenerated) < 0);
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
            generated.push_back(GenerateV7WithPrefixAndFixedRandom(kTestV7Prefix, baseTimestampMs + i * 2));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V8SortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(3);

        for (ui32 i = 0; i < 3; ++i) {
            generated.push_back(GenerateV8WithFixedRandom(kTestV8Prefix, baseEpochSeconds + i));
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithoutPrefixVaryingRandom) {
        SetRandomSeed(12345);
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV7Bytes(0, baseTimestampMs + i * 2, false));
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
            generated.push_back(MakeV7Bytes(kTestV7Prefix, baseTimestampMs + i * 2, true));
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
            generated.push_back(MakeV7Bytes(0, timestampMs, false));
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
            generated.push_back(MakeV7Bytes(prefix, timestampMs, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V8DistinctAcrossTimestampsWithVaryingRandom) {
        SetRandomSeed(24680);
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV8Bytes(kTestV8Prefix, baseEpochSeconds + i, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V8DistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(112233);
        const ui64 epochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeV8Bytes(kTestV8Prefix, epochSeconds, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V7StringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeV7Bytes(0, MilliSeconds(), false);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes), '7');
    }

    Y_UNIT_TEST(V7PrefixStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeV7Bytes(kTestV7Prefix, MilliSeconds(), true);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes), '7');
    }

    Y_UNIT_TEST(V8StringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeV8Bytes(0, MilliSeconds() / 1000, false);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes), '8');
    }

    Y_UNIT_TEST(V8PrefixStringFormatShowsVersionDigit) {
        SetRandomSeed(42);
        const auto bytes = MakeV8Bytes(kTestV8Prefix, MilliSeconds() / 1000, true);
        AssertUuidStringFormat(UuidBytesToDisplayString(bytes), '8');
    }
}
