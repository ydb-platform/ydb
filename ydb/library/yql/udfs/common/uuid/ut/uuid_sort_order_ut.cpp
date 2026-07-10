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

constexpr ui64 kTestV7Prefix = 0x2A5ULL;
constexpr ui64 kTestV8Prefix = 0x00000A9400000000ULL;
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

TUuidBytes BuildV7ReferenceBytes(ui64 timestampMs, ui32 seed) {
    SetRandomSeed(seed);
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfcLayout{};
    FillRandomBytes(rfcLayout.data(), rfcLayout.size());

    rfcLayout[0] = static_cast<ui8>((timestampMs >> 40) & 0xff);
    rfcLayout[1] = static_cast<ui8>((timestampMs >> 32) & 0xff);
    rfcLayout[2] = static_cast<ui8>((timestampMs >> 24) & 0xff);
    rfcLayout[3] = static_cast<ui8>((timestampMs >> 16) & 0xff);
    rfcLayout[4] = static_cast<ui8>((timestampMs >> 8) & 0xff);
    rfcLayout[5] = static_cast<ui8>(timestampMs & 0xff);
    rfcLayout[6] = static_cast<ui8>((rfcLayout[6] & 0x0f) | 0x70);
    rfcLayout[8] = static_cast<ui8>((rfcLayout[8] & 0x3f) | 0x80);

    TUuidBytes result{};
    JavaUuidToYdbStorageBytes(
        ReadBe64(rfcLayout.data()),
        ReadBe64(rfcLayout.data() + 8),
        result.data());
    return result;
}

TUuidBytes BuildV8JavaSdkReferenceBytes(ui64 prefix, ui64 epochSeconds, ui32 seed) {
    SetRandomSeed(seed);
    std::array<ui8, NKikimr::NUuid::UUID_LEN> rfcLayout{};
    FillRandomBytes(rfcLayout.data(), rfcLayout.size());
    rfcLayout[6] = static_cast<ui8>((rfcLayout[6] & 0x0f) | 0x80);
    rfcLayout[8] = static_cast<ui8>((rfcLayout[8] & 0x3f) | 0x80);

    ui64 msb = ReadBe64(rfcLayout.data());
    const ui64 javaLsb = ReadBe64(rfcLayout.data() + 8);
    msb = UpdateMsb(msb, prefix, epochSeconds, true);

    TUuidBytes result{};
    JavaUuidToYdbStorageBytes(ReorderMsb(msb), javaLsb, result.data());
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(V7UsesBottom10PrefixBits) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 timestampMs = 1'700'000'000'000ULL;

        UNIT_ASSERT_VALUES_EQUAL(
            ApplyV7Prefix(timestampMs, rawPrefix),
            ApplyV7Prefix(timestampMs, rawPrefix & PrefixMask10));
    }

    Y_UNIT_TEST(V8UsesTop10PrefixBitsLikeJava) {
        const ui64 rawPrefix = 0xAABBCCDDEEFF0011ULL;
        const ui64 epochSeconds = 1'700'000'000ULL;

        SetRandomSeed(77);
        const auto v8FromRaw = MakeV8Bytes(rawPrefix, epochSeconds, true);
        SetRandomSeed(77);
        const auto v8FromTopBits = MakeV8Bytes(rawPrefix & V8PrefixMask, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(v8FromRaw, v8FromTopBits);

        SetRandomSeed(88);
        const auto v8WithSmallPrefix = MakeV8Bytes(kSmallPrefix, epochSeconds, true);
        SetRandomSeed(88);
        const auto v8WithoutPrefix = MakeV8Bytes(0, epochSeconds, true);
        UNIT_ASSERT_VALUES_EQUAL(v8WithSmallPrefix, v8WithoutPrefix);
    }

    Y_UNIT_TEST(V7MatchesJavaSdkStorageLayout) {
        const ui64 timestampMs = 1'700'000'123'456ULL;
        SetRandomSeed(31415);
        const auto generated = MakeV7Bytes(timestampMs);
        const auto reference = BuildV7ReferenceBytes(timestampMs, 31415);
        UNIT_ASSERT_VALUES_EQUAL(generated, reference);
    }

    Y_UNIT_TEST(V8MatchesJavaSdkStorageLayout) {
        const ui64 epochSeconds = 1'700'000'000ULL;
        SetRandomSeed(27182);
        const auto generated = MakeV8Bytes(kTestV8Prefix, epochSeconds, true);
        const auto reference = BuildV8JavaSdkReferenceBytes(kTestV8Prefix, epochSeconds, 27182);
        UNIT_ASSERT_VALUES_EQUAL(generated, reference);
    }

    Y_UNIT_TEST(V7SortOrderMatchesReferenceAtTimestampBoundary) {
        const ui64 earlierTimestampMs = 0x00FFFFFFULL;
        const ui64 laterTimestampMs = 0x01000000ULL;

        SetRandomSeed(16180);
        const auto earlierGenerated = MakeV7Bytes(earlierTimestampMs);
        SetRandomSeed(16180);
        const auto laterGenerated = MakeV7Bytes(laterTimestampMs);

        SetRandomSeed(16180);
        const auto earlierReference = BuildV7ReferenceBytes(earlierTimestampMs, 16180);
        SetRandomSeed(16180);
        const auto laterReference = BuildV7ReferenceBytes(laterTimestampMs, 16180);

        UNIT_ASSERT_VALUES_EQUAL(earlierGenerated, earlierReference);
        UNIT_ASSERT_VALUES_EQUAL(laterGenerated, laterReference);
        UNIT_ASSERT_VALUES_EQUAL(
            CompareUuidBytes(earlierGenerated, laterGenerated),
            CompareUuidBytes(earlierReference, laterReference));
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
            generated.push_back(MakeV7Bytes(ApplyV7Prefix(baseTimestampMs + i * 2, kTestV7Prefix)));
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
}
