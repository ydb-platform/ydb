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

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(V7SortOrderWithoutPrefixFixedRandom) {
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateV7WithFixedRandom(baseTimestampMs + i * 2));
            if (i + 1 < 10) {
                Sleep(TDuration::MilliSeconds(2));
            }
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 prefix = 0x2A5ULL;
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateV7WithPrefixAndFixedRandom(prefix, baseTimestampMs + i * 2));
            if (i + 1 < 10) {
                Sleep(TDuration::MilliSeconds(2));
            }
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V8SortOrderWithFixedPrefixAndFixedRandom) {
        const ui64 prefix = NewPrefixValue() & V8PrefixMask;
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(3);

        for (ui32 i = 0; i < 3; ++i) {
            generated.push_back(GenerateV8WithFixedRandom(prefix, baseEpochSeconds + i));
            if (i + 1 < 3) {
                Sleep(TDuration::MilliSeconds(1100));
            }
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
            if (i + 1 < 10) {
                Sleep(TDuration::MilliSeconds(2));
            }
        }

        AssertGenerationOrderIsSortOrder(generated);
        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithPrefixVaryingRandom) {
        SetRandomSeed(54321);
        const ui64 prefix = 0x2A5ULL;
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV7Bytes(ApplyV7Prefix(baseTimestampMs + i * 2, prefix)));
            if (i + 1 < 10) {
                Sleep(TDuration::MilliSeconds(2));
            }
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
        const ui64 prefix = 0x2A5ULL << 54;
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(MakeV8Bytes(prefix, baseEpochSeconds + i, true));
        }

        AssertAllDistinct(generated);
    }

    Y_UNIT_TEST(V8DistinctRandomSuffixAtSameTimestamp) {
        SetRandomSeed(112233);
        const ui64 prefix = 0x2A5ULL << 54;
        const ui64 epochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(32);

        for (ui32 i = 0; i < 32; ++i) {
            generated.push_back(MakeV8Bytes(prefix, epochSeconds, true));
        }

        AssertAllDistinct(generated);
    }
}
