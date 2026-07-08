#include <ydb/library/yql/udfs/common/uuid/uuid_keygen.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>
#include <util/system/datetime.h>

#include <array>
#include <cstring>
#include <vector>

using namespace NYql::NUuidKeyGen;

namespace {

using TUuidBytes = std::array<ui8, NKikimr::NUuid::UUID_LEN>;

int CompareUuidBytes(const TUuidBytes& lhs, const TUuidBytes& rhs) {
    return std::memcmp(lhs.data(), rhs.data(), rhs.size());
}

void AssertGenerationOrderIsSortOrder(const TVector<TUuidBytes>& generated) {
    UNIT_ASSERT(generated.size() >= 2);

    for (size_t i = 1; i < generated.size(); ++i) {
        UNIT_ASSERT_C(CompareUuidBytes(generated[i - 1], generated[i]) < 0,
            "UUID at index " << i - 1 << " must sort before UUID at index " << i
            << " in YDB byte order");
    }
}

TUuidBytes GenerateV7WithTimestamp(ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeV7Bytes(timestampMs);
}

TUuidBytes GenerateV7WithPrefixAndTimestamp(ui64 prefix, ui64 timestampMs) {
    SetRandomSeed(42);
    return MakeV7Bytes(ApplyV7Prefix(timestampMs, prefix));
}

TUuidBytes GenerateV8WithTimestamp(ui64 prefix, ui64 epochSeconds) {
    SetRandomSeed(42);
    return MakeV8Bytes(prefix, epochSeconds, true);
}

} // namespace

Y_UNIT_TEST_SUITE(TUuidSortOrder) {
    Y_UNIT_TEST(V7SortOrderWithoutPrefix) {
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateV7WithTimestamp(baseTimestampMs + i * 2));
            if (i + 1 < 10) {
                Sleep(TDuration::MilliSeconds(2));
            }
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V7SortOrderWithFixedPrefix) {
        const ui64 prefix = 0x2A5ULL;
        const ui64 baseTimestampMs = MilliSeconds();
        TVector<TUuidBytes> generated;
        generated.reserve(10);

        for (ui32 i = 0; i < 10; ++i) {
            generated.push_back(GenerateV7WithPrefixAndTimestamp(prefix, baseTimestampMs + i * 2));
            if (i + 1 < 10) {
                Sleep(TDuration::MilliSeconds(2));
            }
        }

        AssertGenerationOrderIsSortOrder(generated);
    }

    Y_UNIT_TEST(V8SortOrderWithFixedPrefix) {
        const ui64 prefix = NewPrefixValue() & V8PrefixMask;
        const ui64 baseEpochSeconds = MilliSeconds() / 1000;
        TVector<TUuidBytes> generated;
        generated.reserve(3);

        for (ui32 i = 0; i < 3; ++i) {
            generated.push_back(GenerateV8WithTimestamp(prefix, baseEpochSeconds + i));
            if (i + 1 < 3) {
                Sleep(TDuration::MilliSeconds(1100));
            }
        }

        AssertGenerationOrderIsSortOrder(generated);
    }
}
