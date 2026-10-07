#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/common/blob.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NOlap {

namespace {

TBlobRange MakeRange(const ui32 group, const ui32 offset, const ui32 size, const ui32 blobSize = 16 << 20) {
    TLogoBlobID blobId(72075186224040201ull, 1, 1, 0, blobSize, 0);
    return TBlobRange(TUnifiedBlobId(group, blobId), offset, size);
}

void CheckGroupInvariants(const THashMap<TBlobRange, std::vector<TBlobRange>>& groups) {
    for (const auto& [group, subs] : groups) {
        UNIT_ASSERT(!subs.empty());
        for (const auto& sub : subs) {
            UNIT_ASSERT_VALUES_EQUAL(group.GetBlobId(), sub.GetBlobId());
            UNIT_ASSERT(group.Offset <= sub.Offset);
            UNIT_ASSERT(sub.Offset + sub.GetBlobSize() <= group.Offset + group.GetBlobSize());
        }
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TBlobRangeOrdering) {
    Y_UNIT_TEST(SameLogoDifferentDsGroupStrictWeakOrdering) {
        const auto a = MakeRange(1, 200, 10);
        const auto b = MakeRange(2, 100, 10);
        const auto c = MakeRange(1, 210, 10);

        UNIT_ASSERT(a.GetBlobId() != b.GetBlobId());
        UNIT_ASSERT(a.GetBlobId().GetLogoBlobId() == b.GetBlobId().GetLogoBlobId());

        // Different DsGroup must participate in ordering; otherwise a~b, b~c but a<c.
        UNIT_ASSERT(a < b || b < a);
        UNIT_ASSERT(!(a < b && b < a));

        std::vector<TBlobRange> ranges = { a, b, c, a, c, b };
        std::sort(ranges.begin(), ranges.end());
        UNIT_ASSERT(std::is_sorted(ranges.begin(), ranges.end()));

        for (size_t i = 0; i < ranges.size(); ++i) {
            for (size_t j = 0; j < ranges.size(); ++j) {
                if (ranges[i] == ranges[j]) {
                    UNIT_ASSERT(!(ranges[i] < ranges[j]));
                    UNIT_ASSERT(!(ranges[j] < ranges[i]));
                } else if (ranges[i] < ranges[j]) {
                    UNIT_ASSERT(!(ranges[j] < ranges[i]));
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(TBlobsGlueingGroupRanges) {
    Y_UNIT_TEST(SequentialGlueKeepsOffsetInvariant) {
        std::vector<TBlobRange> ranges = {
            MakeRange(1, 0, 100),
            MakeRange(1, 100, 50),
            MakeRange(1, 150, 25),
            MakeRange(2, 0, 10),
        };
        auto groups = TBlobsGlueing::GroupRanges(std::move(ranges), TBlobsGlueing::TSequentialGluePolicy());
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        CheckGroupInvariants(groups);
    }

    Y_UNIT_TEST(BlobGlueWithGapsKeepsOffsetInvariant) {
        std::vector<TBlobRange> ranges = {
            MakeRange(7, 1000, 100),
            MakeRange(7, 0, 50),
            MakeRange(7, 500, 10),
            MakeRange(8, 0, 20),
        };
        auto groups = TBlobsGlueing::GroupRanges(std::move(ranges), TBlobsGlueing::TBlobGluePolicy(8LLU << 20));
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);
        CheckGroupInvariants(groups);

        auto it = groups.find(MakeRange(7, 0, 1100));
        UNIT_ASSERT(it != groups.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second.size(), 3);
    }

    Y_UNIT_TEST(SameLogoDifferentGroupAreNotGluedTogether) {
        std::vector<TBlobRange> ranges = {
            MakeRange(1, 0, 100),
            MakeRange(2, 100, 100),
        };
        auto sequential = TBlobsGlueing::GroupRanges(std::vector<TBlobRange>(ranges), TBlobsGlueing::TSequentialGluePolicy());
        auto blobGlue = TBlobsGlueing::GroupRanges(std::vector<TBlobRange>(ranges), TBlobsGlueing::TBlobGluePolicy(8LLU << 20));
        UNIT_ASSERT_VALUES_EQUAL(sequential.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(blobGlue.size(), 2);
        CheckGroupInvariants(sequential);
        CheckGroupInvariants(blobGlue);
    }
}

}   // namespace NKikimr::NOlap
