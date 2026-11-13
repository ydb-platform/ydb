#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/test_helper/portion_test_helper.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap {

Y_UNIT_TEST_SUITE(TPortionChunksPair) {
    Y_UNIT_TEST(TwoPortionsShareOneChunksObject) {
        const auto pathId = TInternalPathId::FromRawValue(1);
        const auto portion = NTest::MakeTestCompactedPortion(pathId, 7, 1, 2, 2, TSnapshot(1, 1), std::nullopt);
        const TUnifiedBlobId blob(1, TLogoBlobID(123, 1, 1, 2, 100, 0));
        const TPortionDataAccessor accessor(
            portion, std::vector<TUnifiedBlobId>{ blob }, std::vector<TColumnRecord>{}, std::vector<TIndexChunk>{}, false);

        auto copy = portion->MakeCopy();
        copy->SetRemoveSnapshot(2, 2);
        const TPortionDataAccessor paired(copy, accessor.GetChunksPtr());

        UNIT_ASSERT(accessor.GetChunksPtr().get() == paired.GetChunksPtr().get());
        UNIT_ASSERT(accessor.GetPortionInfoPtr().get() != paired.GetPortionInfoPtr().get());
        UNIT_ASSERT(!accessor.GetPortionInfo().HasRemoveSnapshot());
        UNIT_ASSERT(paired.GetPortionInfo().HasRemoveSnapshot());
        UNIT_ASSERT(paired.GetChunks().GetAddress() == paired.GetPortionInfo().GetAddress());
        UNIT_ASSERT_VALUES_EQUAL(paired.GetBlobIds().size(), 1);
    }
}

}   // namespace NKikimr::NOlap
