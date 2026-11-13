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

    Y_UNIT_TEST(SharedExtractCopiesAndUniqueExtractMoves) {
        const auto pathId = TInternalPathId::FromRawValue(1);
        const auto portion = NTest::MakeTestCompactedPortion(pathId, 7, 1, 2, 2, TSnapshot(1, 1), std::nullopt);
        const TUnifiedBlobId blob(1, TLogoBlobID(123, 1, 1, 2, 100, 0));
        auto record = TColumnRecord::TTestInstanceBuilder::Build(1, 0, 0, 100, 2, 80);
        record.RegisterBlobIdx(0);
        const TIndexChunk indexChunk(4, 0, 2, 16, TBlobRangeLink16(0, 0, 16));

        auto makeAccessor = [&](const TPortionInfo::TConstPtr& info) {
            return TPortionDataAccessor(
                info, std::vector<TUnifiedBlobId>{ blob }, std::vector<TColumnRecord>{ record }, std::vector<TIndexChunk>{ indexChunk }, false);
        };

        TPortionDataAccessor owner = makeAccessor(portion);
        auto removed = portion->MakeCopy();
        removed->SetRemoveSnapshot(4, 4);
        TPortionDataAccessor shared(removed, owner.GetChunksPtr());

        const auto copiedRecords = shared.ExtractRecords();
        const auto copiedIndexes = shared.ExtractIndexes();
        const auto copiedBlobs = shared.ExtractBlobIds();
        UNIT_ASSERT_VALUES_EQUAL(copiedRecords.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(copiedIndexes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(copiedBlobs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(copiedRecords.front().GetBlobRange().GetBlobIdxVerified(), 0);
        UNIT_ASSERT(owner.GetChunksPtr().get() == shared.GetChunksPtr().get());
        UNIT_ASSERT_VALUES_EQUAL(owner.GetRecordsVerified().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(owner.GetIndexesVerified().size(), 1);
        UNIT_ASSERT(owner.GetBlobId(copiedRecords.front().GetBlobRange().GetBlobIdxVerified()) == blob);
        UNIT_ASSERT(!owner.GetPortionInfo().HasRemoveSnapshot());
        UNIT_ASSERT(shared.GetPortionInfo().HasRemoveSnapshot());

        TPortionDataAccessor sole = makeAccessor(portion);
        auto movedRecords = sole.ExtractRecords();
        auto movedIndexes = sole.ExtractIndexes();
        auto movedBlobs = sole.ExtractBlobIds();
        UNIT_ASSERT(sole.GetRecordsVerified().empty());
        UNIT_ASSERT(sole.GetIndexesVerified().empty());
        UNIT_ASSERT(sole.GetBlobIds().empty());

        const TPortionDataAccessor rebuilt(portion, std::move(movedBlobs), std::move(movedRecords), std::move(movedIndexes), true);
        UNIT_ASSERT(rebuilt.GetBlobId(rebuilt.GetRecordsVerified().front().GetBlobRange().GetBlobIdxVerified()) == blob);
        UNIT_ASSERT_VALUES_EQUAL(rebuilt.RestoreBlobRange(rebuilt.GetRecordsVerified().front().GetBlobRange()).Size, 100u);
        UNIT_ASSERT_VALUES_EQUAL(owner.GetRecordsVerified().size(), 1);
    }
}

}   // namespace NKikimr::NOlap
