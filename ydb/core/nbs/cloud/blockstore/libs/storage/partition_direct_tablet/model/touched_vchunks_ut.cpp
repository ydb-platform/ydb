#include "touched_vchunks.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTouchedVChunksTest)
{
    Y_UNIT_TEST(ShouldAddAndGetVChunks)
    {
        TTouchedVChunks touchedVChunks;

        UNIT_ASSERT(!touchedVChunks.Get(0));
        UNIT_ASSERT(
            touchedVChunks.Add(0, NThreading::NewPromise<EPersistResult>()));
        UNIT_ASSERT(
            touchedVChunks.Add(1023, NThreading::NewPromise<EPersistResult>()));
        UNIT_ASSERT(
            touchedVChunks.Add(1024, NThreading::NewPromise<EPersistResult>()));
        UNIT_ASSERT(!touchedVChunks.Add(
            1024,
            NThreading::NewPromise<EPersistResult>()));

        UNIT_ASSERT(touchedVChunks.Get(0));
        UNIT_ASSERT(touchedVChunks.Get(1023));
        UNIT_ASSERT(touchedVChunks.Get(1024));
        UNIT_ASSERT(!touchedVChunks.Get(1));
        UNIT_ASSERT_VALUES_EQUAL(3, touchedVChunks.GetCount());
    }

    Y_UNIT_TEST(ShouldLoadMaskChunk)
    {
        TString mask(TTouchedVChunks::MaskSize, 0);
        mask[0] = 1;

        TTouchedVChunks touchedVChunks;
        touchedVChunks.Load({
            .VChunkStartIndex = TTouchedVChunks::VChunksPerMask,
            .Mask = std::move(mask),
        });

        UNIT_ASSERT(!touchedVChunks.Get(0));
        UNIT_ASSERT(touchedVChunks.Get(TTouchedVChunks::VChunksPerMask));
        UNIT_ASSERT_VALUES_EQUAL(1, touchedVChunks.GetCount());

        mask = TString(TTouchedVChunks::MaskSize, 0);
        mask[0] = 3;
        touchedVChunks.Load({
            .VChunkStartIndex = TTouchedVChunks::VChunksPerMask,
            .Mask = std::move(mask),
        });
        UNIT_ASSERT_VALUES_EQUAL(2, touchedVChunks.GetCount());
    }

    Y_UNIT_TEST(ShouldGetTouchedVChunksForRegion)
    {
        TTouchedVChunks touchedVChunks;
        touchedVChunks.Add(0, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(9, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(18, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(31, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(32, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(63, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(64, NThreading::NewPromise<EPersistResult>());

        const auto firstRegion = touchedVChunks.GetTouchedVChunks(0);
        UNIT_ASSERT(firstRegion.Get(0));
        UNIT_ASSERT(firstRegion.Get(9));
        UNIT_ASSERT(firstRegion.Get(18));
        UNIT_ASSERT(firstRegion.Get(31));
        UNIT_ASSERT(!firstRegion.Get(1));

        const auto secondRegion = touchedVChunks.GetTouchedVChunks(1);
        UNIT_ASSERT(secondRegion.Get(0));
        UNIT_ASSERT(secondRegion.Get(31));
        UNIT_ASSERT(!secondRegion.Get(1));

        const auto thirdRegion = touchedVChunks.GetTouchedVChunks(2);
        UNIT_ASSERT(thirdRegion.Get(0));
        UNIT_ASSERT(!thirdRegion.Get(1));
    }

    Y_UNIT_TEST(ShouldSaveOnlyChangedMaskChunks)
    {
        TTouchedVChunks touchedVChunks;
        touchedVChunks.Add(1, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(
            TTouchedVChunks::VChunksPerMask,
            NThreading::NewPromise<EPersistResult>());

        auto chunks = touchedVChunks.BeginSave();
        UNIT_ASSERT_VALUES_EQUAL(2, chunks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, chunks[0].VChunkStartIndex);
        UNIT_ASSERT_VALUES_EQUAL(
            TTouchedVChunks::VChunksPerMask,
            chunks[1].VChunkStartIndex);
        UNIT_ASSERT_VALUES_EQUAL(
            TTouchedVChunks::MaskSize,
            chunks[0].Mask.size());
        UNIT_ASSERT(touchedVChunks.IsSaveInProgress());

        auto savingBitPromise = NThreading::NewPromise<EPersistResult>();
        auto savingBitFuture = savingBitPromise.GetFuture();
        UNIT_ASSERT(!touchedVChunks.Add(1, std::move(savingBitPromise)));
        UNIT_ASSERT(!touchedVChunks.HasPendingChanges());

        auto pendingBitPromise = NThreading::NewPromise<EPersistResult>();
        auto pendingBitFuture = pendingBitPromise.GetFuture();
        UNIT_ASSERT(!touchedVChunks.Add(2, std::move(pendingBitPromise)));

        auto duplicatePendingBitPromise =
            NThreading::NewPromise<EPersistResult>();
        auto duplicatePendingBitFuture = duplicatePendingBitPromise.GetFuture();
        UNIT_ASSERT(
            !touchedVChunks.Add(2, std::move(duplicatePendingBitPromise)));

        UNIT_ASSERT(!savingBitFuture.HasValue());
        UNIT_ASSERT(!pendingBitFuture.HasValue());
        UNIT_ASSERT(!duplicatePendingBitFuture.HasValue());

        touchedVChunks.OnSaveCompleted();
        UNIT_ASSERT_VALUES_EQUAL(
            EPersistResult::Success,
            savingBitFuture.GetValue());
        UNIT_ASSERT(!pendingBitFuture.HasValue());
        UNIT_ASSERT(!duplicatePendingBitFuture.HasValue());
        UNIT_ASSERT(touchedVChunks.HasPendingChanges());

        chunks = touchedVChunks.BeginSave();
        UNIT_ASSERT_VALUES_EQUAL(1, chunks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, chunks[0].VChunkStartIndex);
        UNIT_ASSERT(touchedVChunks.Get(1));
        UNIT_ASSERT(touchedVChunks.Get(2));

        touchedVChunks.OnSaveCompleted();
        UNIT_ASSERT_VALUES_EQUAL(
            EPersistResult::Success,
            pendingBitFuture.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(
            EPersistResult::Success,
            duplicatePendingBitFuture.GetValue());
        UNIT_ASSERT(!touchedVChunks.HasPendingChanges());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
