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
    }

    Y_UNIT_TEST(ShouldGetTouchedVChunksForRegion)
    {
        TTouchedVChunks touchedVChunks;
        touchedVChunks.Add(31, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(32, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(63, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.Add(64, NThreading::NewPromise<EPersistResult>());

        const auto firstRegion = touchedVChunks.GetTouchedVChunks(0);
        UNIT_ASSERT(firstRegion.Get(31));
        UNIT_ASSERT(!firstRegion.Get(0));

        const auto secondRegion = touchedVChunks.GetTouchedVChunks(32);
        UNIT_ASSERT(secondRegion.Get(0));
        UNIT_ASSERT(secondRegion.Get(31));
        UNIT_ASSERT(!secondRegion.Get(1));

        const auto thirdRegion = touchedVChunks.GetTouchedVChunks(64);
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

        touchedVChunks.Add(2, NThreading::NewPromise<EPersistResult>());
        touchedVChunks.OnSaveCompleted();
        UNIT_ASSERT(touchedVChunks.HasPendingChanges());

        chunks = touchedVChunks.BeginSave();
        UNIT_ASSERT_VALUES_EQUAL(1, chunks.size());
        UNIT_ASSERT_VALUES_EQUAL(0, chunks[0].VChunkStartIndex);
        UNIT_ASSERT(touchedVChunks.Get(1));
        UNIT_ASSERT(touchedVChunks.Get(2));

        touchedVChunks.OnSaveCompleted();
        UNIT_ASSERT(!touchedVChunks.HasPendingChanges());
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
