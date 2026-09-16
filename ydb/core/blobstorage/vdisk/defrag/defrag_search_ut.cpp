#include "defrag_search.h"

#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugerecovery.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TDefragStripeSearch) {
        Y_UNIT_TEST(SelectsLowOccupancyStripeChunks) {
            constexpr ui32 append = 56896;
            constexpr ui32 chunkSize = 134274560;
            TTestContexts ctxs(chunkSize);

            auto pers = std::make_shared<NHuge::THullHugeKeeperPersState>(
                    ctxs.GetVCtx(),
                    chunkSize,
                    append,
                    append,
                    512u << 10u,
                    10u << 20u,
                    8,
                    0,
                    false,
                    0,
                    false,
                    [](const TString&) {});

            auto hugeBlobCtx = std::make_shared<THugeBlobCtx>(
                    "",
                    pers->Heap->BuildHugeSlotsMap(),
                    EBlobHeaderMode::OLD_HEADER,
                    chunkSize);
            TDefragQuantumChunkFinder finder(hugeBlobCtx, THashSet<TChunkIdx>{1, 2});
            const TLogoBlobID id;
            finder.Add(TDiskPart(1, 0, 1000), id, true, nullptr);
            finder.Add(TDiskPart(2, 0, 8000), id, true, nullptr);
            finder.Add(TDiskPart(2, 8000, 1000), id, false, nullptr);

            TChunksToDefrag result = finder.GetChunksToDefrag(1);
            UNIT_ASSERT_VALUES_EQUAL(result.FoundChunksToDefrag, 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.Chunks.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.Chunks[0].ChunkId, 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.Chunks[0].SlotSize, 0u);
        }

        Y_UNIT_TEST(DoesNotPreferStripeChunkThatIsMostlySst) {
            constexpr ui32 append = 56896;
            constexpr ui32 chunkSize = 134274560;
            TTestContexts ctxs(chunkSize);

            auto pers = std::make_shared<NHuge::THullHugeKeeperPersState>(
                    ctxs.GetVCtx(),
                    chunkSize,
                    append,
                    append,
                    512u << 10u,
                    10u << 20u,
                    8,
                    0,
                    false,
                    0,
                    false,
                    [](const TString&) {});

            auto hugeBlobCtx = std::make_shared<THugeBlobCtx>(
                    "",
                    pers->Heap->BuildHugeSlotsMap(),
                    EBlobHeaderMode::OLD_HEADER,
                    chunkSize);
            TDefragQuantumChunkFinder finder(hugeBlobCtx, THashSet<TChunkIdx>{1, 2});
            const TLogoBlobID id;
            finder.Add(TDiskPart(1, 0, 1000), id, true, nullptr);
            finder.Add(TDiskPart(1, 1000, 50000), id, true, nullptr); // Blocks/Barriers SST on the same chunk
            finder.Add(TDiskPart(2, 0, 8000), id, true, nullptr);

            TChunksToDefrag result = finder.GetChunksToDefrag(1);
            UNIT_ASSERT_VALUES_EQUAL(result.FoundChunksToDefrag, 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.Chunks.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(result.Chunks[0].ChunkId, 2u);
        }
    }

} // NKikimr
