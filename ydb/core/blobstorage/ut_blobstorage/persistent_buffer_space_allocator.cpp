#include <ydb/core/blobstorage/ddisk/persistent_buffer_space_allocator.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

namespace NKikimr::NDDisk {

namespace {

    std::vector<TPersistentBufferSectorInfo> MakeSectorRange(ui32 chunkIdx, ui32 fromSectorIdx, ui32 toSectorIdx) {
        std::vector<TPersistentBufferSectorInfo> locations;
        locations.reserve(toSectorIdx - fromSectorIdx + 1);
        for (ui32 sectorIdx = fromSectorIdx; sectorIdx <= toSectorIdx; ++sectorIdx) {
            locations.push_back({chunkIdx, sectorIdx, false, 0, 0});
        }
        return locations;
    }

    ui32 SumDescribeFreeSpace(TPersistentBufferSpaceAllocator& allocator) {
        ui32 total = 0;
        for (const auto& chunkRanges : allocator.DescribeFreeSpace()) {
            for (const auto& [first, last] : chunkRanges) {
                total += last - first + 1;
            }
        }
        return total;
    }

} // namespace

Y_UNIT_TEST_SUITE(PersistentBufferSpaceAllocator) {

    Y_UNIT_TEST(Empty) {
        TPersistentBufferSpaceAllocator allocator;
        UNIT_ASSERT_EQUAL(allocator.Occupy(10).size(), 0);
    }

    Y_UNIT_TEST(OneOccupy) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(123);
        auto result = allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(result.size(), 10);
        for(ui32 i : xrange(10)) {
            UNIT_ASSERT_EQUAL(result[i].ChunkIdx, 123);
            UNIT_ASSERT_EQUAL(result[i].SectorIdx, i);
        }
    }

    Y_UNIT_TEST(OccupyAllSpaceAndAddNewChunk) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(123);
        for (ui32 i : xrange(32768 / 10)) {
            auto result = allocator.Occupy(10);
            UNIT_ASSERT_EQUAL(result.size(), 10);
            for(ui32 j : xrange(10)) {
                UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 123);
                UNIT_ASSERT_EQUAL(result[j].SectorIdx, i * 10 + j);
            }
        }
        UNIT_ASSERT_EQUAL(allocator.Occupy(10).size(), 0);
        allocator.AddNewChunk(321);
        auto result = allocator.Occupy(20);
        UNIT_ASSERT_EQUAL(result.size(), 20);
        for(ui32 j : xrange(20)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 321);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, j);
        }
    }

    Y_UNIT_TEST(AddNewChunkAndOccupyAllSpace) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(123);
        allocator.AddNewChunk(321);
        for (ui32 _ : xrange(32768 / 10 * 2)) {
            allocator.Occupy(10);
        }
        UNIT_ASSERT_EQUAL(allocator.Occupy(20).size(), 0);
        auto result = allocator.Occupy(15);
        UNIT_ASSERT_EQUAL(result.size(), 15);
        for(ui32 j : xrange(8)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 123);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, 32760 + j);
        }
        for(ui32 j : xrange(8, 7)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 321);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, 32760 + j - 8);
        }
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), 1);
    }

    Y_UNIT_TEST(OccupyChunkSeedTest) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(11);
        allocator.AddNewChunk(22);
        allocator.AddNewChunk(33);
        for (ui32 i : xrange(32768 / 10)) {
            auto result = allocator.Occupy(10);
            UNIT_ASSERT_EQUAL(result.size(), 10);
            for(ui32 j : xrange(10)) {
                UNIT_ASSERT_EQUAL(result[j].ChunkIdx, (i % 3 + 1) * 11);
                UNIT_ASSERT_EQUAL(result[j].SectorIdx, i / 3 * 10 + j);
            }
        }
    }

    Y_UNIT_TEST(OccupyHoleTest) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(11);
        allocator.Occupy(10);
        allocator.Occupy(10);
        auto result = allocator.Occupy(10);
        allocator.Occupy(10);
        allocator.Occupy(10);
        auto result2 = allocator.Occupy(10);
        allocator.Occupy(10);
        allocator.Free(result);
        allocator.Free(result2);
        result = allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(result.size(), 10);
        for(ui32 j : xrange(10)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 11);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, 20 + j);
        }
    }

    Y_UNIT_TEST(OccupyDoubleHoleTest) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(11);
        allocator.Occupy(10);
        allocator.Occupy(10);
        auto result = allocator.Occupy(10);
        auto result2 = allocator.Occupy(10);
        allocator.Occupy(10);
        allocator.Free(result);
        allocator.Free(result2);
        result = allocator.Occupy(20);
        UNIT_ASSERT_EQUAL(result.size(), 20);
        for(ui32 j : xrange(20)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 11);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, 20 + j);
        }
    }

    Y_UNIT_TEST(OccupyBestChoiseTest) {
        TPersistentBufferSpaceAllocator allocator;
        // 10 20 10 10 30 30 10
        //       XX    XX       XXXXXXX
        //             ^^
        allocator.AddNewChunk(11);
        allocator.Occupy(10);
        allocator.Occupy(20);
        auto result = allocator.Occupy(10);
        allocator.Occupy(10);
        auto result2 = allocator.Occupy(30);
        allocator.Occupy(30);
        allocator.Occupy(10);
        allocator.Free(result);
        allocator.Free(result2);
        result = allocator.Occupy(30);
        UNIT_ASSERT_EQUAL(result.size(), 30);
        for(ui32 j : xrange(30)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 11);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, 50 + j);
        }
    }

    Y_UNIT_TEST(FragmentationTest) {
        TPersistentBufferSpaceAllocator allocator;
        // 10 20 10 10 30 10 10 ......
        //       XX    XX
        //       ^^    ^^
        allocator.AddNewChunk(11);
        ui32 free = 32768;
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free);
        allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free - 10);
        allocator.Occupy(20);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free - 10 - 20);
        auto result = allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free - 10 - 20 - 10);
        allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free - 10 - 20 - 10 - 10);
        auto result2 = allocator.Occupy(30);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free - 10 - 20 - 10 - 10 - 30);
        for (ui32 i : xrange((32768 - 80) / 10)) {
            allocator.Occupy(10);
            UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), free - 90 - 10 * i);
        }
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), 8);
        allocator.Free(result);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), 18);
        allocator.Free(result2);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), 48);
        result = allocator.Occupy(37);
        UNIT_ASSERT_EQUAL(allocator.GetFreeSpace(), 11);
        UNIT_ASSERT_EQUAL(result.size(), 37);
        for(ui32 j : xrange(0, 30)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 11);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, 50 + j);
        }
        for(ui32 j : xrange(0, 7)) {
            UNIT_ASSERT_EQUAL(result[j + 30].ChunkIdx, 11);
            UNIT_ASSERT_EQUAL(result[j + 30].SectorIdx, 30 + j);
        }

    }

    Y_UNIT_TEST(MarkOccupiedTest) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(11);

        allocator.MarkOccupied({{11, 10, false, 0, 0}, {11, 11, false, 0, 0}, {11, 12, false, 0, 0}});
        allocator.MarkOccupied({{11, 20, false, 0, 0}, {11, 21, false, 0, 0}, {11, 22, false, 0, 0}});
        auto result = allocator.Occupy(5);
        UNIT_ASSERT_EQUAL(result[0].SectorIdx, 23);
        result = allocator.Occupy(7);
        UNIT_ASSERT_EQUAL(result[0].SectorIdx, 13);
    }

    Y_UNIT_TEST(TwoFullChunksTieBreakLowerIdxWins) {
        TPersistentBufferSpaceAllocator allocator;
        allocator.AddNewChunk(22);
        allocator.AddNewChunk(11);
        auto result = allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(result.size(), 10);
        for (ui32 j : xrange(10)) {
            UNIT_ASSERT_EQUAL(result[j].ChunkIdx, 11);
            UNIT_ASSERT_EQUAL(result[j].SectorIdx, j);
        }
    }

    Y_UNIT_TEST(MultiChunkFreeReordersMultiplePositions) {
        // Four chunks with fixed free counts: 11(8), 22(2), 33(3), 44(4). Occupy(7) from best
        // chunk 11 leaves it at index 0 with 1 free sector. Free() restores 8 free sectors and
        // moves chunk 11 three slots right (past 22, 33, 44).
        TPersistentBufferSpaceAllocator allocator(64);
        allocator.AddNewChunk(11);
        allocator.AddNewChunk(22);
        allocator.AddNewChunk(33);
        allocator.AddNewChunk(44);
        allocator.MarkOccupied(MakeSectorRange(11, 8, 63));
        allocator.MarkOccupied(MakeSectorRange(22, 2, 63));
        allocator.MarkOccupied(MakeSectorRange(33, 3, 63));
        allocator.MarkOccupied(MakeSectorRange(44, 4, 63));
        auto heldFrom11 = allocator.Occupy(7);
        UNIT_ASSERT_EQUAL(heldFrom11.size(), 7);
        for (const auto& sector : heldFrom11) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 11);
        }
        allocator.Free(heldFrom11);
        auto result = allocator.Occupy(3);
        UNIT_ASSERT_EQUAL(result.size(), 3);
        for (const auto& sector : result) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 11);
        }
    }

    Y_UNIT_TEST(MultiChunkFreeReordersToBest) {
        TPersistentBufferSpaceAllocator allocator(256);
        allocator.AddNewChunk(11);
        allocator.AddNewChunk(22);
        allocator.MarkOccupied(MakeSectorRange(11, 180, 255));
        allocator.MarkOccupied(MakeSectorRange(22, 220, 255));
        auto r22 = allocator.Occupy(200);
        UNIT_ASSERT_EQUAL(r22.size(), 200);
        for (const auto& sector : r22) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 22);
        }
        auto r11 = allocator.Occupy(170);
        UNIT_ASSERT_EQUAL(r11.size(), 170);
        for (const auto& sector : r11) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 11);
        }
        allocator.Free(r11);
        auto result = allocator.Occupy(10);
        UNIT_ASSERT_EQUAL(result.size(), 10);
        for (const auto& sector : result) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 11);
        }
    }

    Y_UNIT_TEST(MultiChunkMarkOccupiedChangesBestChunk) {
        TPersistentBufferSpaceAllocator allocator(256);
        allocator.AddNewChunk(11);
        allocator.AddNewChunk(22);
        allocator.MarkOccupied(MakeSectorRange(22, 100, 255));
        allocator.MarkOccupied(MakeSectorRange(11, 10, 255));
        auto result = allocator.Occupy(5);
        UNIT_ASSERT_EQUAL(result.size(), 5);
        for (const auto& sector : result) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 22);
        }
        allocator.MarkOccupied(MakeSectorRange(22, 10, 99));
        result = allocator.Occupy(5);
        UNIT_ASSERT_EQUAL(result.size(), 5);
        for (const auto& sector : result) {
            UNIT_ASSERT_EQUAL(sector.ChunkIdx, 11);
        }
    }

    Y_UNIT_TEST(DescribeFreeSpaceSanity) {
        TPersistentBufferSpaceAllocator allocator(256);
        allocator.AddNewChunk(11);
        UNIT_ASSERT_EQUAL(SumDescribeFreeSpace(allocator), allocator.GetFreeSpace());
        allocator.Occupy(50);
        UNIT_ASSERT_EQUAL(SumDescribeFreeSpace(allocator), allocator.GetFreeSpace());
        auto result = allocator.Occupy(30);
        allocator.Free(result);
        UNIT_ASSERT_EQUAL(SumDescribeFreeSpace(allocator), allocator.GetFreeSpace());
        UNIT_ASSERT_EQUAL(allocator.DescribeFreeSpace().size(), allocator.OwnedChunks.size());
    }
}
};
