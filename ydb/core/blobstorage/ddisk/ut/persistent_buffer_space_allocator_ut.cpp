#include <ydb/core/blobstorage/ddisk/persistent_buffer_space_allocator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk {

namespace {

TPersistentBufferSpaceAllocator MakeAllocator(ui32 sectorsInChunk = 16) {
    return TPersistentBufferSpaceAllocator(sectorsInChunk);
}

} // namespace

Y_UNIT_TEST_SUITE(TPersistentBufferSpaceAllocatorLockDeallocateTest) {

    Y_UNIT_TEST(LockNextChunkPicksOwnedChunk) {
        auto alloc = MakeAllocator();
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);
        alloc.AddNewChunk(2);

        UNIT_ASSERT(!alloc.IsChunkLocked());
        alloc.LockNextChunk();
        UNIT_ASSERT(alloc.IsChunkLocked());
        UNIT_ASSERT(std::find(alloc.OwnedChunks.begin(), alloc.OwnedChunks.end(), alloc.LockedChunkIdx)
            != alloc.OwnedChunks.end());
    }

    Y_UNIT_TEST(LockNextChunkIsNoopWhenAlreadyLocked) {
        auto alloc = MakeAllocator();
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);

        alloc.LockNextChunk();
        ui32 locked = alloc.LockedChunkIdx;
        alloc.LockNextChunk();
        UNIT_ASSERT_VALUES_EQUAL(locked, alloc.LockedChunkIdx);
    }

    Y_UNIT_TEST(LockNextChunkRoundRobinsThroughOwnedChunks) {
        auto alloc = MakeAllocator();
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);
        alloc.AddNewChunk(2);

        std::vector<ui32> lockedOrder;
        for (ui32 i = 0; i < 3; ++i) {
            alloc.LockNextChunk();
            lockedOrder.push_back(alloc.LockedChunkIdx);
            alloc.UnlockChunk();
        }
        // The three chunks should each be visited exactly once as we round-robin.
        std::vector<ui32> sorted = lockedOrder;
        std::sort(sorted.begin(), sorted.end());
        UNIT_ASSERT_VALUES_EQUAL(sorted.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(sorted[0], 0u);
        UNIT_ASSERT_VALUES_EQUAL(sorted[1], 1u);
        UNIT_ASSERT_VALUES_EQUAL(sorted[2], 2u);
    }

    Y_UNIT_TEST(UnlockChunkResetsLock) {
        auto alloc = MakeAllocator();
        alloc.AddNewChunk(0);

        alloc.LockNextChunk();
        UNIT_ASSERT(alloc.IsChunkLocked());
        alloc.UnlockChunk();
        UNIT_ASSERT(!alloc.IsChunkLocked());
        UNIT_ASSERT_VALUES_EQUAL(alloc.LockedChunkIdx, Max<ui32>());
    }

    Y_UNIT_TEST(DeallocateChunkFailsWhenNothingLocked) {
        auto alloc = MakeAllocator();
        alloc.AddNewChunk(0);

        UNIT_ASSERT(!alloc.DeallocateChunk());
        UNIT_ASSERT_VALUES_EQUAL(alloc.OwnedChunks.size(), 1u);
    }

    Y_UNIT_TEST(DeallocateChunkFailsWhenLockedChunkHasOccupiedSectors) {
        auto alloc = MakeAllocator(16);
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);

        // Occupy a sector so chunk 0 is not fully free (chunk with most free space is picked last
        // by Occupy(), i.e. chunk 1; force occupying explicitly on chunk 0 by depleting chunk 1 first).
        auto sectors = alloc.Occupy(16); // fully occupies one whole chunk (the one with most free space)
        UNIT_ASSERT_VALUES_EQUAL(sectors.size(), 16u);
        ui32 occupiedChunk = sectors.front().ChunkIdx;

        alloc.LockedChunkIdx = occupiedChunk;
        UNIT_ASSERT(!alloc.DeallocateChunk());
        UNIT_ASSERT_VALUES_EQUAL(alloc.OwnedChunks.size(), 2u);
    }

    Y_UNIT_TEST(DeallocateChunkSucceedsWhenLockedChunkFullyFree) {
        auto alloc = MakeAllocator(16);
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);

        alloc.LockedChunkIdx = 1;
        UNIT_ASSERT(alloc.DeallocateChunk());
        UNIT_ASSERT_VALUES_EQUAL(alloc.OwnedChunks.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(alloc.OwnedChunks[0], 0u);
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreeSpace(), 16u);
        // DeallocateChunk should unlock automatically on success.
        UNIT_ASSERT(!alloc.IsChunkLocked());
    }

    Y_UNIT_TEST(DeallocateChunkRemovesChunkFromFutureOccupy) {
        auto alloc = MakeAllocator(4);
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);

        alloc.LockedChunkIdx = 1;
        UNIT_ASSERT(alloc.DeallocateChunk());

        // Only chunk 0 should remain; occupying more than its capacity must fail.
        auto sectors = alloc.Occupy(5);
        UNIT_ASSERT_VALUES_EQUAL(sectors.size(), 0u);

        auto sectors2 = alloc.Occupy(4);
        UNIT_ASSERT_VALUES_EQUAL(sectors2.size(), 4u);
        for (auto& s : sectors2) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(s.ChunkIdx), 0u);
        }
    }

    Y_UNIT_TEST(OccupySkipsLockedChunkWhenOtherSpaceAvailable) {
        auto alloc = MakeAllocator(4);
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);

        // Lock chunk 1 (fully free) to protect it from being used for allocation.
        alloc.LockedChunkIdx = 1;

        // Requesting all 4 sectors of a single chunk should come from chunk 0 (unlocked),
        // leaving chunk 1 fully free and lockable/deallocatable.
        auto sectors = alloc.Occupy(4);
        UNIT_ASSERT_VALUES_EQUAL(sectors.size(), 4u);
        for (auto& s : sectors) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(s.ChunkIdx), 0u);
        }
        UNIT_ASSERT(alloc.DeallocateChunk());
    }

    Y_UNIT_TEST(OccupyUnlocksChunkWhenNoOtherSpaceAvailable) {
        auto alloc = MakeAllocator(4);
        alloc.AddNewChunk(0);
        alloc.AddNewChunk(1);

        // Occupy all of chunk 0 so it's no longer free.
        auto used = alloc.Occupy(4);
        UNIT_ASSERT_VALUES_EQUAL(used.size(), 4u);

        // Now lock the only remaining fully-free chunk (chunk 1).
        alloc.LockedChunkIdx = 1;

        // Free space is only within the locked chunk; Occupy must fall back to using it
        // (unlocking automatically) rather than failing.
        auto sectors = alloc.Occupy(2);
        UNIT_ASSERT_VALUES_EQUAL(sectors.size(), 2u);
        UNIT_ASSERT(!alloc.IsChunkLocked());
    }

} // Y_UNIT_TEST_SUITE(TPersistentBufferSpaceAllocatorLockDeallocateTest)

} // namespace NKikimr::NDDisk
