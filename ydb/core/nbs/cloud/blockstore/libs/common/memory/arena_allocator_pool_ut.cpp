#include "arena_allocator_pool.h"

#include "arena_allocator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <cstring>
#include <memory>
#include <type_traits>
#include <unordered_set>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

namespace {

struct TTrackingAllocator final: public IArenaAllocator
{
    size_t AllocatedCount = 0;

    void* Allocate(size_t size) override
    {
        ++AllocatedCount;
        return ::operator new(size);
    }

    void DeAllocate(void* ptr) override
    {
        --AllocatedCount;
        ::operator delete(ptr);
    }

    size_t AllocatedBlocks() const override
    {
        return AllocatedCount;
    }

    size_t AllocatedSize() const override
    {
        return AllocatedCount;
    }

    TVector<TArenaAllocatorStats> GetStats() const override
    {
        return {};
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(ArenaAllocatorPoolTest)
{
    Y_UNIT_TEST(ShouldNotBeCopyableOrMovable)
    {
        static_assert(!std::is_copy_constructible_v<TArenaAllocatorPool>);
        static_assert(!std::is_copy_assignable_v<TArenaAllocatorPool>);
        static_assert(!std::is_move_constructible_v<TArenaAllocatorPool>);
        static_assert(!std::is_move_assignable_v<TArenaAllocatorPool>);
    }

    Y_UNIT_TEST(TracksAllocatedAndUsedSize)
    {
        constexpr size_t SlotSize = 1024;
        constexpr size_t ChunkSize = 128;

        TArenaAllocatorPool pool(CreateArenaAllocator(), SlotSize);
        UNIT_ASSERT_VALUES_EQUAL(0, pool.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(0, pool.GetUsedSize());

        void* first = pool.Allocate(ChunkSize);
        void* second = pool.Allocate(ChunkSize);
        UNIT_ASSERT_VALUES_EQUAL(SlotSize, pool.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(2 * ChunkSize, pool.GetUsedSize());

        pool.Deallocate(first);
        UNIT_ASSERT_VALUES_EQUAL(SlotSize, pool.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(ChunkSize, pool.GetUsedSize());

        void* reused = pool.Allocate(ChunkSize);
        UNIT_ASSERT_EQUAL(first, reused);
        UNIT_ASSERT_VALUES_EQUAL(SlotSize, pool.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(2 * ChunkSize, pool.GetUsedSize());

        pool.Deallocate(second);
        pool.Deallocate(reused);
        UNIT_ASSERT_VALUES_EQUAL(0, pool.GetAllocatedSize());
        UNIT_ASSERT_VALUES_EQUAL(0, pool.GetUsedSize());
    }

    Y_UNIT_TEST(AllocateAndFreeChunks)
    {
        constexpr size_t SlotSize = 4096;
        constexpr size_t ChunkSize = 512;
        constexpr size_t ChunksPerSlot = SlotSize / ChunkSize;

        auto allocator = std::make_shared<TTrackingAllocator>();
        TArenaAllocatorPool pool(allocator);

        TVector<void*> ptrs;
        for (size_t i = 0; i < ChunksPerSlot; ++i) {
            void* ptr = pool.Allocate(ChunkSize);
            UNIT_ASSERT(ptr);
            ptrs.push_back(ptr);
        }

        // All pointers should be unique.
        std::unordered_set<void*> unique(ptrs.begin(), ptrs.end());
        UNIT_ASSERT_VALUES_EQUAL(ChunksPerSlot, unique.size());

        // Only a single slot should have been acquired.
        UNIT_ASSERT_VALUES_EQUAL(1, allocator->AllocatedBlocks());

        for (void* ptr: ptrs) {
            pool.Deallocate(ptr);
        }

        // The whole slot should be returned to the allocator.
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
    }

    Y_UNIT_TEST(ChunkSizeIsRespected)
    {
        constexpr size_t ChunkSize = 512;

        TArenaAllocatorPool pool(CreateArenaAllocator());

        void* first = pool.Allocate(ChunkSize);
        void* second = pool.Allocate(ChunkSize);

        const auto diff =
            static_cast<char*>(second) - static_cast<char*>(first);
        UNIT_ASSERT_VALUES_EQUAL(ChunkSize, diff);

        pool.Deallocate(first);
        pool.Deallocate(second);
    }

    Y_UNIT_TEST(FreedChunkIsReused)
    {
        constexpr size_t ChunkSize = 512;

        TArenaAllocatorPool pool(CreateArenaAllocator());

        void* ptr = pool.Allocate(ChunkSize);
        UNIT_ASSERT(ptr);
        pool.Deallocate(ptr);

        void* ptr2 = pool.Allocate(ChunkSize);
        UNIT_ASSERT_EQUAL(ptr, ptr2);

        pool.Deallocate(ptr2);
    }

    Y_UNIT_TEST(SlotIsReusedAfterFullRelease)
    {
        constexpr size_t SlotSize = 4096;
        constexpr size_t ChunkSize = 512;
        constexpr size_t ChunksPerSlot = SlotSize / ChunkSize;

        auto allocator = std::make_shared<TTrackingAllocator>();
        TArenaAllocatorPool pool(allocator);

        TVector<void*> ptrs;
        for (size_t i = 0; i < ChunksPerSlot; ++i) {
            ptrs.push_back(pool.Allocate(ChunkSize));
        }
        const void* slotBase = ptrs[0];

        for (void* ptr: ptrs) {
            pool.Deallocate(ptr);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());

        // The next allocation should re-acquire a slot and hand out
        // the same address again.
        void* ptr = pool.Allocate(ChunkSize);
        UNIT_ASSERT_EQUAL(slotBase, ptr);
        pool.Deallocate(ptr);
    }

    Y_UNIT_TEST(MultipleSlots)
    {
        constexpr size_t SlotSize = 4096;
        constexpr size_t ChunkSize = 512;
        constexpr size_t ChunksPerSlot = SlotSize / ChunkSize;

        auto allocator = std::make_shared<TTrackingAllocator>();
        TArenaAllocatorPool pool(allocator);

        TVector<void*> ptrs;
        for (size_t i = 0; i < 2 * ChunksPerSlot; ++i) {
            ptrs.push_back(pool.Allocate(ChunkSize));
        }
        UNIT_ASSERT_VALUES_EQUAL(2, allocator->AllocatedBlocks());

        // Free chunks from the second slot only - its slot should be
        // returned to the allocator while the first one stays alive.
        for (size_t i = ChunksPerSlot; i < ptrs.size(); ++i) {
            pool.Deallocate(ptrs[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, allocator->AllocatedBlocks());

        for (size_t i = 0; i < ChunksPerSlot; ++i) {
            pool.Deallocate(ptrs[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
    }

    Y_UNIT_TEST(FreedMemoryIsZeroedOnReuse)
    {
        constexpr size_t ChunkSize = 512;

        TArenaAllocatorPool pool(CreateArenaAllocator());

        // Allocate, write pattern, free.
        void* ptr1 = pool.Allocate(ChunkSize);
        UNIT_ASSERT(ptr1);
        std::memset(ptr1, 0xFF, ChunkSize);
        pool.Deallocate(ptr1);

        // Re-allocate and check zeroed.
        void* ptr2 = pool.Allocate(ChunkSize);
        UNIT_ASSERT_EQUAL(ptr1, ptr2);

        char* data = static_cast<char*>(ptr2);
        for (size_t i = 0; i < ChunkSize; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, static_cast<unsigned char>(data[i]));
        }

        pool.Deallocate(ptr2);
    }

    Y_UNIT_TEST(MultipleFreeAllocCyclesZeroed)
    {
        constexpr size_t ChunkSize = 1024;
        constexpr int Cycles = 5;

        TArenaAllocatorPool pool(CreateArenaAllocator());

        for (int cycle = 0; cycle < Cycles; ++cycle) {
            void* ptr = pool.Allocate(ChunkSize);
            UNIT_ASSERT(ptr);

            if (cycle > 0) {
                char* data = static_cast<char*>(ptr);
                for (size_t i = 0; i < ChunkSize; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        0,
                        static_cast<unsigned char>(data[i]));
                }
            }

            std::memset(ptr, 0xAA, ChunkSize);
            pool.Deallocate(ptr);
        }
    }

    Y_UNIT_TEST(ZeroedAfterSlotReclaimAndReallocate)
    {
        constexpr size_t SlotSize = 4096;
        constexpr size_t ChunkSize = 512;
        constexpr size_t ChunksPerSlot = SlotSize / ChunkSize;

        TArenaAllocatorPool pool(CreateArenaAllocator());

        TVector<void*> ptrs;
        ptrs.reserve(ChunksPerSlot);
        for (size_t i = 0; i < ChunksPerSlot; ++i) {
            ptrs.push_back(pool.Allocate(ChunkSize));
            UNIT_ASSERT(ptrs.back());
            std::memset(ptrs.back(), 0xFF, ChunkSize);
        }

        // Free everything — slot should be reclaimed.
        for (void* p: ptrs) {
            pool.Deallocate(p);
        }

        // Re-allocate and check zeroed.
        void* ptr = pool.Allocate(ChunkSize);
        UNIT_ASSERT(ptr);
        char* data = static_cast<char*>(ptr);
        for (size_t i = 0; i < ChunkSize; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, static_cast<unsigned char>(data[i]));
        }

        pool.Deallocate(ptr);
    }

    Y_UNIT_TEST(ZeroedAcrossMultipleSlots)
    {
        constexpr size_t SlotSize = 4096;
        constexpr size_t ChunkSize = 512;
        constexpr size_t ChunksPerSlot = SlotSize / ChunkSize;

        TArenaAllocatorPool pool(CreateArenaAllocator());

        // Allocate from two slots.
        TVector<void*> ptrs;
        for (size_t i = 0; i < 2 * ChunksPerSlot; ++i) {
            ptrs.push_back(pool.Allocate(ChunkSize));
            std::memset(ptrs.back(), 0xBB, ChunkSize);
        }

        // Free all from first slot — it gets reclaimed.
        for (size_t i = 0; i < ChunksPerSlot; ++i) {
            pool.Deallocate(ptrs[i]);
        }

        // Allocate in the reclaimed slot — should be zeroed.
        void* ptr = pool.Allocate(ChunkSize);
        UNIT_ASSERT(ptr);
        char* data = static_cast<char*>(ptr);
        for (size_t i = 0; i < ChunkSize; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, static_cast<unsigned char>(data[i]));
        }

        pool.Deallocate(ptr);
    }

    Y_UNIT_TEST(DifferentSizesGetDistinctSlots)
    {
        constexpr size_t SizeA = 128;
        constexpr size_t SizeB = 256;
        constexpr size_t SizeC = 512;

        auto allocator = std::make_shared<TTrackingAllocator>();
        TArenaAllocatorPool pool(allocator);

        // Allocate from three different sizes.
        TVector<void*> ptrsA;
        TVector<void*> ptrsB;
        TVector<void*> ptrsC;

        for (size_t i = 0; i < 4; ++i) {
            ptrsA.push_back(pool.Allocate(SizeA));
            ptrsB.push_back(pool.Allocate(SizeB));
            ptrsC.push_back(pool.Allocate(SizeC));
        }

        // All pointers should be unique across all sizes.
        std::unordered_set<void*> all(ptrsA.begin(), ptrsA.end());
        all.insert(ptrsB.begin(), ptrsB.end());
        all.insert(ptrsC.begin(), ptrsC.end());
        UNIT_ASSERT_VALUES_EQUAL(
            ptrsA.size() + ptrsB.size() + ptrsC.size(),
            all.size());

        // Each size should have its own slot.
        UNIT_ASSERT_VALUES_EQUAL(3, allocator->AllocatedBlocks());

        // Free all from one size — its slot should be reclaimed.
        for (void* ptr: ptrsA) {
            pool.Deallocate(ptr);
        }
        UNIT_ASSERT_VALUES_EQUAL(2, allocator->AllocatedBlocks());

        // Re-allocate same size — should reuse the reclaimed slot.
        void* ptrA = pool.Allocate(SizeA);
        UNIT_ASSERT_EQUAL(ptrsA[0], ptrA);

        // Free everything.
        for (void* ptr: ptrsB) {
            pool.Deallocate(ptr);
        }
        for (void* ptr: ptrsC) {
            pool.Deallocate(ptr);
        }
        pool.Deallocate(ptrA);
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
    }

    Y_UNIT_TEST(MixedSizeAllocFreeCycles)
    {
        constexpr size_t Sizes[] = {64, 128, 256, 512, 1024};
        constexpr size_t NumSizes = sizeof(Sizes) / sizeof(Sizes[0]);

        TArenaAllocatorPool pool(CreateArenaAllocator());

        TVector<TVector<void*>> ptrs(NumSizes);
        for (size_t i = 0; i < NumSizes; ++i) {
            for (size_t j = 0; j < 3; ++j) {
                ptrs[i].push_back(pool.Allocate(Sizes[i]));
            }
        }

        // Free and re-allocate in a mixed order.
        for (size_t round = 0; round < 2; ++round) {
            for (size_t i = 0; i < NumSizes; ++i) {
                for (auto* ptr: ptrs[i]) {
                    pool.Deallocate(ptr);
                }
                for (size_t j = 0; j < 3; ++j) {
                    ptrs[i][j] = pool.Allocate(Sizes[i]);
                }
            }
        }

        // Free everything.
        for (size_t i = 0; i < NumSizes; ++i) {
            for (auto* ptr: ptrs[i]) {
                pool.Deallocate(ptr);
            }
        }
    }
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
