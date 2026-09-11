#include "arena_allocator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/system/thread.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <thread>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(ArenaAllocatorTest)
{
    Y_UNIT_TEST(RoundAllocationSize)
    {
        UNIT_ASSERT_VALUES_EQUAL(4, RoundAllocationSize(0));
        UNIT_ASSERT_VALUES_EQUAL(4, RoundAllocationSize(4));
        UNIT_ASSERT_VALUES_EQUAL(8, RoundAllocationSize(5));
        UNIT_ASSERT_VALUES_EQUAL(128, RoundAllocationSize(128));
        UNIT_ASSERT_VALUES_EQUAL(144, RoundAllocationSize(129));
        UNIT_ASSERT_VALUES_EQUAL(512, RoundAllocationSize(512));
        UNIT_ASSERT_VALUES_EQUAL(544, RoundAllocationSize(513));
        UNIT_ASSERT_VALUES_EQUAL(10_KB, RoundAllocationSize(10_KB));
        UNIT_ASSERT_VALUES_EQUAL(10_KB + 64, RoundAllocationSize(10_KB + 1));
        UNIT_ASSERT_VALUES_EQUAL(128 * 1024, RoundAllocationSize(128 * 1024));
        UNIT_ASSERT_VALUES_EQUAL(
            128 * 1024 + 64,
            RoundAllocationSize(128 * 1024 + 1));
        UNIT_ASSERT_VALUES_EQUAL(256 * 1024, RoundAllocationSize(256 * 1024));
        UNIT_ASSERT_VALUES_EQUAL(
            256 * 1024 + 64,
            RoundAllocationSize(256 * 1024 + 1));
    }

    Y_UNIT_TEST(AllocateAllSizes)
    {
        auto allocator = CreateArenaAllocator();

        for (size_t size:
             {size_t(512), size_t(1024), size_t(2048), size_t(4096)})
        {
            void* ptr = allocator->Allocate(size);
            UNIT_ASSERT(ptr);
            UNIT_ASSERT_VALUES_EQUAL(size, allocator->AllocatedSize());
            allocator->DeAllocate(ptr);
            UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedSize());
        }
    }

    Y_UNIT_TEST(ReportsMemoryUsageBySlotSize)
    {
        auto allocator = CreateArenaAllocator();
        void* slot512 = allocator->Allocate(512);
        void* anotherSlot512 = allocator->Allocate(512);
        void* slot2048 = allocator->Allocate(2048);

        const auto stats = allocator->GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, stats.size());
        UNIT_ASSERT_VALUES_EQUAL(512, stats[0].SlotSize);
        UNIT_ASSERT_VALUES_EQUAL(1_MB, stats[0].ReservedSize);
        UNIT_ASSERT_VALUES_EQUAL(1024, stats[0].UsedSize);
        UNIT_ASSERT_VALUES_EQUAL(2, stats[0].Count);
        UNIT_ASSERT_VALUES_EQUAL(2048, stats[1].SlotSize);
        UNIT_ASSERT_VALUES_EQUAL(1_MB, stats[1].ReservedSize);
        UNIT_ASSERT_VALUES_EQUAL(2048, stats[1].UsedSize);
        UNIT_ASSERT_VALUES_EQUAL(1, stats[1].Count);

        allocator->DeAllocate(slot512);

        const auto statsAfterPartialDeallocation = allocator->GetStats();
        UNIT_ASSERT_VALUES_EQUAL(
            512,
            statsAfterPartialDeallocation[0].UsedSize);
        UNIT_ASSERT_VALUES_EQUAL(
            2048,
            statsAfterPartialDeallocation[1].UsedSize);

        allocator->DeAllocate(anotherSlot512);
        allocator->DeAllocate(slot2048);

        const auto statsAfterDeallocation = allocator->GetStats();
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfterDeallocation[0].UsedSize);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfterDeallocation[1].UsedSize);
    }

    Y_UNIT_TEST(SupportsLargeSlotSizes)
    {
        auto allocator = CreateArenaAllocator();

        for (size_t size: {128_KB, 2_MB, 3_MB + 1}) {
            void* ptr = allocator->Allocate(size);
            UNIT_ASSERT(ptr);
            UNIT_ASSERT_VALUES_EQUAL(
                RoundAllocationSize(size),
                allocator->AllocatedSize());

            const auto stats = allocator->GetStats();
            const size_t allocationSize = RoundAllocationSize(size);
            const size_t expectedBlockSize = Max(
                1_MB,
                (1_MB + allocationSize - 1) / allocationSize * allocationSize);
            bool found = false;
            for (const auto& stat: stats) {
                if (stat.SlotSize == allocationSize) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        expectedBlockSize,
                        stat.ReservedSize);
                    found = true;
                }
            }
            UNIT_ASSERT(found);

            allocator->DeAllocate(ptr);
            UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedSize());
        }
    }

    Y_UNIT_TEST(AllocationIsAligned)
    {
        constexpr size_t Alignment = 8;

        auto allocator = CreateArenaAllocator();

        for (size_t size:
             {size_t(512), size_t(1024), size_t(2048), size_t(4096)})
        {
            void* ptr = allocator->Allocate(size);
            UNIT_ASSERT(ptr);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                reinterpret_cast<uintptr_t>(ptr) % Alignment);
            allocator->DeAllocate(ptr);
        }
    }

    Y_UNIT_TEST(ReuseFreedSlot)
    {
        auto allocator = CreateArenaAllocator();

        void* ptr = allocator->Allocate(1024);
        void* livePtr = allocator->Allocate(1024);
        UNIT_ASSERT(ptr);
        UNIT_ASSERT(livePtr);
        allocator->DeAllocate(ptr);

        void* ptr2 = allocator->Allocate(1024);
        UNIT_ASSERT_EQUAL(ptr, ptr2);

        allocator->DeAllocate(livePtr);
        allocator->DeAllocate(ptr2);
    }

    Y_UNIT_TEST(ArenaIsReallocatedAfterFullRelease)
    {
        auto allocator = CreateArenaAllocator();

        TVector<void*> ptrs;
        for (size_t i = 0; i < 100; ++i) {
            ptrs.push_back(allocator->Allocate(512));
        }
        UNIT_ASSERT_VALUES_EQUAL(100, allocator->AllocatedBlocks());
        UNIT_ASSERT_VALUES_EQUAL(100 * 512, allocator->AllocatedSize());

        // All pointers should be unique.
        std::unordered_set<void*> unique(ptrs.begin(), ptrs.end());
        UNIT_ASSERT_VALUES_EQUAL(100, unique.size());

        for (void* ptr: ptrs) {
            allocator->DeAllocate(ptr);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedSize());

        // After freeing everything a new arena can be allocated at any
        // address.
        void* ptr = allocator->Allocate(512);
        UNIT_ASSERT(ptr);
        UNIT_ASSERT_VALUES_EQUAL(512, allocator->AllocatedSize());
        allocator->DeAllocate(ptr);
    }

    Y_UNIT_TEST(Multithreaded)
    {
        auto allocator = CreateArenaAllocator();

        constexpr int ThreadCount = 8;
        constexpr int Iterations = 10000;

        std::atomic<int> failed{0};

        TVector<std::thread> threads;
        for (int t = 0; t < ThreadCount; ++t) {
            threads.emplace_back(
                [&, t]
                {
                    try {
                        for (int i = 0; i < Iterations; ++i) {
                            const size_t size = size_t(512) << ((t + i) % 4);
                            void* ptr = allocator->Allocate(size);
                            if (!ptr) {
                                failed++;
                                return;
                            }
                            allocator->DeAllocate(ptr);
                        }
                    } catch (...) {
                        failed++;
                    }
                });
        }

        for (auto& thread: threads) {
            thread.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(0, failed.load());
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedSize());
    }

    Y_UNIT_TEST(FreedMemoryIsZeroedOnReuse)
    {
        constexpr size_t Size = 1024;

        auto allocator = CreateArenaAllocator();

        // Keep another slot alive so that the arena itself is not released.
        void* ptr1 = allocator->Allocate(Size);
        void* livePtr = allocator->Allocate(Size);
        UNIT_ASSERT(ptr1);
        UNIT_ASSERT(livePtr);
        std::memset(ptr1, 0xFF, Size);
        allocator->DeAllocate(ptr1);

        // Re-allocate the same slot and check it is zeroed.
        void* ptr2 = allocator->Allocate(Size);
        UNIT_ASSERT_EQUAL(ptr1, ptr2);

        char* data = static_cast<char*>(ptr2);
        for (size_t i = 0; i < Size; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, static_cast<unsigned char>(data[i]));
        }

        allocator->DeAllocate(livePtr);
        allocator->DeAllocate(ptr2);
    }

    Y_UNIT_TEST(AllSlotSizesMemoryIsZeroedOnReuse)
    {
        constexpr size_t SlotSizes[] = {512, 1024, 2048, 4096};

        auto allocator = CreateArenaAllocator();

        for (size_t slotSize: SlotSizes) {
            // Keep another slot alive so that the arena itself is not
            // released.
            void* ptr1 = allocator->Allocate(slotSize);
            void* livePtr = allocator->Allocate(slotSize);
            UNIT_ASSERT(ptr1);
            UNIT_ASSERT(livePtr);
            std::memset(ptr1, 0xFF, slotSize);
            allocator->DeAllocate(ptr1);

            // Second allocation: must be zeroed.
            void* ptr2 = allocator->Allocate(slotSize);
            UNIT_ASSERT_EQUAL(ptr1, ptr2);

            char* data = static_cast<char*>(ptr2);
            for (size_t i = 0; i < slotSize; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    0,
                    static_cast<unsigned char>(data[i]));
            }

            allocator->DeAllocate(livePtr);
            allocator->DeAllocate(ptr2);
        }
    }

    Y_UNIT_TEST(MultipleFreeAllocCyclesZeroed)
    {
        constexpr size_t Size = 2048;
        constexpr int Cycles = 5;

        auto allocator = CreateArenaAllocator();

        for (int cycle = 0; cycle < Cycles; ++cycle) {
            void* ptr = allocator->Allocate(Size);
            UNIT_ASSERT(ptr);

            if (cycle > 0) {
                // After the first cycle, memory should be zeroed.
                char* data = static_cast<char*>(ptr);
                for (size_t i = 0; i < Size; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(
                        0,
                        static_cast<unsigned char>(data[i]));
                }
            }

            // Write a non-zero pattern before freeing.
            std::memset(ptr, 0xAA, Size);
            allocator->DeAllocate(ptr);
        }
    }

    Y_UNIT_TEST(ZeroedAfterBlockReclaimAndReallocate)
    {
        // Fill a block, free everything so the block is returned to the
        // system, then re-allocate: the new block should be fresh (zeroed).
        constexpr size_t Size = 512;
        constexpr size_t BlockSlots = 256;   // BlockSize(1MB) / 512

        auto allocator = CreateArenaAllocator();

        TVector<void*> ptrs;
        ptrs.reserve(BlockSlots);
        for (size_t i = 0; i < BlockSlots; ++i) {
            ptrs.push_back(allocator->Allocate(Size));
            UNIT_ASSERT(ptrs.back());
            std::memset(ptrs.back(), 0xFF, Size);
        }

        // Free everything — the block should be reclaimed.
        for (void* p: ptrs) {
            allocator->DeAllocate(p);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedBlocks());
        UNIT_ASSERT_VALUES_EQUAL(0, allocator->AllocatedSize());

        // Re-allocate one chunk and check it's zeroed.
        void* ptr = allocator->Allocate(Size);
        UNIT_ASSERT(ptr);
        char* data = static_cast<char*>(ptr);
        for (size_t i = 0; i < Size; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(0, static_cast<unsigned char>(data[i]));
        }

        allocator->DeAllocate(ptr);
    }
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
