#pragma once

#include "arena_allocator.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/generic/vector.h>

#include <cstddef>
#include <memory>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

// A fixed-size-chunk pool that hands out ui64 indices instead of pointers.
// The index encodes the slot and the chunk within the slot, so stable chunk
// addresses are guaranteed by keeping slots in a vector of unique_ptrs.
class TArenaAllocatorIndexPool: TDisableCopyMove
{
public:
    static constexpr ui64 InvalidIndex = 0xFFFFFFFF;

    explicit TArenaAllocatorIndexPool(
        IArenaAllocatorPtr allocator,
        size_t slotSize,
        size_t maxSizeBytes,
        size_t chunkSize);
    ~TArenaAllocatorIndexPool();

    // Returns InvalidIndex if the pool is exhausted.
    [[nodiscard]] ui64 Allocate();
    void Deallocate(ui64 index) noexcept;
    void DeallocateAll() noexcept;

    template <typename T>
    [[nodiscard]] T* GetAddress(ui64 index) const noexcept
    {
        if (index == InvalidIndex) {
            return nullptr;
        }
        return static_cast<T*>(GetChunkAddress(index));
    }

    template <typename T>
    [[nodiscard]] T* GetAddress(ui64 index) noexcept
    {
        if (index == InvalidIndex) {
            return nullptr;
        }
        return static_cast<T*>(GetChunkAddress(index));
    }

    [[nodiscard]] size_t GetAllocatedCount() const;
    [[nodiscard]] TArenaPoolStats GetMemoryStats() const;
    [[nodiscard]] size_t GetUsedSize() const;

private:
    // Intrusive free list node stored in the first bytes of a free chunk
    // (same trick as in arena_allocator_pool.cpp).
    struct TFreeChunk
    {
        TFreeChunk* Next = nullptr;
    };

    // A slot allocates its chunks itself, carving them sequentially from
    // the base pointer and keeping freed chunks in an intrusive free list.
    // The slot owns its memory and returns it to the allocator in its
    // destructor.
    struct TSlot
    {
        void* Base = nullptr;
        IArenaAllocatorPtr Allocator;
        const size_t BaseIndex = 0;
        const size_t ChunksPerSlot = 0;
        const size_t ChunkSize = 0;
        size_t AllocatedChunks = 0;   // chunks carved so far
        TFreeChunk* FreeList = nullptr;
        size_t FreeCount = 0;

        TSlot(
            IArenaAllocatorPtr allocator,
            size_t slotIndex,
            size_t slotSize,
            size_t chunksPerSlot,
            size_t chunkSize);
        ~TSlot();

        ui64 Allocate();
        void Free(ui64 index) noexcept;
        [[nodiscard]] bool Full() const noexcept;
        [[nodiscard]] bool Empty() const noexcept;
        [[nodiscard]] size_t AllocatedCount() const noexcept;
        [[nodiscard]] TFreeChunk* GetAddress(ui64 index) const noexcept;
        [[nodiscard]] ui64 GetIndex(const TFreeChunk* chunk) const noexcept;
    };

    [[nodiscard]] void* GetChunkAddress(ui64 index) const noexcept;

    void AcquireSlot();
    void ReleaseSlot(size_t slotIndex) noexcept;

    IArenaAllocatorPtr Allocator;
    size_t SlotSize;
    size_t ChunkSize;
    size_t ChunksPerSlot;
    // Upper bound for the total number of chunks in the pool.
    size_t MaxChunks;
    size_t UsedChunks = 0;
    size_t AllocationCount = 0;

    TVector<std::unique_ptr<TSlot>> Slots;
    // Slot currently being carved into chunks.
    TSlot* CurrentSlot = nullptr;
};

/////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
