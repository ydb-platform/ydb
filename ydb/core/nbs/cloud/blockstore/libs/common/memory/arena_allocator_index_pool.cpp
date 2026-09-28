#include "arena_allocator_index_pool.h"

#include <util/system/yassert.h>

#include <cstring>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

TArenaAllocatorIndexPool::TSlot::TSlot(
    IArenaAllocatorPtr allocator,
    size_t slotIndex,
    size_t slotSize,
    size_t chunksPerSlot,
    size_t chunkSize)
    : Base(allocator->Allocate(slotSize))
    , Allocator(std::move(allocator))
    , BaseIndex(slotIndex * chunksPerSlot)
    , ChunksPerSlot(chunksPerSlot)
    , ChunkSize(chunkSize)
{
    Y_ABORT_UNLESS(Base);
}

TArenaAllocatorIndexPool::TSlot::~TSlot()
{
    if (Base) {
        Allocator->DeAllocate(Base);
    }
}

ui64 TArenaAllocatorIndexPool::TSlot::Allocate()
{
    if (FreeList) {
        TFreeChunk* result = FreeList;
        FreeList = FreeList->Next;
        std::memset(result, 0, ChunkSize);
        --FreeCount;
        return GetIndex(result);
    }
    if (AllocatedChunks == ChunksPerSlot) {
        return InvalidIndex;
    }

    ui64 index = AllocatedChunks++;
    void* ptr = static_cast<char*>(Base) + (index * ChunkSize);
    std::memset(ptr, 0, ChunkSize);
    return index + BaseIndex;
}

void TArenaAllocatorIndexPool::TSlot::Free(ui64 index) noexcept
{
    TFreeChunk* ptr = GetAddress(index);
    ptr->Next = FreeList;
    FreeList = ptr;
    ++FreeCount;
}

bool TArenaAllocatorIndexPool::TSlot::Full() const noexcept
{
    return AllocatedChunks == ChunksPerSlot && FreeCount == 0;
}

bool TArenaAllocatorIndexPool::TSlot::Empty() const noexcept
{
    return FreeCount == AllocatedChunks;
}

size_t TArenaAllocatorIndexPool::TSlot::AllocatedCount() const noexcept
{
    return AllocatedChunks - FreeCount;
}

TArenaAllocatorIndexPool::TFreeChunk*
TArenaAllocatorIndexPool::TSlot::GetAddress(ui64 index) const noexcept
{
    return reinterpret_cast<TFreeChunk*>(
        static_cast<char*>(Base) + ((index - BaseIndex) * ChunkSize));
}

ui64 TArenaAllocatorIndexPool::TSlot::GetIndex(
    const TFreeChunk* chunk) const noexcept
{
    return BaseIndex +
           (reinterpret_cast<const char*>(chunk) - static_cast<char*>(Base)) /
               ChunkSize;
}

//////////////////////////////////////////////////////////////////////////////

TArenaAllocatorIndexPool::TArenaAllocatorIndexPool(
    IArenaAllocatorPtr allocator,
    size_t slotSize,
    size_t maxSizeBytes,
    size_t chunkSize)
    : Allocator(allocator)
    , SlotSize(slotSize)
    , ChunkSize(chunkSize)
    , ChunksPerSlot(SlotSize / ChunkSize)
    , MaxChunks(maxSizeBytes / ChunkSize)
{
    Y_ABORT_UNLESS(Allocator);
    Y_ABORT_UNLESS(SlotSize);
    Y_ABORT_UNLESS(ChunkSize);
    Y_ABORT_UNLESS(ChunkSize <= SlotSize);
    Y_ABORT_UNLESS(SlotSize % ChunkSize == 0);
    // ui16 index: slotIndex * ChunksPerSlot + chunkIndex must fit.
    Y_ABORT_UNLESS(MaxChunks <= 65534);
}

TArenaAllocatorIndexPool::~TArenaAllocatorIndexPool() = default;

ui64 TArenaAllocatorIndexPool::Allocate()
{
    if (UsedChunks >= MaxChunks) {
        return InvalidIndex;
    }

    if (!CurrentSlot) {
        AcquireSlot();
    }

    if (ui64 result = CurrentSlot->Allocate(); result != InvalidIndex) {
        ++UsedChunks;
        ++AllocationCount;
        return result;
    }

    AcquireSlot();
    if (!CurrentSlot) {
        // The pool is exhausted.
        return InvalidIndex;
    }
    const ui64 result = CurrentSlot->Allocate();
    Y_ABORT_UNLESS(result != InvalidIndex);
    ++UsedChunks;
    ++AllocationCount;
    return result;
}

void TArenaAllocatorIndexPool::Deallocate(ui64 index) noexcept
{
    if (index == InvalidIndex) {
        return;
    }

    const size_t slotIndex = index / ChunksPerSlot;
    auto& slot = *Slots[slotIndex];
    --UsedChunks;
    slot.Free(index);
    if (slot.Empty()) {
        Slots[slotIndex].reset();
        CurrentSlot = nullptr;
    }
}

void TArenaAllocatorIndexPool::DeallocateAll() noexcept
{
    Slots.clear();
    CurrentSlot = nullptr;
    UsedChunks = 0;
}

size_t TArenaAllocatorIndexPool::GetAllocatedCount() const
{
    size_t result = 0;
    for (const auto& slot: Slots) {
        if (slot) {
            result += slot->AllocatedCount();
        }
    }
    return result;
}

TArenaPoolStats TArenaAllocatorIndexPool::GetMemoryStats() const
{
    size_t reservedSize = 0;
    for (const auto& slot: Slots) {
        if (slot) {
            reservedSize += SlotSize;
        }
    }
    return {
        .ReservedSize = reservedSize,
        .UsedSize = GetUsedSize(),
        .AllocationCount = GetAllocatedCount(),
    };
}

size_t TArenaAllocatorIndexPool::GetUsedSize() const
{
    return UsedChunks * ChunkSize;
}

void* TArenaAllocatorIndexPool::GetChunkAddress(ui64 index) const noexcept
{
    const size_t slotIndex = index / ChunksPerSlot;
    return Slots[slotIndex]->GetAddress(index);
}

void TArenaAllocatorIndexPool::AcquireSlot()
{
    // Try to find an existing slot with free chunks.
    for (const auto& slot: Slots) {
        if (slot && !slot->Full()) {
            CurrentSlot = slot.get();
            return;
        }
    }

    for (size_t i = 0; i < Slots.size(); ++i) {
        if (Slots[i]) {
            continue;
        }

        Slots[i] = std::make_unique<TSlot>(
            Allocator,
            i,
            SlotSize,
            ChunksPerSlot,
            ChunkSize);
        CurrentSlot = Slots[i].get();
        return;
    }

    if (Slots.size() * ChunksPerSlot >= MaxChunks) {
        CurrentSlot = nullptr;
        return;
    }

    // Allocate a new slot.
    Slots.push_back(std::make_unique<TSlot>(
        Allocator,
        Slots.size(),
        SlotSize,
        ChunksPerSlot,
        ChunkSize));
    CurrentSlot = Slots.back().get();
}

void TArenaAllocatorIndexPool::ReleaseSlot(size_t /*slotIndex*/) noexcept
{
    // Slots are kept alive for the lifetime of the pool so that ui16 indices
    // remain stable. Memory is returned to the underlying allocator when the
    // pool is destroyed.
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
