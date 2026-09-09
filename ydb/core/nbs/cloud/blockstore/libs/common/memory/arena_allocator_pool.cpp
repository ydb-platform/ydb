#include "arena_allocator_pool.h"

#include <util/generic/algorithm.h>
#include <util/system/yassert.h>

#include <cstring>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

namespace {
constexpr size_t DefaultSlotSize = 4096;
}   // namespace

//////////////////////////////////////////////////////////////////////////////

TArenaAllocatorPool::TSlot::TSlot(
    IArenaAllocatorPtr allocator,
    size_t slotSize,
    size_t chunkSize)
    : Base(allocator->Allocate(slotSize))
    , Allocator(std::move(allocator))
    , SlotSize(slotSize)
    , ChunkSize(chunkSize)
    , MaxChunkCapacity(slotSize / chunkSize)
{
    Y_ABORT_UNLESS(Base);
}

TArenaAllocatorPool::TSlot::~TSlot()
{
    if (Base) {
        Allocator->DeAllocate(Base);
    }
}

void* TArenaAllocatorPool::TSlot::Allocate()
{
    if (FreeList) {
        TFreeChunk* result = FreeList;
        FreeList = FreeList->Next;
        std::memset(result, 0, ChunkSize);
        --FreeCount;
        return result;
    }
    if (AllocatedChunks == MaxChunkCapacity) {
        return nullptr;
    }
    return static_cast<char*>(Base) + AllocatedChunks++ * ChunkSize;
}

void TArenaAllocatorPool::TSlot::Free(void* chunk) noexcept
{
    auto* ptr = static_cast<TFreeChunk*>(chunk);
    ptr->Next = FreeList;
    FreeList = ptr;
    ++FreeCount;
}

bool TArenaAllocatorPool::TSlot::Full() const noexcept
{
    return AllocatedChunks == MaxChunkCapacity && FreeCount == 0;
}

bool TArenaAllocatorPool::TSlot::Empty() const noexcept
{
    return FreeCount == AllocatedChunks;
}

//////////////////////////////////////////////////////////////////////////////

TArenaAllocatorPool::TSlot* TArenaAllocatorPool::TSlots::Acquire(
    IArenaAllocatorPtr allocator,
    size_t chunkSize,
    size_t slotSize)
{
    if (!SlotSize) {
        SlotSize = slotSize ? slotSize : Max(DefaultSlotSize, chunkSize);
    }

    Slots.emplace_back(std::move(allocator), SlotSize, chunkSize);
    CurrentSlot = &Slots.back();
    return CurrentSlot;
}

void TArenaAllocatorPool::TSlots::Release(TSlot* slot) noexcept
{
    if (CurrentSlot == slot) {
        CurrentSlot = nullptr;
    }
    Slots.remove_if([slot](const TSlot& s) { return &s == slot; });
}

size_t TArenaAllocatorPool::TSlots::GetAllocatedSize() const
{
    return Slots.size() * SlotSize;
}

//////////////////////////////////////////////////////////////////////////////

TArenaAllocatorPool::TArenaAllocatorPool(
    IArenaAllocatorPtr allocator,
    size_t slotSize)
    : Allocator(std::move(allocator))
    , SlotSize(slotSize)
{
    Y_ABORT_UNLESS(Allocator);
}

TArenaAllocatorPool::~TArenaAllocatorPool() = default;

void* TArenaAllocatorPool::Allocate(size_t size)
{
    auto& slots = SizeMap[size];
    if (!slots.CurrentSlot) {
        auto* slot = slots.Acquire(Allocator, size, SlotSize);
        Bases.emplace(slot->Base, slot);
    }

    if (void* ptr = slots.CurrentSlot->Allocate()) {
        UsedSize += size;
        return ptr;
    }

    auto* slot = slots.Acquire(Allocator, size, SlotSize);
    Bases.emplace(slot->Base, slot);
    void* ptr = slot->Allocate();
    Y_ABORT_UNLESS(ptr);
    UsedSize += size;
    return ptr;
}

void TArenaAllocatorPool::Deallocate(void* ptr) noexcept
{
    if (!ptr) {
        return;
    }

    // Find the slot whose [Base, Base + SlotSize) range contains ptr:
    // it is the slot with the greatest base <= ptr.
    auto it = Bases.upper_bound(ptr);
    if (it == Bases.begin()) {
        Y_ABORT_UNLESS(false, "Deallocate: unknown pointer");
    }
    --it;
    auto* slot = it->second;

    Y_ABORT_UNLESS(
        static_cast<char*>(ptr) <
            static_cast<char*>(slot->Base) + slot->SlotSize,
        "Deallocate: unknown pointer");

    UsedSize -= slot->ChunkSize;
    slot->Free(ptr);

    if (slot->Empty()) {
        Bases.erase(it);
        auto& slots = SizeMap[slot->ChunkSize];
        slots.Release(slot);
        if (slots.Empty()) {
            SizeMap.erase(slot->ChunkSize);
        }
    }
}

size_t TArenaAllocatorPool::GetAllocatedSize() const
{
    size_t result = 0;
    for (const auto& [size, slots]: SizeMap) {
        Y_UNUSED(size);
        result += slots.GetAllocatedSize();
    }
    return result;
}

size_t TArenaAllocatorPool::GetUsedSize() const
{
    return UsedSize;
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
