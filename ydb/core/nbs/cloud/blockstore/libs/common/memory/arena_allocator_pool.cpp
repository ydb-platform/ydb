#include "arena_allocator_pool.h"

#include <util/generic/algorithm.h>
#include <util/system/yassert.h>

#include <cstring>
#include <memory>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

// TODO: Move this implementation back to arena_allocator.cpp after fixing
// the final static link order for users of TArenaPoolStats.
void TArenaPoolStats::Aggregate(const TArenaPoolStats& stats)
{
    ReservedSize += stats.ReservedSize;
    UsedSize += stats.UsedSize;
    AllocationCount += stats.AllocationCount;
}

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

IArenaAllocatorPtr TArenaAllocatorPool::GetAllocator() const
{
    return Allocator;
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

TArenaAllocatorStats TArenaAllocatorPool::TSlots::GetStats(
    size_t chunkSize) const
{
    return {
        .SlotSize = chunkSize,
        .ArenaSize = SlotSize,
        .ReservedSize = GetAllocatedSize(),
        .UsedSize = UsedSize,
        .MaxUsedSize = MaxUsedSize,
        .Count = AllocationCount};
}

void TArenaAllocatorPool::TSlots::OnAllocate(size_t chunkSize)
{
    UsedSize += chunkSize;
    MaxUsedSize = Max(MaxUsedSize, UsedSize);
    ++AllocationCount;
}

void TArenaAllocatorPool::TSlots::OnDeallocate(size_t chunkSize)
{
    UsedSize -= chunkSize;
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
    size = RoundAllocationSize(size);

    auto& slots = SizeMap[size];
    if (!slots.CurrentSlot) {
        auto* slot = slots.Acquire(Allocator, size, SlotSize);
        Bases.emplace(slot->Base, slot);
    }

    if (void* ptr = slots.CurrentSlot->Allocate()) {
        slots.OnAllocate(size);
        UsedSize += size;
        return ptr;
    }

    auto* slot = slots.Acquire(Allocator, size, SlotSize);
    Bases.emplace(slot->Base, slot);
    void* ptr = slot->Allocate();
    Y_ABORT_UNLESS(ptr);
    slots.OnAllocate(size);
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
    const size_t chunkSize = slot->ChunkSize;

    Y_ABORT_UNLESS(
        static_cast<char*>(ptr) <
            static_cast<char*>(slot->Base) + slot->SlotSize,
        "Deallocate: unknown pointer");

    UsedSize -= chunkSize;
    auto& slots = SizeMap[chunkSize];
    slots.OnDeallocate(chunkSize);
    slot->Free(ptr);

    if (slot->Empty()) {
        Bases.erase(it);
        slots.Release(slot);
    }
}

TArenaPoolStats TArenaAllocatorPool::GetMemoryStats() const
{
    size_t reservedSize = 0;
    size_t allocationCount = 0;
    for (const auto& [chunkSize, slots]: SizeMap) {
        reservedSize += slots.GetAllocatedSize();
        allocationCount += slots.GetStats(chunkSize).Count;
    }
    return {
        .ReservedSize = reservedSize,
        .UsedSize = UsedSize,
        .AllocationCount = allocationCount,
    };
}

size_t TArenaAllocatorPool::GetUsedSize() const
{
    return UsedSize;
}

TVector<TArenaAllocatorStats> TArenaAllocatorPool::GetDetailedStat() const
{
    TVector<TArenaAllocatorStats> result;
    result.reserve(SizeMap.size());
    for (const auto& [chunkSize, slots]: SizeMap) {
        result.push_back(slots.GetStats(chunkSize));
    }
    return result;
}

//////////////////////////////////////////////////////////////////////////////

TArenaAllocatorPoolPtr CreateArenaAllocatorPool()
{
    return std::make_shared<TArenaAllocatorPool>(CreateArenaAllocator());
}

//////////////////////////////////////////////////////////////////////////////
}   // namespace NYdb::NBS::NBlockStore
