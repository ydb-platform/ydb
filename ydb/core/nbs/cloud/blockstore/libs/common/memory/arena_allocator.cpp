#include "arena_allocator.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/generic/algorithm.h>
#include <util/generic/bitops.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/map.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>

namespace NYdb::NBS::NBlockStore {

namespace {

//////////////////////////////////////////////////////////////////////////////

constexpr size_t MinArenaSize = 1_MB;
constexpr size_t Alignment = 8;

size_t RoundUp(size_t size, size_t alignment)
{
    const size_t remainder = size % alignment;
    return remainder ? size + alignment - remainder : size;
}

size_t CalculateArenaSize(size_t slotSize)
{
    const size_t slotCount = (MinArenaSize + slotSize - 1) / slotSize;
    return slotCount * slotSize;
}

//////////////////////////////////////////////////////////////////////////////

class TArena;
using TBase = void*;
using TArenaPtr = TArena*;

//////////////////////////////////////////////////////////////////////////////

void* AlignedAlloc(size_t size, size_t alignment)
{
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0) {
        return nullptr;
    }
    return ptr;
}

void AlignedFree(void* ptr)
{
    free(ptr);
}

////////////////////////////////////////////////////////////////////////////////

// Owns one contiguous memory arena split into fixed-size slots.
class TArena
{
    struct TSlot
    {
        TSlot* Next = nullptr;
    };

public:
    explicit TArena(size_t slotSize)
        : ArenaSize(CalculateArenaSize(slotSize))
        , Base(AlignedAlloc(ArenaSize, Alignment))
        , SlotSize(slotSize)
        , SlotsPerArena(ArenaSize / slotSize)
    {
        std::memset(Base, 0, ArenaSize);
    }

    ~TArena()
    {
        AlignedFree(Base);
    }

    [[nodiscard]] TBase GetBase() const
    {
        return Base;
    }

    [[nodiscard]] size_t GetSlotSize() const
    {
        return SlotSize;
    }

    [[nodiscard]] size_t GetArenaSize() const
    {
        return ArenaSize;
    }

    [[nodiscard]] bool Contains(void* ptr) const
    {
        return static_cast<char*>(ptr) >= static_cast<char*>(Base) &&
               static_cast<char*>(ptr) < static_cast<char*>(Base) + ArenaSize;
    }

    void* Allocate()
    {
        void* result = nullptr;
        if (FreeList) {
            result = FreeList;
            FreeList = FreeList->Next;
            std::memset(result, 0, SlotSize);
            --FreeCount;
        } else {
            if (AllocatedSlots == SlotsPerArena) {
                return nullptr;
            }
            result = static_cast<char*>(Base) + AllocatedSlots++ * SlotSize;
        }

        MaxAllocatedSlots = Max(MaxAllocatedSlots, AllocatedSlots - FreeCount);
        return result;
    }

    bool Deallocate(void* slot)
    {
        TSlot* ptr = static_cast<TSlot*>(slot);
        ptr->Next = FreeList;
        FreeList = ptr;
        ++FreeCount;
        return Empty();
    }

    [[nodiscard]] bool Empty() const
    {
        return FreeCount == AllocatedSlots;
    }

    [[nodiscard]] size_t GetMaxAllocatedSlots() const
    {
        return MaxAllocatedSlots;
    }

private:
    const size_t ArenaSize = 0;
    const TBase Base = nullptr;
    const size_t SlotSize = 0;
    const size_t SlotsPerArena = 0;
    size_t AllocatedSlots = 0;
    TSlot* FreeList = nullptr;
    size_t FreeCount = 0;
    size_t MaxAllocatedSlots = 0;
};

// Manages arenas that contain slots of the same size.
class TArenaList
{
public:
    explicit TArenaList(size_t slotSize)
        : SlotSize(slotSize)
        , ArenaSize(CalculateArenaSize(slotSize))
    {}

    void* Allocate(TArenaPtr* arena)
    {
        ++AllocateCount;

        if (LastUsed) {
            if (auto* result = LastUsed->Allocate()) {
                return result;
            }
        }

        for (auto& arena: Arenas) {
            if (auto* result = arena.Allocate()) {
                LastUsed = &arena;
                return result;
            }
        }

        Arenas.emplace_back(SlotSize);
        LastUsed = &Arenas.back();
        *arena = LastUsed;
        return LastUsed->Allocate();
    }

    bool Deallocate(TArenaPtr arena, void* ptr)
    {
        Y_ABORT_UNLESS(arena->Contains(ptr), "Deallocate: unknown pointer");
        ++DeallocateCount;

        if (arena->Deallocate(ptr)) {
            FreeArena(arena);
            return true;
        }
        return false;
    }

    [[nodiscard]] size_t GetAllocatedSize() const
    {
        return (AllocateCount - DeallocateCount) * SlotSize;
    }

    [[nodiscard]] size_t GetReservedSize() const
    {
        return Arenas.size() * ArenaSize;
    }

    [[nodiscard]] size_t GetSlotSize() const
    {
        return SlotSize;
    }

    [[nodiscard]] TArenaAllocatorStats GetStats() const
    {
        return TArenaAllocatorStats{
            .SlotSize = SlotSize,
            .ArenaSize = ArenaSize,
            .ReservedSize = GetReservedSize(),
            .UsedSize = GetAllocatedSize(),
            .MaxUsedSize = Max(MaxUsedSize, GetMaxUsedSize()),
            .Count = AllocateCount};
    }

private:
    [[nodiscard]] size_t GetMaxUsedSize() const
    {
        size_t result = 0;
        for (const auto& arena: Arenas) {
            result += arena.GetMaxAllocatedSlots() * SlotSize;
        }
        return result;
    }

    void FreeArena(TArenaPtr arena)
    {
        if (arena == LastUsed) {
            LastUsed = nullptr;
        }
        for (auto it = Arenas.begin(); it != Arenas.end(); ++it) {
            if (&*it == arena) {
                MaxUsedSize = Max(MaxUsedSize, GetMaxUsedSize());
                Arenas.erase(it);
                return;
            }
        }
        Y_ABORT_UNLESS(false, "FreeArena: unknown arena");
    }

    const size_t SlotSize = 0;
    const size_t ArenaSize = 0;
    TList<TArena> Arenas;
    TArena* LastUsed = nullptr;
    size_t AllocateCount = 0;
    size_t DeallocateCount = 0;
    size_t MaxUsedSize = 0;
};

///////////////////////////////////////////////////////////////////////////

// Thread-safe allocator that routes allocations to size-specific arena lists.
class TArenaAllocator final
    : public IArenaAllocator
    , public TDisableCopyMove
{
public:
    void* Allocate(size_t size) override
    {
        with_lock (Mutex) {
            ++AllocatedAllocationCount;
            TArenaPtr newArena = nullptr;
            void* result =
                GetArenaList(RoundAllocationSize(size)).Allocate(&newArena);
            if (newArena) {
                Bases.emplace(newArena->GetBase(), newArena);
            }
            return result;
        }
    }

    void DeAllocate(void* ptr) override
    {
        if (!ptr) {
            return;
        }

        with_lock (Mutex) {
            --AllocatedAllocationCount;
            // Find the arena whose [Base, Base + ArenaSize) range contains
            // ptr: it is the arena with the greatest base <= ptr.
            auto it = Bases.upper_bound(ptr);
            if (it == Bases.begin()) {
                // Unknown pointer.
                Y_ABORT_UNLESS(false, "DeAllocate: unknown pointer");
            }
            --it;
            TArena* arena = it->second;
            auto& arenaList = GetArenaList(arena->GetSlotSize());
            if (arenaList.Deallocate(arena, ptr)) {
                Bases.erase(it);
            }
        }
    }

    [[nodiscard]] size_t AllocatedBlocks() const override
    {
        with_lock (Mutex) {
            return AllocatedAllocationCount;
        }
    }

    [[nodiscard]] size_t AllocatedSize() const override
    {
        with_lock (Mutex) {
            return Accumulate(
                ArenasBySlotSize,
                0,
                [](size_t result, const auto& entry)
                { return result + entry.second.GetAllocatedSize(); });
        }
    }

    [[nodiscard]] size_t UsedSize() const override
    {
        return AllocatedSize();
    }

    [[nodiscard]] TVector<TArenaAllocatorStats> GetStats() const override
    {
        with_lock (Mutex) {
            TVector<TArenaAllocatorStats> result;
            result.reserve(ArenasBySlotSize.size());
            for (const auto& entry: ArenasBySlotSize) {
                result.push_back(entry.second.GetStats());
            }
            Sort(
                result,
                [](const auto& lhs, const auto& rhs)
                { return lhs.SlotSize < rhs.SlotSize; });
            return result;
        }
    }

private:
    TArenaList& GetArenaList(size_t slotSize)
    {
        return ArenasBySlotSize.try_emplace(slotSize, TArenaList{slotSize})
            .first->second;
    }

    TMutex Mutex;
    TMap<TBase, TArena*> Bases;
    THashMap<size_t, TArenaList> ArenasBySlotSize;
    size_t AllocatedAllocationCount = 0;
};

}   // namespace

//////////////////////////////////////////////////////////////////////////////

IArenaAllocatorPtr CreateArenaAllocator()
{
    return std::make_shared<TArenaAllocator>();
}

size_t RoundAllocationSize(size_t size)
{
    if (size <= 4) {
        return 4;
    }
    if (size <= 128) {
        return RoundUp(size, 8);
    }
    if (size <= 512) {
        return RoundUp(size, 16);
    }
    if (size <= 10_KB) {
        return RoundUp(size, 32);
    }
    return RoundUp(size, 64);
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
