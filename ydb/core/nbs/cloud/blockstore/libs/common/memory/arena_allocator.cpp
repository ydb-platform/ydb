#include "arena_allocator.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/generic/algorithm.h>
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

constexpr size_t BlockSize = 1_MB;
constexpr size_t MinSlotSize = 256;
constexpr size_t MaxSlotSize = 4096;
constexpr size_t SlotSizeCount = 5;   // 256, 512, 1024, 2048, 4096

//////////////////////////////////////////////////////////////////////////////

class TBlock;
using TBase = void*;
using TBlockPtr = TBlock*;
using TBlocks = TMap<void*, TBlock*>;

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

size_t SlotIndexForSize(size_t size)
{
    // Only exact power-of-two slot sizes within [MinSlotSize, MaxSlotSize]
    // are allowed.
    Y_ABORT_UNLESS(
        size >= MinSlotSize && size <= MaxSlotSize && (size & (size - 1)) == 0);
    return size == MinSlotSize
               ? 0
               : static_cast<size_t>(__builtin_ctzll(size)) -
                     static_cast<size_t>(__builtin_ctzll(MinSlotSize));
}

////////////////////////////////////////////////////////////////////////////////

class TBlock
{
    struct TSlot
    {
        TSlot* Next = nullptr;
    };

public:
    explicit TBlock(size_t slotSize)
        : Base(AlignedAlloc(BlockSize, 8))
        , SlotSize(slotSize)
        , SlotsPerBlock(BlockSize / slotSize)
    {
        std::memset(Base, 0, BlockSize);
    }

    ~TBlock()
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

    void* Allocate()
    {
        if (FreeList) {
            TSlot* result = FreeList;
            FreeList = FreeList->Next;
            std::memset(result, 0, SlotSize);
            --FreeCount;
            return result;
        }
        if (AllocatedSlots == SlotsPerBlock) {
            return nullptr;
        }
        return static_cast<char*>(Base) + AllocatedSlots++ * SlotSize;
    }

    void Free(void* slot)
    {
        TSlot* ptr = static_cast<TSlot*>(slot);
        ptr->Next = FreeList;
        FreeList = ptr;
        ++FreeCount;
    }

    [[nodiscard]] bool Empty() const
    {
        return FreeCount == AllocatedSlots;
    }

private:
    const TBase Base = nullptr;
    const size_t SlotSize = 0;
    const size_t SlotsPerBlock = 0;
    size_t AllocatedSlots = 0;
    TSlot* FreeList = nullptr;
    size_t FreeCount = 0;
};

class TBlockList
{
public:
    explicit TBlockList(size_t slotSize)
        : SlotSize(slotSize)
    {}

    void* Allocate(TBlockPtr* block)
    {
        ++AllocateCount;

        if (LastUsed) {
            if (auto* result = LastUsed->Allocate()) {
                return result;
            }
        }

        for (auto& block: Blocks) {
            if (auto* result = block.Allocate()) {
                LastUsed = &block;
                return result;
            }
        }

        Blocks.emplace_back(SlotSize);
        LastUsed = &Blocks.back();
        *block = LastUsed;
        return LastUsed->Allocate();
    }

    bool Deallocate(TBlockPtr block, void* ptr)
    {
        block->Free(ptr);
        ++DeallocateCount;

        if (block->Empty()) {
            FreeBlock(block);
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
        return Blocks.size() * BlockSize;
    }

    [[nodiscard]] size_t GetSlotSize() const
    {
        return SlotSize;
    }

    [[nodiscard]] TArenaAllocatorStats GetStats() const
    {
        return TArenaAllocatorStats{
            .SlotSize = SlotSize,
            .ReservedSize = GetReservedSize(),
            .UsedSize = GetAllocatedSize(),
            .Count = AllocateCount};
    }

private:
    void FreeBlock(TBlockPtr block)
    {
        if (block == LastUsed) {
            LastUsed = nullptr;
        }
        for (auto it = Blocks.begin(); it != Blocks.end(); ++it) {
            if (&*it == block) {
                Blocks.erase(it);
                return;
            }
        }
        Y_ABORT_UNLESS(false, "FreeBlock: unknown block");
    }

    const size_t SlotSize = 0;
    TList<TBlock> Blocks;
    TBlock* LastUsed = nullptr;
    size_t AllocateCount = 0;
    size_t DeallocateCount = 0;
};

///////////////////////////////////////////////////////////////////////////

class TArenaAllocator final
    : public IArenaAllocator
    , public TDisableCopyMove
{
public:
    void* Allocate(size_t size) override
    {
        with_lock (Mutex) {
            ++AllocatedBlockCount;
            TBlockPtr newBlock = nullptr;
            void* result = GetBlockList(size).Allocate(&newBlock);
            if (newBlock) {
                Bases.emplace(newBlock->GetBase(), newBlock);
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
            --AllocatedBlockCount;
            // Find the block whose [Base, Base + BlockSize) range contains
            // ptr: it is the block with the greatest base <= ptr.
            auto it = Bases.upper_bound(ptr);
            if (it == Bases.begin()) {
                // Unknown pointer.
                Y_ABORT_UNLESS(false, "DeAllocate: unknown pointer");
            }
            --it;
            TBlock* block = it->second;
            Y_ABORT_UNLESS(
                static_cast<char*>(ptr) <
                    static_cast<char*>(block->GetBase()) + BlockSize,
                "DeAllocate: unknown pointer");
            auto& blockList = GetBlockList(block->GetSlotSize());
            if (blockList.Deallocate(block, ptr)) {
                Bases.erase(it);
            }
        }
    }

    [[nodiscard]] size_t AllocatedBlocks() const override
    {
        with_lock (Mutex) {
            return AllocatedBlockCount;
        }
    }

    [[nodiscard]] size_t AllocatedSize() const override
    {
        with_lock (Mutex) {
            return Accumulate(
                BlocksBySlotSize,
                0,
                [](size_t result, const TBlockList& block)
                { return result + block.GetAllocatedSize(); });
        }
    }

    [[nodiscard]] TVector<TArenaAllocatorStats> GetStats() const override
    {
        with_lock (Mutex) {
            TVector<TArenaAllocatorStats> result;
            result.reserve(BlocksBySlotSize.size());
            for (const auto& blockList: BlocksBySlotSize) {
                result.push_back(blockList.GetStats());
            }
            return result;
        }
    }

private:
    TBlockList& GetBlockList(size_t slotSize)
    {
        return BlocksBySlotSize[SlotIndexForSize(slotSize)];
    }

    TMutex Mutex;
    TMap<TBase, TBlock*> Bases;
    std::array<TBlockList, SlotSizeCount> BlocksBySlotSize{
        TBlockList(256),
        TBlockList(512),
        TBlockList(1024),
        TBlockList(2048),
        TBlockList(4096)};
    size_t AllocatedBlockCount = 0;
};

}   // namespace

//////////////////////////////////////////////////////////////////////////////

IArenaAllocatorPtr CreateArenaAllocator()
{
    return std::make_shared<TArenaAllocator>();
}

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
