#pragma once

#include "block_range_field_impl.h"
#include "block_range_field_simple.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

class TBlockRangeFieldTestAccessor;
class TBlockRangeFieldBitMask;

// Stores zero or one block range inline and uses the selected backend for
// multiple ranges. MaxBlockCount must be a power of two, as guaranteed by the
// vchunk geometry.
class TBlockRangeField
{
public:
    using EBackend = IBlockRangeFieldImpl::EBackend;

    explicit TBlockRangeField(
        IArenaAllocatorPtr arenaAllocator,
        ui16 maxBlockCount = 32768,
        EBackend preferredBackend = EBackend::StdSet);
    ~TBlockRangeField();

    // Copying is forbidden: each field owns its own memory pool.
    TBlockRangeField(const TBlockRangeField&) = delete;
    TBlockRangeField& operator=(const TBlockRangeField&) = delete;

    // Movable: implementation is moved as a whole.
    TBlockRangeField(TBlockRangeField&& other) noexcept;
    TBlockRangeField& operator=(TBlockRangeField&& other) noexcept;

    // Returns true if the intervals have actually changed.
    bool Add(TBlockRange16 range);
    void Add(const TBlockRangeField& field);
    // Returns true if the intervals have actually changed.
    bool Remove(TBlockRange16 range);
    void Remove(const TBlockRangeField& field);
    // Returns true if the intervals have actually changed.
    bool Clear();

    [[nodiscard]] bool Overlaps(TBlockRange16 other) const;
    [[nodiscard]] bool Overlaps(const TBlockRangeField& other) const;

    [[nodiscard]] bool Empty() const;
    [[nodiscard]] size_t GetBlockCount() const;
    [[nodiscard]] std::optional<TBlockRange16> GetFirstRange() const;
    [[nodiscard]] TString Print() const;

    // Returns the backend currently used for storage: Simple while the field
    // holds at most one range, the preferred backend after the switch.
    [[nodiscard]] EBackend GetBackend() const;

    // Proto serialization.
    [[nodiscard]] bool IsBitmapBased() const;
    [[nodiscard]] TString Serialize() const;
    void DeserializeFromBitmap(const TString& source);
    void DeserializeFromRLE(const TString& source);

    // Memory usage.
    [[nodiscard]] TArenaPoolStats GetMemoryStats() const;

private:
    friend class TBlockRangeFieldTestAccessor;
    using EEnumerateContinuation =
        TNodeBasedBlockRangeField::EEnumerateContinuation;

    void CollapseImpl();
    void Upgrade();
    void UpgradeToPreferredBackend();
    void UpgradeToBitmapBackend();
    void DowngradeToSimpleBackendIfEmpty();

    [[nodiscard]] IBlockRangeFieldImpl* GetImpl();
    [[nodiscard]] const IBlockRangeFieldImpl* GetImpl() const;

    [[nodiscard]] TNodeBasedBlockRangeField* GetNodeBasedImpl();
    [[nodiscard]] const TNodeBasedBlockRangeField* GetNodeBasedImpl() const;

    ui16 MaxBlockCount;
    EBackend PreferredBackend;

    // Owns the underlying arena allocator used by backends that allocate
    // fixed-size chunks (currently TBlockRangeFieldSet).
    // Must be declared before Impl so that Impl is destroyed before
    // ArenaAllocator (Impl's backends hold raw pointers into it).
    std::shared_ptr<IArenaAllocator> ArenaAllocator;

    std::optional<TBlockRangeFieldSimple> SimpleImpl;
    std::unique_ptr<TNodeBasedBlockRangeField> NodeBasedImpl;
    std::unique_ptr<TBlockRangeFieldBitMask> BitMaskBasedImpl;
    size_t AllocationCount = 0;
};

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
