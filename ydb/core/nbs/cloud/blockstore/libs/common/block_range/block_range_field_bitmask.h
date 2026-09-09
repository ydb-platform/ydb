#pragma once

#include "block_range_field_impl.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/disable_copy.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

// Memory-optimized block range storage using a bit mask.
// Each bit corresponds to a single block index. Bit set = block allocated.
// The maximum block count must be a power of two. This keeps the mask byte
// aligned and matches the vchunk geometry invariant.
class TBlockRangeFieldBitMask final
    : public IBlockRangeFieldImpl
    , public TDisableCopy
{
public:
    TBlockRangeFieldBitMask(IArenaAllocatorPtr allocator, size_t maxBlockCount);

    static size_t CalcMemoryUsage(size_t blockCount);

    [[nodiscard]] EBackend GetBackend() const override;

    bool TryAdd(TBlockRange16 range, bool* changed) override;
    bool TryRemove(TBlockRange16 range, bool* changed) override;
    void Clear() override;

    [[nodiscard]] bool Overlaps(TBlockRange16 other) const override;

    [[nodiscard]] bool Empty() const override;
    [[nodiscard]] size_t GetBlockCount() const override;
    [[nodiscard]] std::optional<TBlockRange16> GetFirstRange() const override;

    [[nodiscard]] size_t GetAllocatedSize() const override;
    [[nodiscard]] size_t GetUsedSize() const override;

    [[nodiscard]] TString Save() const override;
    [[nodiscard]] TString Print() const override;

    void DeserializeFromBitmap(const TString& input);
    void Add(const TBlockRangeFieldBitMask& other);
    void Remove(const TBlockRangeFieldBitMask& other);

    [[nodiscard]] bool OverlapsWithBitMask(
        const TBlockRangeFieldBitMask& other) const;

private:
    const size_t MaxBlockCount;
    const std::shared_ptr<IArenaAllocator> Allocator;
    ui8* const Mask = nullptr;
    size_t BlockCount = 0;   // Cached count of set bits

    [[nodiscard]] size_t GetMaskSize() const;

    // Recomputes the cached number of set bits from the mask.
    void RecountBlockCount();

    // Check if any bit is set in [start, end), similar to TBitMapOps::HasAny.
    [[nodiscard]] bool HasAnyInRange(size_t start, size_t end) const;
};

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
