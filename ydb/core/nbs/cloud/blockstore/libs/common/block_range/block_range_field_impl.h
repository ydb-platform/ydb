#pragma once

#include "block_range.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_allocator.h>

#include <util/generic/string.h>

#include <functional>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////

// Defines operations supported by block-range field implementations.
class IBlockRangeFieldImpl
{
public:
    using TRange = TBlockRange16;

    enum class EBackend
    {
        Simple,
        StdSet,
        Set,
        FlatSet,
        Bitmask,
    };

    virtual ~IBlockRangeFieldImpl() = default;

    [[nodiscard]] virtual EBackend GetBackend() const = 0;

    virtual bool TryAdd(TBlockRange16 range, bool* changed) = 0;
    virtual bool TryRemove(TBlockRange16 range, bool* changed) = 0;
    virtual void Clear() = 0;

    // Returns true when the field overlaps the range.
    [[nodiscard]] virtual bool Overlaps(TBlockRange16 other) const = 0;
    [[nodiscard]] virtual bool Empty() const = 0;
    [[nodiscard]] virtual size_t GetBlockCount() const = 0;
    [[nodiscard]] virtual std::optional<TBlockRange16>
    GetFirstRange() const = 0;

    // Memory usage.
    [[nodiscard]] virtual TArenaPoolStats GetMemoryStats() const = 0;

    // Proto serialization.
    [[nodiscard]] virtual TString Save() const = 0;

    // Debug.
    [[nodiscard]] virtual TString Print() const = 0;
};

//////////////////////////////////////////////////////////////////////////////

class TNodeBasedBlockRangeField: public IBlockRangeFieldImpl
{
public:
    enum class EEnumerateContinuation
    {
        Continue,
        Stop,
    };

    using TEnumerateFunc = std::function<EEnumerateContinuation(
        IBlockRangeFieldImpl::TRange item)>;

    // Validates and enumerates an RLE stream. Returns false for malformed data.
    static bool DeserializeFromRLE(
        const TString& input,
        ui16 maxBlockCount,
        TEnumerateFunc func);

    // Enumerates ranges until the callback requests a stop.
    virtual void Enumerate(TEnumerateFunc func) const = 0;
    [[nodiscard]] virtual size_t GetSegmentCount() const = 0;

    [[nodiscard]] std::optional<TBlockRange16> GetFirstRange() const final;
    [[nodiscard]] TString Save() const final;
    [[nodiscard]] TString Print() const final;
};

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
