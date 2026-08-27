#pragma once

#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/pbuffer_key.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <span>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TReadRangeHint
{
    TReadRangeHint(
        THostMask hostMask,
        TPBufferKey pBufferKey,
        TBlockRange64 requestRelativeRange,
        TBlockRange64 vchunkRange,
        TRangeLock&& lock);

    TReadRangeHint(TReadRangeHint&& other) noexcept;
    TReadRangeHint& operator=(TReadRangeHint&& other) noexcept;

    THostMask HostMask;
    // PBufferKey.Lsn == 0 -> read from DDisk (HostMask is the DDisk hosts to
    // choose from).
    // PBufferKey.Lsn > 0 -> read from a PBuffer that holds this inflight record
    // (HostMask is the PBuffer hosts that confirmed the write).
    TPBufferKey PBufferKey;

    // Range relative to the request.
    TBlockRange64 RequestRelativeRange;

    // Range relative to the VChunk.
    TBlockRange64 VChunkRange;

    // Should call Lock.Arm() before reading.
    TRangeLock Lock;

    [[nodiscard]] TString DebugPrint() const;
};

struct TReadHint
{
    // If the RangeHints is empty, then you need to wait for the WaitReady
    // feature to be IsReady and repeat the request.
    TVector<TReadRangeHint> RangeHints;
    NThreading::TFuture<void> WaitReady;

    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TPBufferSegment
{
    TPBufferKey PBufferKey;
    TBlockRange64 Range;

    static TVector<TPBufferKey> MakePBufferKeys(
        std::span<const TPBufferSegment> segments);

    [[nodiscard]] TString DebugPrint(bool brief) const;
};

struct TFlushHint
{
    TVector<TPBufferSegment> Segments;

    [[nodiscard]] TString DebugPrint(bool brief) const;
};

class TFlushHints
{
public:
    using THints = TMap<THostRoute, TFlushHint>;

    void AddHint(
        THostIndex source,
        THostIndex destination,
        TPBufferKey pBufferKey,
        TBlockRange64 range);

    [[nodiscard]] bool Empty() const;

    [[nodiscard]] const THints& GetAllHints() const;
    [[nodiscard]] THints TakeAllHints();

    [[nodiscard]] TString DebugPrint() const;

private:
    THints Hints;
};

////////////////////////////////////////////////////////////////////////////////

struct TEraseSegment
{
    TPBufferKey PBufferKey;

    [[nodiscard]] TString DebugPrint(bool brief) const;
};

using TEraseSegments = TVector<TEraseSegment>;

struct TEraseHint
{
    TEraseSegments Segments;

    [[nodiscard]] TString DebugPrint(bool brief) const;
};

class TEraseHints
{
public:
    using THints = TMap<THostIndex, TEraseHint>;

    void AddHint(THostIndex host, TPBufferKey pBufferKey);

    [[nodiscard]] bool Empty() const;

    [[nodiscard]] const THints& GetAllHints() const;
    [[nodiscard]] THints TakeAllHints();

    [[nodiscard]] TString DebugPrint() const;

private:
    THints Hints;
};

////////////////////////////////////////////////////////////////////////////////

struct TSyncHint
{
    ui64 SyncId = 0;
    THostIndex Host = InvalidHostIndex;
    TBlockRange64 Range;

    // ReadyToStart will be triggered at the moment when all
    // overlapping flush operations with this range are completed.
    // After that, the range synchronization can begin.
    NThreading::TFuture<void> ReadyToStart;
};

////////////////////////////////////////////////////////////////////////////////

TVector<TPBufferKey> MakePBufferKeys(std::span<const TPBufferSegment> segments);
TVector<TPBufferKey> MakePBufferKeys(std::span<const TEraseSegment> segments);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
