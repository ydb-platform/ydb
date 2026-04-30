#pragma once

#include "inflight_info.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/ddisk_state.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_mask.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TReadRangeHint
{
    TReadRangeHint(
        THostMask hostMask,
        bool fromDDisk,
        ui64 lsn,
        TBlockRange64 requestRelativeRange,
        TBlockRange64 vchunkRange,
        TRangeLock&& lock);

    TReadRangeHint(TReadRangeHint&& other) noexcept;
    TReadRangeHint& operator=(TReadRangeHint&& other) noexcept;

    THostMask HostMask;
    bool FromDDisk = false;
    ui64 Lsn = 0;

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

struct TPBufferSegment
{
    ui64 Lsn = 0;
    TBlockRange64 Range;

    [[nodiscard]] TString DebugPrint() const;
};

struct TFlushHint
{
    TVector<TPBufferSegment> Segments;

    [[nodiscard]] TString DebugPrint() const;
};

class TFlushHints
{
public:
    using THints = TMap<THostRoute, TFlushHint>;

    void AddHint(
        THostIndex source,
        THostIndex destination,
        ui64 lsn,
        TBlockRange64 range);

    [[nodiscard]] bool Empty() const;

    [[nodiscard]] const THints& GetAllHints() const;
    [[nodiscard]] THints TakeAllHints();

    [[nodiscard]] TString DebugPrint() const;

private:
    THints Hints;
};

struct TEraseHint
{
    TVector<TPBufferSegment> Segments;

    [[nodiscard]] TString DebugPrint() const;
};

class TEraseHints
{
public:
    using THints = TMap<THostIndex, TEraseHint>;

    void AddHint(THostIndex host, ui64 lsn, TBlockRange64 range);

    [[nodiscard]] bool Empty() const;

    [[nodiscard]] const THints& GetAllHints() const;
    [[nodiscard]] THints TakeAllHints();

    [[nodiscard]] TString DebugPrint() const;

private:
    THints Hints;
};

struct TPBufferCounters
{
    size_t CurrentRecordsCount = 0;
    size_t CurrentBytesCount = 0;
    size_t TotalRecordsCount = 0;
    size_t TotalBytesCount = 0;

    size_t CurrentLockedRecordsCount = 0;
    size_t CurrentLockedBytesCount = 0;
    size_t TotalLockedRecordsCount = 0;
    size_t TotalLockedBytesCount = 0;
};

class TBlocksDirtyMap
    : public ILockableRanges
    , public IReadyQueue
    , public TDisableCopyMove
{
public:
    TBlocksDirtyMap(ui32 blockSize, size_t hostCount);
    ~TBlocksDirtyMap() override;

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, THostIndex host);

    [[nodiscard]] TReadHint MakeReadHint(
        TBlockRange64 range,
        THostMask ddiskReadable,
        THostMask pbufferReadable,
        const TDDiskStateList& ddiskStates);
    [[nodiscard]] TFlushHints MakeFlushHint(
        size_t batchSize,
        THostMask ddiskFlushTargets,
        const TDDiskStateList& ddiskStates);
    [[nodiscard]] TEraseHints MakeEraseHint(
        size_t batchSize,
        THostMask pbufferEraseTargets);

    void WriteFinished(
        ui64 lsn,
        TBlockRange64 range,
        THostMask requested,
        THostMask confirmed);
    void FlushFinished(
        THostRoute route,
        const TVector<ui64>& flushOk,
        const TVector<ui64>& flushFailed);
    void EraseFinished(
        THostIndex host,
        const TVector<ui64>& eraseOk,
        const TVector<ui64>& eraseFailed);

    // Returns the number of in-flight write requests.
    [[nodiscard]] size_t GetInflightCount() const;
    [[nodiscard]] size_t GetFlushPendingCount() const;
    [[nodiscard]] size_t GetErasePendingCount() const;
    [[nodiscard]] ui64 GetMinFlushPendingLsn() const;
    [[nodiscard]] ui64 GetMinErasePendingLsn() const;
    [[nodiscard]] const TPBufferCounters& GetPBufferCounters(
        THostIndex host) const;

    // ILockableRanges implementation
    void LockPBuffer(ui64 lsn) override;
    void UnlockPBuffer(ui64 lsn) override;
    TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        THostMask mask) override;
    void UnLockDDiskRange(TLockRangeHandle handle) override;

    // IReadyQueue implementation
    void Register(ui64 lsn, EQueueType queueType) override;
    void UnRegister(ui64 lsn) override;
    void DataToPBufferAdded(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) override;
    void DataFromPBufferReleased(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) override;

    // Debug purposes
    [[nodiscard]] TString DebugPrintLockedDDiskRanges();

private:
    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, THostMask>;

    const ui32 BlockSize;

    // Inflight write requests.
    TInflightMap Inflight;

    // Ranges that need to be copied to other PBuffers in order to reach a
    // quorum.
    THashSet<ui64> ReadyToClone;

    // Ranges that are written PBuffers with quorum and ready to be flushed to
    // DDisk. Using TSet for O(1) min LSN access.
    TSet<ui64> ReadyToFlush;

    // Ranges that are fully transferred to DDisk and can be erased.
    // Using TSet for O(1) min LSN access.
    TSet<ui64> ReadyToErase;

    // In-flight reads and the locks they create.
    ILockableRanges::TLockRangeHandle InflightDDiskReadsGenerator = 0;
    TInflightDDiskReadsMap InflightDDiskReads;

    // PBuffers space usage counters. Indexed by host index.
    TVector<TPBufferCounters> PBufferCounters;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
