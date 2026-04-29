#pragma once

#include "inflight_info.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
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

class TDDiskState
{
public:
    enum class EState
    {
        Operational,   // The ddisk is fully functional and can be read from
                       // anywhere.
        Fresh,   // The ddisk is only partially filled, and you can only read
                 // from the blocks below the OperationalBlockCount.
    };

    void Init(ui64 totalBlockCount, ui64 operationalBlockCount);

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] bool CanReadFromDDisk(TBlockRange64 range) const;
    [[nodiscard]] bool NeedFlushToDDisk(TBlockRange64 range) const;

    void SetReadWatermark(ui64 blockCount);
    void SetFlushWatermark(ui64 blockCount);
    [[nodiscard]] ui64 GetOperationalBlockCount() const;

    [[nodiscard]] TString DebugPrint() const;

private:
    void UpdateState();

    EState State = EState::Operational;

    ui64 TotalBlockCount = 0;

    // If the block address below OperationalBlockCount, then it can be read
    // from DDisk.
    ui64 OperationalBlockCount = 0;

    // If the block address below FlushableBlockCount, then it should be written
    // (flushed) to DDisk.
    ui64 FlushableBlockCount = 0;
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
    TBlocksDirtyMap(ui32 blockSize, ui64 blockCount, size_t hostCount);
    ~TBlocksDirtyMap() override;

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, THostIndex host);

    [[nodiscard]] TReadHint MakeReadHint(
        TBlockRange64 range,
        THostMask ddiskReadable,
        THostMask pbufferReadable);
    [[nodiscard]] TFlushHints MakeFlushHint(
        size_t batchSize,
        THostMask ddiskFlushTargets);
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

    // Sets a mark on the ddisk to which offset it contains data and can be read
    // from it.
    void MarkFresh(THostIndex host, ui64 bytesOffset);
    // Returns the offset to which ddisk contains the data. nullopt means that
    // the disk is completely full of data. And you can read it from anywhere.
    [[nodiscard]] std::optional<ui64> GetFreshWatermark(THostIndex host) const;
    // Sets the mark up to which the disk can be read.
    void SetReadWatermark(THostIndex host, ui64 bytesOffset);
    // Sets the mark to which writes should be flushed to the ddisk.
    void SetFlushWatermark(THostIndex host, ui64 bytesOffset);

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
    [[nodiscard]] TString DebugPrintDDiskState() const;

private:
    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, THostMask>;

    [[nodiscard]] THostMask FilterDDiskHosts(
        THostMask mask,
        TBlockRange64 range) const;

    const ui32 BlockSize;
    const ui64 BlockCount;

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

    // DDisks freshness state. Indexed by host index.
    TVector<TDDiskState> DDiskStates;

    // PBuffers space usage counters. Indexed by host index.
    TVector<TPBufferCounters> PBufferCounters;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
