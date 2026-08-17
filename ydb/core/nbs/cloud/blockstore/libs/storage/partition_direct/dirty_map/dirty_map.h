#pragma once

#include "inflight_info.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_field.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

#include <span>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

class TVChunkConfig;

////////////////////////////////////////////////////////////////////////////////

struct TReadRangeHint
{
    TReadRangeHint(
        THostMask hostMask,
        ui64 lsn,
        TBlockRange64 requestRelativeRange,
        TBlockRange64 vchunkRange,
        TRangeLock&& lock);

    TReadRangeHint(TReadRangeHint&& other) noexcept;
    TReadRangeHint& operator=(TReadRangeHint&& other) noexcept;

    THostMask HostMask;
    // 0 -> read from DDisk (HostMask is the DDisk hosts to choose from).
    // >0 -> read from a PBuffer that holds the inflight write at this lsn
    // (HostMask is the PBuffer hosts that confirmed the write).
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

////////////////////////////////////////////////////////////////////////////////

struct TPBufferSegment
{
    ui64 Lsn = 0;
    TBlockRange64 Range;

    static TVector<ui64> MakeLsnVector(
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
        ui64 lsn,
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
    ui32 Generation = 0;
    ui64 Lsn = 0;

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

    void AddHint(THostIndex host, ui64 lsn);

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

class TDDiskState
{
public:
    enum class EState
    {
        Disabled,   // There are no DDisks with data on the host and DDisk
                    // cannot be used.

        Operational,   // The DDisk is fully functional and can be read from
                       // anywhere. BehindField and AheadField are empty.

        Fresh,   // The ddisk is only partially filled, and you can only read
                 // from the blocks below the OperationalBlockCount.
                 // The AheadField shows which ranges flushed over watermark and
                 // can be read from. The BehindField shows which ranges
                 // outdated and can't be read.
    };

    enum class EFlushCompletion
    {
        Completed,   // Data flushed to DDisk
        Missed,      // Data not flushed to DDisk
    };

    // Enables the use of DDisk. If the operational blocks count less then total
    // block count, then the DDisk is only partially filled (fresh).
    void Init(ui64 totalBlockCount, ui64 operationalBlockCount);

    // Completely disables DDisk usage.
    void SwitchOffline();

    [[nodiscard]] bool IsLagging() const;
    // DDisk has stopped receiving writes. Now the written ranges are
    // interpreted as "bad" and added to the BehindField.
    void StartLagging();
    // DDisk now receive all writes. The written ranges are interpreted as
    // "good" and removed from the BehindField.
    void StopLagging();
    // Is it necessary to receive information about all written ranges. If true
    // is returned, it means that all ranged that have been flushed must be
    // passed to the OnRangeFlushed() method.
    [[nodiscard]] bool IsTrackingEnabled() const;
    // Updates the BehindField and the Ahead Field if required.
    void OnRangeFlushed(TBlockRange64 range, EFlushCompletion flush);

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] bool CanReadFromDDisk(TBlockRange64 range) const;

    [[nodiscard]] std::optional<TBlockRange64> GetFreshRange() const;
    void RangeSynced(TBlockRange64 range);

    void UpdateWatermarkDebugOnly(ui64 blockCount);
    [[nodiscard]] TString DebugPrint() const;
    [[nodiscard]] TString DebugPrintAhead() const;
    [[nodiscard]] TString DebugPrintBehind() const;

private:
    [[nodiscard]] bool IsFresh() const;
    void UpdateState(bool force);
    void AddAhead(TBlockRange64 range);

    EState State = EState::Disabled;

    ui64 TotalBlockCount = 0;

    // If the block address below OperationalBlockCount, then it can be read
    // from DDisk (except BehindField).
    ui64 OperationalBlockCount = 0;

    // Lagging means that flush operations are not performed and DDisk has
    // outdated data in the ranges listed in the BehindField.
    bool Lagging = false;
    TBlockRangeField BehindField;
    // When a user writes to a range above OperationalBlockCount, this range has
    // up-to-date data and does not require sync.
    TBlockRangeField AheadField;
};

struct TPBufferCounters
{
    // The current count of records stored in PBuffer
    size_t CurrentRecordsCount = 0;
    // The current count of bytes stored in PBuffer
    size_t CurrentBytesCount = 0;
    // Total count of records written to PBuffer and possibly already deleted
    size_t TotalRecordsCount = 0;
    // Total count of bytes written to PBuffer and possibly already deleted
    size_t TotalBytesCount = 0;

    // The current number of records prohibited for deletion from PBuffer
    size_t CurrentLockedRecordsCount = 0;
    // The current number of bytes prohibited for deletion from PBuffer
    size_t CurrentLockedBytesCount = 0;

    // The total number of records ever prohibited for deletion from PBuffer
    size_t TotalLockedRecordsCount = 0;
    // The total number of bytes ever prohibited for deletion from PBuffer
    size_t TotalLockedBytesCount = 0;

    [[nodiscard]] TString DebugPrint() const;
};

class TBlocksDirtyMap
    : public ILockableRanges
    , public IReadyQueue
    , public TDisableCopyMove
    , public std::enable_shared_from_this<TBlocksDirtyMap>
{
public:
    enum class EEraseType
    {
        Standard,
        Belated
    };

    TBlocksDirtyMap(
        const TVChunkConfig& vChunkConfig,
        ui32 blockSize,
        ui64 blockCount);
    ~TBlocksDirtyMap() override;

    // Note. Fresh watermarks are not applying for exists DDisks.
    void UpdateConfig(const TVChunkConfig& vChunkConfig);

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, THostIndex host);

    // MakeReadHint can work with multiple locations and returns multiple
    // RangeHints
    [[nodiscard]] TReadHint MakeReadHint(TBlockRange64 range);
    [[nodiscard]] TFlushHints MakeFlushHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseBelatedHint();

    // Registers a write as pending (lsn generated, data not in any PBuffer
    // yet) so that the cleanup bound covers it from the moment of generation.
    void RegisterInflightWrite(ui64 lsn, TBlockRange64 range);

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

    void UpdateBelatedEraseQueue(THostMask completedWrites, ui64 lsn);

    // Sets the mark up to which the disk can be read.
    void UpdateWatermarkDebugOnly(THostIndex host, ui64 bytesOffset);
    // Returns the first "fresh" range to be synced with data from another
    // replicas. Nullopt means that the disk is completely full of data. And you
    // can read it from anywhere.
    [[nodiscard]] std::optional<TBlockRange64> GetFreshRange(
        THostIndex host) const;
    // See TSyncHint for details.
    // The BeginRangeSync and EndRangeSync calls must be paired.
    TSyncHint BeginRangeSync(THostIndex host, TBlockRange64 range);
    // Should be called when the range synchronization is complete or failed.
    void EndRangeSync(ui64 syncId, bool success);
    void ClearRangeSyncs(THostIndex host);

    [[nodiscard]] size_t GetHostCount() const;
    // Returns the number of in-flight write requests.
    [[nodiscard]] size_t GetInflightCount() const;
    [[nodiscard]] size_t GetFlushPendingCount() const;
    [[nodiscard]] size_t GetErasePendingCount() const;
    [[nodiscard]] size_t GetEraseBelatedCount() const;
    [[nodiscard]] ui64 GetMinFlushPendingLsn() const;
    [[nodiscard]] ui64 GetMinErasePendingLsn() const;
    [[nodiscard]] std::optional<ui64> GetSafeBarrierForErase() const;
    [[nodiscard]] const TPBufferCounters& GetPBufferCounters(
        THostIndex host) const;
    [[nodiscard]] ui64 GetPBufferUsedSize(THostIndex host) const;

    // ILockableRanges implementation
    void LockPBuffer(ui64 lsn) override;
    void UnlockPBuffer(ui64 lsn) override;
    TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        THostMask mask) override;
    void UnLockDDiskRange(TLockRangeHandle handle) override;

    // IReadyQueue implementation
    void Register(ui64 lsn, EQueueType queueType) override;
    void UnRegister(ui64 lsn, EQueueType queueType) override;
    void FlushCompleted(ui64 lsn, THostMask ddisks) override;
    void DataToPBufferAdded(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) override;
    void DataFromPBufferReleased(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) override;

    [[nodiscard]] bool NeedFlush() const;
    [[nodiscard]] bool NeedErase() const;

    // Debug purposes
    [[nodiscard]] TString DebugPrintPBuffers();
    [[nodiscard]] TString DebugPrintPBuffersUsage() const;
    [[nodiscard]] TString DebugPrintLockedDDiskRanges();
    [[nodiscard]] TString DebugPrintDDiskState() const;
    [[nodiscard]] TString DebugPrintReadyToClone() const;
    [[nodiscard]] TString DebugPrintReadyToFlush() const;
    [[nodiscard]] TString DebugPrintReadyToErase() const;
    [[nodiscard]] TString DebugPrintAhead() const;
    [[nodiscard]] TString DebugPrintBehind() const;
    [[nodiscard]] TString DebugPrintInflightSync();

private:
    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, THostMask>;

    struct TInfoEraseBelated
    {
        ui64 Lsn{};
        THostMask Hosts;

        bool operator<(const TInfoEraseBelated& other) const;
    };

    struct TInflightDDiskSync
    {
        THostIndex DestinationHost = InvalidHostIndex;
        NThreading::TPromise<void> SyncStartTrigger =
            NThreading::NewPromise<void>();
    };

    using TInflightDDiskSyncMap = TBlockRangeMap<ui64, TInflightDDiskSync>;

    void ResizeHosts(size_t newHostCount);

    [[nodiscard]] THostMask FilterLocations(
        THostMask mask,
        TBlockRange64 range) const;

    // Create single readRangeHint for specified parameters
    [[nodiscard]] TReadRangeHint MakeReadRangeHint(
        THostMask mask,
        ui64 lsn,
        TBlockRange64 range,
        ui64 offsetBlocks);

    void AddToAheadAndBehindOnFlushCompleted(ui64 lsn, THostMask ddisks);

    [[nodiscard]] bool HasInflightFlush(THostIndex host, TBlockRange64 range);
    void InflightFlushFinished(TBlockRange64 range);

    const ui32 BlockSize;
    const ui64 BlockCount;

    THostMask DesiredDDisks;
    THostMask DisabledHosts;

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

    TSet<TInfoEraseBelated> ReadyToEraseBelated;

    // In-flight reads and the locks they create.
    ILockableRanges::TLockRangeHandle InflightDDiskReadsGenerator = 0;
    TInflightDDiskReadsMap InflightDDiskReads;

    // DDisk sync operations that are running or waiting for overlapped flushes
    // to complete in order to start execution.
    ui64 InflightDDiskSyncIdGenerator = 0;
    TInflightDDiskSyncMap InflightDDiskSyncMap;

    // DDisks freshness state.
    TVector<TDDiskState> DDiskStates;

    // PBuffers space usage counters.
    TVector<TPBufferCounters> PBufferCounters;
};

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> MakeLsnVector(std::span<const TPBufferSegment> segments);
TVector<ui64> MakeLsnVector(std::span<const TEraseSegment> segments);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
