#pragma once

#include "ddisk_state.h"
#include "hints.h"
#include "inflight_info.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/count_size.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_mask.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/public.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

class TVChunkConfig;

////////////////////////////////////////////////////////////////////////////////

struct TPBufferCounters
{
    // The current PBuffer usage.
    TCountAndSize Current;

    // Overall count and size written PBuffers and possibly already deleted.
    TCountAndSize Total;

    // The current prohibited for deletion PBuffers.
    TCountAndSize CurrentLocked;

    // The total number of records ever prohibited for deletion from PBuffer
    TCountAndSize TotalLocked;

    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

class TBlocksDirtyMap
    : public ILockableRanges
    , public IReadyQueue
    , public IBehindAheadMonitor
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

    void Load(const TDirtyMapStateProto& proto);

    // Note. Fresh watermarks are not applying for exists DDisks.
    void UpdateConfig(const TVChunkConfig& vChunkConfig);

    void RestorePBuffer(
        TPBufferKey pBufferKey,
        TBlockRange64 range,
        THostIndex host);

    // MakeReadHint can work with multiple locations and returns multiple
    // RangeHints
    [[nodiscard]] TReadHint MakeReadHint(TBlockRange64 range);
    [[nodiscard]] TFlushHints MakeFlushHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseBelatedHint();

    // Registers a write as pending (lsn generated, data not in any PBuffer
    // yet) so that the cleanup bound covers it from the moment of generation.
    void RegisterInflightWrite(TPBufferKey pBufferKey, TBlockRange64 range);

    void WriteFinished(
        TPBufferKey pBufferKey,
        TBlockRange64 range,
        THostMask requested,
        THostMask confirmed);
    void FlushFinished(
        THostRoute route,
        const TVector<TPBufferKey>& flushOk,
        const TVector<TPBufferKey>& flushFailed);
    void EraseFinished(
        THostIndex host,
        const TVector<TPBufferKey>& eraseOk,
        const TVector<TPBufferKey>& eraseFailed);

    void UpdateBelatedEraseQueue(
        THostMask completedWrites,
        TPBufferKey pBufferKey);

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
    [[nodiscard]] std::optional<TPBufferKey> GetSafeBarrierForErase() const;
    [[nodiscard]] const TPBufferCounters& GetPBufferCounters(
        THostIndex host) const;
    [[nodiscard]] TCountAndSize GetPBuffersUsage(THostIndex host) const;
    [[nodiscard]] TCountAndSize GetAheadBlocks(THostIndex host) const;
    [[nodiscard]] TCountAndSize GetBehindBlocks(THostIndex host) const;

    // ILockableRanges implementation
    void LockPBuffer(TPBufferKey pBufferKey) override;
    void UnlockPBuffer(TPBufferKey pBufferKey) override;
    TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        THostMask mask) override;
    void UnLockDDiskRange(TLockRangeHandle handle) override;

    // IReadyQueue implementation
    void Register(TPBufferKey pBufferKey, EQueueType queueType) override;
    void UnRegister(TPBufferKey pBufferKey, EQueueType queueType) override;
    void FlushCompleted(TPBufferKey pBufferKey, THostMask ddisks) override;
    void DataToPBufferAdded(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) override;
    void DataFromPBufferReleased(
        THostIndex host,
        EPBufferCounter counter,
        size_t byteCount) override;

    // IBehindAheadMonitor implementation
    void OnBehindAheadChanged() override;

    [[nodiscard]] bool NeedFlush() const;
    [[nodiscard]] bool NeedErase() const;

    // Persist
    [[nodiscard]] bool NeedPersist() const;
    [[nodiscard]] TDirtyMapStateProto GetStateForPersist() const;
    void StatePersisted(ui32 persistGeneration);
    [[nodiscard]] ui32 GetCurrentGeneration() const;

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
    [[nodiscard]] TString DebugPrintAheadBehindBrief() const;
    [[nodiscard]] TString DebugPrintInflightSync();

private:
    using TInflightMap = TBlockRangeMap<TPBufferKey, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, THostMask>;

    struct TInfoEraseBelated
    {
        TPBufferKey PBufferKey;
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
        TPBufferKey pBufferKey,
        TBlockRange64 range,
        ui64 offsetBlocks);

    void AddToAheadAndBehindOnFlushCompleted(
        TPBufferKey pBufferKey,
        THostMask ddisks);

    [[nodiscard]] bool HasInflightFlush(THostIndex host, TBlockRange64 range);
    void InflightFlushFinished(TBlockRange64 range);

    [[nodiscard]] bool CheckEraseAbility(
        TBlockRange64 range,
        TInflightInfo& inflightInfo);

    const ui32 BlockSize;
    const ui64 BlockCount;

    THostMask DesiredDDisks;
    THostMask DisabledHosts;

    // Inflight write requests.
    TInflightMap Inflight;

    // Ranges that need to be copied to other PBuffers in order to reach a
    // quorum.
    THashSet<TPBufferKey> ReadyToClone;

    // Ranges that are written PBuffers with quorum and ready to be flushed to
    // DDisk. Using TSet for O(1) min LSN access.
    TSet<TPBufferKey> ReadyToFlush;

    // Ranges that are fully transferred to DDisk and can be erased.
    // Using TSet for O(1) min LSN access.
    TSet<TPBufferKey> ReadyToErase;

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
    // Changed when DDiskState changed his behind or ahead map.
    ui32 BehindAheadGeneration = 0;
    // Last persisted DDisks states generation.
    ui32 PersistedGeneration = 0;

    // PBuffers space usage counters.
    TVector<TPBufferCounters> PBufferCounters;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
