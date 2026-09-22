#pragma once

#include "inflight_info.h"
#include "mon_model.h"
#include "range_locker.h"

<<<<<<< HEAD
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
=======
#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/memory/arena_std_containers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/count_size.h>
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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

<<<<<<< HEAD
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

class TDDiskState
{
public:
    enum class EState
    {
        Disabled,   // There are no DDisks with data on the host and DDisk
                    // cannot be used.

        Operational,   // The DDisk is fully functional and can be read from
                       // anywhere.
        Fresh,   // The ddisk is only partially filled, and you can only read
                 // from the blocks below the OperationalBlockCount.
    };

    // Enables the use of DDisk. If the operational blocks count less then total
    // block count, then the DDisk is only partially filled (fresh).
    void Init(ui64 totalBlockCount, ui64 operationalBlockCount);

    // Completely disables DDisk usage.
    void SwitchOffline();

    [[nodiscard]] EState GetState() const;
    [[nodiscard]] bool CanReadFromDDisk(TBlockRange64 range) const;
    [[nodiscard]] bool NeedFlushToDDisk(TBlockRange64 range) const;

    void SetReadWatermark(ui64 blockCount);
    void SetFlushWatermark(ui64 blockCount);
    [[nodiscard]] ui64 GetOperationalBlockCount() const;

    [[nodiscard]] TString DebugPrint() const;

private:
    void UpdateState();

    EState State = EState::Disabled;

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

=======
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
class TBlocksDirtyMap
    : public ILockableRanges
    , public IReadyQueue
<<<<<<< HEAD
=======
    , public IBehindMonitor
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    , public TDisableCopyMove
{
public:
    enum class EEraseType
    {
        Standard,
        Belated
    };
    TBlocksDirtyMap(
        TArenaAllocatorPoolPtr arenaAllocatorPool,
        const TVChunkConfig& vChunkConfig,
        bool isTouched,
        const TDirtyMapStateProto& state,
        ui32 blockSize,
        ui16 blockCount);
    ~TBlocksDirtyMap() override;

    // Existing DDisks retain their Behind state; newly added DDisks start
    // with full Behind only when the VChunk is touched.
    void UpdateConfig(const TVChunkConfig& vChunkConfig, bool isTouched);

<<<<<<< HEAD
    void ResizeHosts(size_t newHostCount);

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, THostIndex host);
=======
    void RestorePBuffer(
        TPBufferKey pBufferKey,
        TBlockRange16 range,
        THostIndex host);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    // MakeReadHint can work with multiple locations and returns multiple
    // RangeHints
    [[nodiscard]] TReadHint MakeReadHint(TBlockRange16 range);
    [[nodiscard]] TFlushHints MakeFlushHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseBelatedHint();

    // Registers a write as pending (lsn generated, data not in any PBuffer
    // yet) so that the cleanup bound covers it from the moment of generation.
<<<<<<< HEAD
    void RegisterInflightWrite(ui64 lsn, TBlockRange64 range);

    void WriteFinished(
        ui64 lsn,
        TBlockRange64 range,
=======
    void RegisterInflightWrite(TPBufferKey pBufferKey, TBlockRange16 range);

    void WriteFinished(
        TPBufferKey pBufferKey,
        TBlockRange16 range,
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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

<<<<<<< HEAD
    // Sets a mark on the ddisk to which offset it contains data and can be read
    // from it.
    void MarkFresh(THostIndex host, ui64 bytesOffset);
    // Returns the offset to which ddisk contains the data. nullopt means that
    // the disk is completely full of data. And you can read it from anywhere.
    [[nodiscard]] std::optional<ui64> GetFreshWatermark(THostIndex host) const;
    // Sets the mark up to which the disk can be read.
<<<<<<< HEAD
    void SetReadWatermark(THostIndex host, ui64 bytesOffset);
    // Sets the mark to which writes should be flushed to the ddisk.
    void SetFlushWatermark(THostIndex host, ui64 bytesOffset);
=======
    void UpdateWatermarkDebugOnly(THostIndex host, ui64 bytesOffset);
=======
    // Sets the readable prefix of one DDisk for tests.
    void SetReadablePrefixDebugOnly(THostIndex host, ui64 bytesOffset);
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    // Returns the first "fresh" range to be synced with data from another
    // replicas. Nullopt means that the disk is completely full of data. And you
    // can read it from anywhere.
    [[nodiscard]] std::optional<TBlockRange16> GetFreshRange(
        THostIndex host) const;
    [[nodiscard]] THostMask GetOutdatedDDisks() const;
    // See TSyncHint for details.
    // The BeginRangeSync and EndRangeSync calls must be paired.
    TSyncHint BeginRangeSync(THostIndex host, TBlockRange16 range);
    // Should be called when the range synchronization is complete or failed.
    void EndRangeSync(ui64 syncId, bool success);
    void ClearRangeSyncs(THostIndex host);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    // Returns the number of in-flight write requests.
    [[nodiscard]] size_t GetInflightCount() const;
    [[nodiscard]] size_t GetFlushPendingCount() const;
    [[nodiscard]] size_t GetErasePendingCount() const;
    [[nodiscard]] size_t GetEraseBelatedCount() const;
    [[nodiscard]] ui64 GetMinFlushPendingLsn() const;
    [[nodiscard]] ui64 GetMinErasePendingLsn() const;
<<<<<<< HEAD
    [[nodiscard]] std::optional<ui64> GetSafeBarrierForErase() const;
    [[nodiscard]] const TPBufferCounters& GetPBufferCounters(
        THostIndex host) const;
<<<<<<< HEAD
=======
    [[nodiscard]] TCountAndSize GetPBuffersUsage(THostIndex host) const;
    [[nodiscard]] ui64 GetFreshTotalBytes(THostIndex host) const;
    [[nodiscard]] ui64 GetRottenTotalBytes(THostIndex host) const;
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
    [[nodiscard]] std::optional<TPBufferKey> GetSafeBarrierForErase() const;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    // ILockableRanges implementation
    void LockPBuffer(ui64 lsn) override;
    void UnlockPBuffer(ui64 lsn) override;
    TLockRangeHandle LockDDiskRange(
        TBlockRange16 range,
        THostMask mask) override;
    void UnLockDDiskRange(TLockRangeHandle handle) override;

    // IReadyQueue implementation
<<<<<<< HEAD
<<<<<<< HEAD
    void Register(ui64 lsn, EQueueType queueType) override;
    void UnRegister(ui64 lsn) override;
=======
    void Register(TPBufferKey pBufferKey, EQueueType queueType) override;
    void UnRegister(TPBufferKey pBufferKey, EQueueType queueType) override;
=======
    TPBufferKey GetPBufferKey(const TInflightInfo& inflight) const override;
    void Register(const TInflightInfo& inflight, EQueueType queueType) override;
    void UnRegister(
        const TInflightInfo& inflight,
        EQueueType queueType) override;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    void InflightFlushFinished(
        const TInflightInfo& inflight,
        THostIndex host) override;
<<<<<<< HEAD
    void FlushCompleted(TPBufferKey pBufferKey, THostMask ddisks) override;
>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
=======
    void FlushCompleted(
        const TInflightInfo& inflight,
        THostMask ddisks) override;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    void DataToPBufferAdded(
        const TInflightInfo& inflight,
        THostIndex host,
        EPBufferCounter counter) override;
    void DataFromPBufferReleased(
        const TInflightInfo& inflight,
        THostIndex host,
        EPBufferCounter counter) override;

<<<<<<< HEAD
=======
    // IBehindMonitor implementation
    void OnBehindChanged() override;

>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    [[nodiscard]] bool NeedFlush() const;
    [[nodiscard]] bool NeedErase() const;

<<<<<<< HEAD
=======
    // Persist
    [[nodiscard]] bool NeedPersist() const;
    // Returns an empty proto when no DDisk needs repair.
    [[nodiscard]] TDirtyMapStateProto GetStateForPersist() const;
    // Predicts the future state after applying vChunkConfig without changing
    // the current in-memory state.
    [[nodiscard]] TDirtyMapStateProto MakeFutureState(
        const TVChunkConfig& vChunkConfig,
        bool isTouched) const;
    void StatePersisted(ui32 persistGeneration);
    [[nodiscard]] ui32 GetCurrentGeneration() const;

    // Memory management
    void Trim();

    // Stats
    [[nodiscard]] TDirtyMapStats GetStats() const;
    [[nodiscard]] TDirtyMapHostStats GetHostStats(THostIndex host) const;
    [[nodiscard]] const TPBufferCounters& GetPBufferCounters(
        THostIndex host) const;
    [[nodiscard]] TCountAndSize GetPBuffersUsage(THostIndex host) const;

>>>>>>> 78a20188184 (Field map set optimization (#52565))
    // Debug purposes
    [[nodiscard]] TString DebugPrintPBuffers() const;
    [[nodiscard]] TString DebugPrintPBuffersUsage() const;
    [[nodiscard]] TString DebugPrintLockedDDiskRanges();
    [[nodiscard]] TString DebugPrintDDiskState() const;
    [[nodiscard]] TString DebugPrintReadyToClone() const;
    [[nodiscard]] TString DebugPrintReadyToFlush() const;
    [[nodiscard]] TString DebugPrintReadyToErase() const;
<<<<<<< HEAD
<<<<<<< HEAD
=======
    [[nodiscard]] TString DebugPrintAhead() const;
=======
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    [[nodiscard]] TString DebugPrintBehind() const;
    [[nodiscard]] TString DebugPrintBehindBrief() const;
    [[nodiscard]] TString DebugPrintInflightSync() const;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

private:
<<<<<<< HEAD
    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, THostMask>;

=======
    using TPBufferKeySet = TArenaSet<TPBufferKey>;
    using TInflightMap =
        TBlockRangeMap<TPBufferKey, TInflightInfo, TBlockRange16, true>;
    using TInflightDDiskReadsMap = TBlockRangeMap<
        ILockableRanges::TLockRangeHandle,
        THostMask,
        TBlockRange16>;

    struct TInfoEraseBelated
    {
        TPBufferKey PBufferKey;
        THostMask Hosts;

        bool operator<(const TInfoEraseBelated& other) const;
    };

    using TInfoEraseBelatedSet = TArenaSet<TInfoEraseBelated>;

    struct TInflightDDiskSync
    {
        THostIndex DestinationHost = InvalidHostIndex;
        NThreading::TPromise<void> SyncStartTrigger =
            NThreading::NewPromise<void>();
    };

    using TInflightDDiskSyncMap =
        TBlockRangeMap<ui64, TInflightDDiskSync, TBlockRange16>;

    void ResizeHosts(size_t newHostCount);

>>>>>>> 78a20188184 (Field map set optimization (#52565))
    [[nodiscard]] THostMask FilterLocations(
        THostMask mask,
        TBlockRange16 range) const;

    // Create single readRangeHint for specified parameters
    [[nodiscard]] TReadRangeHint MakeReadRangeHint(
        THostMask mask,
<<<<<<< HEAD
        ui64 lsn,
        TBlockRange64 range,
        ui64 offsetBlocks);
=======
        TPBufferKey pBufferKey,
        TBlockRange16 range,
        ui16 offsetBlocks);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

<<<<<<< HEAD
<<<<<<< HEAD
    const ui32 BlockSize;
    const ui64 BlockCount;
=======
    void AddToAheadAndBehindOnFlushCompleted(
=======
    void UpdateBehindOnFlushCompleted(
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
        const TInflightInfo& inflight,
        THostMask ddisks);

    [[nodiscard]] bool HasInflightFlush(THostIndex host, TBlockRange16 range);

    [[nodiscard]] bool HasOlderUnflushedOverlap(
        TPBufferKey pBufferKey,
        TBlockRange16 range);

    [[nodiscard]] bool HasOlderOverlap(
        TPBufferKey pBufferKey,
        TBlockRange16 range);

    [[nodiscard]] bool CheckEraseAbility(
        TBlockRange16 range,
        TInflightInfo& inflightInfo);

    void RemovePBuffer(TPBufferKey pBufferKey);
>>>>>>> 78a20188184 (Field map set optimization (#52565))

    const TArenaAllocatorPoolPtr ArenaAllocatorPool;
    const IArenaAllocatorPtr ArenaAllocator;
    const ui32 BlockSize;
    const ui16 BlockCount;
    TDirtyMapStats Stats;

    THostMask DesiredDDisks;
    THostMask DisabledHosts;

    // Inflight write requests.
    TInflightMap Inflight;

    // Ranges that need to be copied to other PBuffers in order to reach a
    // quorum.
    THashSet<ui64> ReadyToClone;

    // Ranges that are written PBuffers with quorum and ready to be flushed to
    // DDisk. Using TSet for O(1) min LSN access.
<<<<<<< HEAD
<<<<<<< HEAD
    TSet<ui64> ReadyToFlush;

    // Ranges that are fully transferred to DDisk and can be erased.
    // Using TSet for O(1) min LSN access.
    TSet<ui64> ReadyToErase;

    struct TInfoEraseBelated
    {
        ui64 Lsn{};
        THostMask Hosts;

        bool operator<(const TInfoEraseBelated& other) const;
    };
=======
    TPBufferKeySet ReadyToFlush{&ArenaAllocatorPool};

    // Ranges that are fully transferred to DDisk and can be erased.
    // Using TSet for O(1) min LSN access.
    TPBufferKeySet ReadyToErase{&ArenaAllocatorPool};
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
    TPBufferKeySet ReadyToFlush{ArenaAllocatorPool.get()};

    // Ranges that are fully transferred to DDisk and can be erased.
    // Using TSet for O(1) min LSN access.
    TPBufferKeySet ReadyToErase{ArenaAllocatorPool.get()};
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    TInfoEraseBelatedSet ReadyToEraseBelated{ArenaAllocatorPool.get()};

    // In-flight reads and the locks they create.
    ILockableRanges::TLockRangeHandle InflightDDiskReadsGenerator = 0;
    TInflightDDiskReadsMap InflightDDiskReads;

    // DDisks freshness state.
    TVector<TDDiskState> DDiskStates;
<<<<<<< HEAD
<<<<<<< HEAD
=======
    // Changes when behind/ahead map changes.
=======
    // Changes when the behind map changes.
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    ui32 StateGeneration = 0;
    // Last persisted DDisks states generation.
    ui32 PersistedStateGeneration = 0;
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))

    // PBuffers space usage counters.
    TVector<TPBufferCounters> PBufferCounters;
};

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> MakeLsnVector(std::span<const TPBufferSegment> segments);
TVector<ui64> MakeLsnVector(std::span<const TEraseSegment> segments);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
