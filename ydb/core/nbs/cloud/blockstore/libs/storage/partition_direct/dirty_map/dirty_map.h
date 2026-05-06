#pragma once

#include "inflight_info.h"
#include "location.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
#include <util/generic/set.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TReadRangeHint
{
    TReadRangeHint(
        TLocationMask locationMask,
        ui64 lsn,
        TBlockRange64 requestRelativeRange,
        TBlockRange64 vchunkRange,
        TRangeLock&& lock);

    TReadRangeHint(TReadRangeHint&& other) noexcept;
    TReadRangeHint& operator=(TReadRangeHint&& other) noexcept;

    TLocationMask LocationMask;
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
    using THints = TMap<TRoute, TFlushHint>;

    void AddHint(
        ELocation source,
        ELocation destination,
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
    using THints = TMap<ELocation, TEraseHint>;

    void AddHint(ELocation location, ui64 lsn, TBlockRange64 range);

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
    TBlocksDirtyMap(ui32 blockSize, ui64 blockCount);
    ~TBlocksDirtyMap() override;

    void UpdateConfig(TLocationMask desired, TLocationMask disabled);

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, ELocation location);

    // MakeReadHint can work with multiple locations and returns multiple
    // RangeHints
    [[nodiscard]] TReadHint MakeReadHint(TBlockRange64 range);
    [[nodiscard]] TFlushHints MakeFlushHint(size_t batchSize);
    [[nodiscard]] TEraseHints MakeEraseHint(size_t batchSize);

    void WriteFinished(
        ui64 lsn,
        TBlockRange64 range,
        TLocationMask requested,
        TLocationMask confirmed);
    void FlushFinished(
        TRoute route,
        const TVector<ui64>& flushOk,
        const TVector<ui64>& flushFailed);
    void EraseFinished(
        ELocation location,
        const TVector<ui64>& eraseOk,
        const TVector<ui64>& eraseFailed);

    // Sets a mark on the ddisk to which offset it contains data and can be read
    // from it.
    void MarkFresh(ELocation location, ui64 bytesOffset);
    // Returns the offset to which ddisk contains the data. nullopt means that
    // the disk is completely full of data. And you can read it from anywhere.
    [[nodiscard]] std::optional<ui64> GetFreshWatermark(
        ELocation location) const;
    // Sets the mark up to which the disk can be read.
    void SetReadWatermark(ELocation location, ui64 bytesOffset);
    // Sets the mark to which writes should be flushed to the ddisk.
    void SetFlushWatermark(ELocation location, ui64 bytesOffset);

    // Returns the number of in-flight write requests.
    [[nodiscard]] size_t GetInflightCount() const;
    [[nodiscard]] size_t GetFlushPendingCount() const;
    [[nodiscard]] size_t GetErasePendingCount() const;
    [[nodiscard]] ui64 GetMinFlushPendingLsn() const;
    [[nodiscard]] ui64 GetMinErasePendingLsn() const;
    [[nodiscard]] const TPBufferCounters& GetPBufferCounters(
        ELocation pbuffer) const;

    // ILockableRanges implementation
    void LockPBuffer(ui64 lsn) override;
    void UnlockPBuffer(ui64 lsn) override;
    TLockRangeHandle LockDDiskRange(
        TBlockRange64 range,
        TLocationMask mask) override;
    void UnLockDDiskRange(TLockRangeHandle handle) override;

    // IReadyQueue implementation
    void Register(ui64 lsn, EQueueType queueType) override;
    void UnRegister(ui64 lsn) override;
    void DataToPBufferAdded(
        ELocation location,
        EPBufferCounter counter,
        size_t byteCount) override;
    void DataFromPBufferReleased(
        ELocation location,
        EPBufferCounter counter,
        size_t byteCount) override;

    // Debug purposes
    [[nodiscard]] TString DebugPrintLockedDDiskRanges();
    [[nodiscard]] TString DebugPrintDDiskState() const;
    [[nodiscard]] TString DebugPrintReadyToFlush() const;

private:
    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, TLocationMask>;

    [[nodiscard]] TLocationMask FilterLocations(
        TLocationMask mask,
        TBlockRange64 range) const;

    // Create single readRangeHint for specified parameters
    [[nodiscard]] TReadRangeHint MakeReadRangeHint(
        TLocationMask locationMask,
        ui64 lsn,
        TBlockRange64 range,
        ui64 offsetBlocks);

    const ui32 BlockSize;
    const ui64 BlockCount;

    TLocationMask DesiredPBuffers = TLocationMask::MakePrimaryPBuffers();
    TLocationMask DesiredDDisks = TLocationMask::MakePrimaryDDisks();
    TLocationMask DisabledLocations;

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

    // DDisks freshness state.
    THolderForLocation<TDDiskState> DDiskStates;

    // PBuffers space usage counters.
    THolderForLocation<TPBufferCounters> PBufferCounters;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
