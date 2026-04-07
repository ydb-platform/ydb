#pragma once

#include "location.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>

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

struct IReadyQueue
{
    enum class EQueueType
    {
        Clone,
        Flush,
        Erase,
    };

    virtual ~IReadyQueue() = default;

    // Registers an Lsn ready for cloning, flushing, or erasing.
    // An Lsn can only be registered in one queue. The new registration deletes
    // the old one.
    virtual void Register(ui64 lsn, EQueueType queueType) = 0;

    // Removes all registrations from Lsn.
    virtual void UnRegister(ui64 lsn) = 0;
};

class TInflightInfo: public TDisableCopy
{
public:
    enum class EState
    {
        // During the recovery, a item without quorum was detected. It must be
        // copied to other PBuffers.
        // Reading will be possible only after receiving a quorum.
        PBufferIncompleteWrite,

        // Data written to PBuffers with quorum.
        // Read from any confirmed PBuffer.
        PBufferWritten,

        // Started flushing from PBuffers to DDisk.
        // Read from any confirmed PBuffer.
        PBufferFlushing,

        // Data flushed to DDisk.
        // Read from DDisk.
        PBufferFlushed,

        // The data is now being erasing from the PBuffers.
        // Read from DDisk.
        PBufferErasing,

        // The data is erased from the PBuffers.
        // Read from DDisk.
        PBufferErased,
    };

    TInflightInfo(IReadyQueue* readyQueues, ui64 lsn, ELocation location);
    TInflightInfo(
        IReadyQueue* readyQueues,
        ui64 lsn,
        TLocationMask writeRequested,
        TLocationMask writeConfirmed);

    TInflightInfo(TInflightInfo&& other) noexcept;

    ~TInflightInfo();

    void RestorePBuffer(ELocation location);

    [[nodiscard]] EState GetState() const;

    // The subscription is triggered when the quorum is reached.
    [[nodiscard]] NThreading::TFuture<void> GetQuorumReadyFuture();

    // The mask from which data sources can be read.
    [[nodiscard]] TLocationMask ReadMask() const;

    // Returns the PBuffer source from where the data will be transferred to
    // DDisk, specified in the parameter destination. If ELocation::Unknown is
    // returned, it means that the transfer of data to destination has already
    // been requested earlier.
    [[nodiscard]] ELocation RequestFlush(ELocation destination);
    void ConfirmFlush(TRoute route);
    void FlushFailed(TRoute route);
    [[nodiscard]] TLocationMask GetRequestedFlushes() const;

    // Returns true when erase request needed.
    [[nodiscard]] bool RequestErase(ELocation location);
    // Returns true when all erases confirmed.
    [[nodiscard]] bool ConfirmErase(ELocation location);
    void EraseFailed(ELocation location);

    // Sets a lock that prohibits erasing the PBuffer.
    void LockPBuffer();
    // Removes the lock that prohibits erasing the PBuffer.
    void UnlockPBuffer();

private:
    EState State;

    IReadyQueue* ReadyQueue = nullptr;
    ui64 Lsn = 0;
    TInstant StartAt;
    size_t PBuffersLockCount = 0;
    NThreading::TPromise<void> QuorumReadyPromise;

    TLocationMask WriteRequested;
    TLocationMask WriteConfirmed;
    TLocationMask FlushDesired;
    TLocationMask FlushRequested;
    TLocationMask FlushConfirmed;
    TLocationMask EraseRequested;
    TLocationMask EraseConfirmed;
};

class TBlocksDirtyMap
    : public ILockableRanges
    , public IReadyQueue
    , public TDisableCopyMove
{
public:
    TBlocksDirtyMap(ui32 blockSize, ui64 blockCount);

    void UpdateConfig(TLocationMask desired, TLocationMask disabled);

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, ELocation location);

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

    // Debug purposes
    [[nodiscard]] TString DebugPrintLockedDDiskRanges();
    [[nodiscard]] TString DebugPrintDDiskState() const;

private:
    struct TEmpty
    {
    };

    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, TLocationMask>;

    [[nodiscard]] TLocationMask FilterLocations(
        TLocationMask mask,
        TBlockRange64 range) const;

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
    // DDisk.
    THashSet<ui64> ReadyToFlush;

    // Ranges that are fully transferred to DDisk and can be erased.
    THashSet<ui64> ReadyToErase;

    // In-flight reads and the locks they create.
    ILockableRanges::TLockRangeHandle InflightDDiskReadsGenerator = 0;
    TInflightDDiskReadsMap InflightDDiskReads;

    // DDisks freshness state.
    THolderForLocation<TDDiskState> DDiskStates;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
