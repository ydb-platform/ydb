#pragma once

#include "location.h"
#include "range_locker.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>

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
};

struct TReadHint
{
    // If the RangeHints is empty, then you need to wait for the WaitReady
    // feature to be IsReady and repeat the request.
    TVector<TReadRangeHint> RangeHints;
    NThreading::TFuture<void> WaitReady;
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

struct TEraseHint
{
    TVector<TPBufferSegment> Segments;

    [[nodiscard]] TString DebugPrint() const;
};

struct TInflightInfo
{
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

        // The data is now reading from PBuffers.
        // Can't erase from PBuffers.
        PBufferEraseLocked,

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

    TInflightInfo() = default;
    explicit TInflightInfo(ELocation location);
    TInflightInfo(TLocationMask writeRequested, TLocationMask writeConfirmed);

    ~TInflightInfo();

    TLocationMask WriteRequested;
    TLocationMask WriteConfirmed;
    TLocationMask FlushRequested;
    TLocationMask FlushConfirmed;
    TLocationMask EraseRequested;
    TLocationMask EraseConfirmed;

    TInstant StartAt;

    // Blocking while reading from PBUffer is taking place.
    size_t LockCount = 0;

    NThreading::TPromise<void> ReadyPromise;

    [[nodiscard]] NThreading::TFuture<void> GetReadyFuture();
    [[nodiscard]] EState GetState() const;
    [[nodiscard]] TLocationMask ReadMask() const;
};

class TBlocksDirtyMap
    : public ILockableRanges
    , public TDisableCopyMove
{
public:
    [[nodiscard]] TReadHint MakeReadHint(TBlockRange64 range);
    [[nodiscard]] TMap<ELocation, TFlushHint> MakeFlushHint(size_t batchSize);
    [[nodiscard]] TMap<ELocation, TEraseHint> MakeEraseHint(size_t batchSize);

    void WriteFinished(
        ui64 lsn,
        TBlockRange64 range,
        TLocationMask requested,
        TLocationMask confirmed);
    void FlushFinished(
        ELocation location,
        const TVector<ui64>& flushOk,
        const TVector<ui64>& flushFailed);
    void EraseFinished(
        ELocation location,
        const TVector<ui64>& eraseOk,
        const TVector<ui64>& eraseFailed);

    void RestorePBuffer(ui64 lsn, TBlockRange64 range, ELocation location);
    void PrepareReadyItems();

    [[nodiscard]] size_t GetInflightCount() const;

    // ILockableRanges implementation
    void LockPBuffer(ui64 lsn) override;
    void UnlockPBuffer(ui64 lsn) override;
    TLockRangeHandle LockDDiskRange(TBlockRange64 range) override;
    void UnLockDDiskRange(TLockRangeHandle handle) override;

private:
    struct TEmpty
    {
    };

    using TInflightMap = TBlockRangeMap<ui64, TInflightInfo>;
    using TInflightDDiskReadsMap =
        TBlockRangeMap<ILockableRanges::TLockRangeHandle, TEmpty>;

    void RegisterInQueues(ui64 lsn, const TInflightInfo& inflight);

    TInflightMap Inflight;

    // Ranges that need to be copied to other PBuffers in order to reach a
    // quorum.
    TSet<ui64> ReadyToClone;

    // Ranges that are written PBuffers with quorum and ready to be flushed to
    // DDisk.
    TSet<ui64> ReadyToFlush;

    // Ranges that are fully transferred to DDisk and can be erased.
    TSet<ui64> ReadyToErase;

    ILockableRanges::TLockRangeHandle InflightDDiskReadsGenerator = 0;
    TInflightDDiskReadsMap InflightDDiskReads;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
