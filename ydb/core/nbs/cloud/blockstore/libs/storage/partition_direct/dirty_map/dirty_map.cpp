#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    TLocationMask locationMask,
    ui64 lsn,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : LocationMask(locationMask)
    , Lsn(lsn)
    , RequestRelativeRange(requestRelativeRange)
    , VChunkRange(vchunkRange)
    , Lock(std::move(lock))
{}

TReadRangeHint::TReadRangeHint(TReadRangeHint&& other) noexcept = default;
TReadRangeHint& TReadRangeHint::operator=(
    TReadRangeHint&& other) noexcept = default;

TString TReadRangeHint::DebugPrint() const
{
    return TStringBuilder()
           << Lsn << "{" << LocationMask.Print() << VChunkRange.Print()
           << RequestRelativeRange.Print() << "};";
}

TString TReadHint::DebugPrint() const
{
    if (RangeHints.empty()) {
        return (WaitReady.IsReady()) ? "WaitReady:Ready" : "WaitReady:NotReady";
    }

    TStringBuilder result;
    for (const auto& hint: RangeHints) {
        result << hint.DebugPrint();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TString TPBufferSegment::DebugPrint() const
{
    return TStringBuilder() << Lsn << Range.Print();
}

TString TFlushHint::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& segment: Segments) {
        builder << segment.DebugPrint() << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString TEraseHint::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& segment: Segments) {
        builder << segment.DebugPrint() << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueues,
    ui64 lsn,
    ELocation location)
    : ReadyQueue(readyQueues)
    , Lsn(lsn)
{
    WriteRequested.Set(location);
    WriteConfirmed.Set(location);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
}

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueue,
    ui64 lsn,
    TLocationMask writeRequested,
    TLocationMask writeConfirmed)
    : ReadyQueue(readyQueue)
    , Lsn(lsn)
    , WriteRequested(writeRequested)
    , WriteConfirmed(writeConfirmed)
{
    Y_ABORT_UNLESS(WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
    : ReadyQueue(other.ReadyQueue)
    , Lsn(other.Lsn)
    , WriteRequested(other.WriteRequested)
    , WriteConfirmed(other.WriteConfirmed)
    , FlushRequested(other.FlushRequested)
    , FlushConfirmed(other.FlushConfirmed)
{
    other.ReadyQueue = nullptr;
}

TInflightInfo::~TInflightInfo()
{
    Y_ABORT_UNLESS(PBuffersLockCount == 0);
}

NThreading::TFuture<void> TInflightInfo::TInflightInfo::GetReadyFuture()
{
    if (!ReadyPromise.Initialized()) {
        ReadyPromise = NThreading::NewPromise<void>();
    }
    return ReadyPromise.GetFuture();
}

TInflightInfo::EStatus TInflightInfo::TInflightInfo::GetStatus() const
{
    if (EraseRequested == FlushConfirmed && !EraseRequested.Empty()) {
        // Started erasing from PBuffers.
        // There should be no readings from them.
        Y_ABORT_UNLESS(PBuffersLockCount == 0);

        return (EraseRequested == EraseConfirmed) ? EStatus::PBufferErased
                                                  : EStatus::PBufferErasing;
    }

    if (FlushRequested == WriteConfirmed && !FlushRequested.Empty()) {
        // Started flushing to DDisk.

        if (FlushRequested != FlushConfirmed) {
            // Not all the flushes have been completed, which means that the
            // flushes are still being executed.
            return EStatus::PBufferFlushing;
        }

        if (PBuffersLockCount > 0) {
            // If someone is reading from PBuffer.
            return EStatus::PBufferEraseLocked;
        }

        return EStatus::PBufferFlushed;
    }

    return WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount
               ? EStatus::PBufferWritten
               : EStatus::PBufferIncompleteWrite;
}

TLocationMask TInflightInfo::ReadMask() const
{
    switch (GetStatus()) {
        case EStatus::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return TLocationMask::MakeEmpty();

        case EStatus::PBufferWritten:
        case EStatus::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer.
            return WriteConfirmed;

        case EStatus::PBufferFlushed:
        case EStatus::PBufferEraseLocked:
        case EStatus::PBufferErasing:
        case EStatus::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from primary DDisks.
            return TLocationMask::MakePrimaryDDisk();
    }
}

bool TInflightInfo::RequestFlush(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));

    if (WriteConfirmed.Get(location) && !FlushRequested.Get(location)) {
        FlushRequested.Set(location);
        return true;
    }
    return false;
}

bool TInflightInfo::RequestErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    if (FlushConfirmed.Get(location) && !EraseRequested.Get(location)) {
        EraseRequested.Set(location);
        return true;
    }
    return false;
}

void TInflightInfo::ConfirmFlush(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(FlushRequested.Get(location));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(location));

    FlushConfirmed.Set(location);
    if (WriteConfirmed == FlushRequested && FlushRequested == FlushConfirmed) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::FlushFailed(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(FlushRequested.Get(location));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(location));

    FlushRequested.Reset(location);

    UpdateReadyQueue();
}

bool TInflightInfo::ConfirmErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(EraseRequested.Get(location));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseConfirmed.Set(location);

    return EraseConfirmed == EraseRequested;
}

void TInflightInfo::EraseFailed(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseRequested.Reset(location);

    UpdateReadyQueue();
}

void TInflightInfo::RestorePBuffer(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(!WriteRequested.Get(location));
    Y_ABORT_UNLESS(!WriteConfirmed.Get(location));

    WriteRequested.Set(location);
    WriteConfirmed.Set(location);

    ReadyQueue->Register(
        Lsn,
        WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount
            ? IReadyQueue::EQueueType::Flush
            : IReadyQueue::EQueueType::Clone);
}

void TInflightInfo::LockPBuffer()
{
    ++PBuffersLockCount;
    if (PBuffersLockCount == 1) {
        UpdateReadyQueue();
    }
}

void TInflightInfo::UnlockPBuffer()
{
    Y_ABORT_UNLESS(PBuffersLockCount > 0);

    --PBuffersLockCount;
    if (PBuffersLockCount == 0) {
        UpdateReadyQueue();
    }
}

void TInflightInfo::UpdateReadyQueue()
{
    switch (GetStatus()) {
        case EStatus::PBufferIncompleteWrite: {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Clone);
            break;
        }
        case EStatus::PBufferWritten: {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
            break;
        }
        case EStatus::PBufferFlushed: {
            ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
            break;
        }
        case EStatus::PBufferFlushing:
        case EStatus::PBufferEraseLocked:
        case EStatus::PBufferErasing:
        case EStatus::PBufferErased: {
            ReadyQueue->UnRegister(Lsn);
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    TReadHint result;

    auto makeDefaultHint = [this](TBlockRange64 range)
    {
        return TReadRangeHint(
            TLocationMask::MakePrimaryDDisk(),
            0,
            TBlockRange64::WithLength(0, range.Size()),
            range,
            TRangeLock(this, range));
    };
    auto makeHint =
        [this](TLocationMask location, ui64 lsn, TBlockRange64 range)
    {
        return TReadRangeHint(
            location,
            lsn,
            TBlockRange64::WithLength(0, range.Size()),
            range,
            location.OnlyDDisk() ? TRangeLock(this, range)
                                 : TRangeLock(this, lsn));
    };

    if (!Inflight.HasOverlaps(range)) {
        result.RangeHints.push_back(makeDefaultHint(range));
        return result;
    }

    ui64 maxLsn = 0;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const ui64 lsn = item.Key;
            maxLsn = std::max(maxLsn, lsn);

            return TInflightMap::EEnumerateContinuation::Continue;
        });

    const auto item = Inflight.GetValue(maxLsn);

    // Read from DDisk if the range is not covered by the inflight range.
    if (!item->Range.Contains(range)) {
        result.RangeHints.push_back(makeDefaultHint(range));
        return result;
    }

    TReadRangeHint readHint =
        makeHint(item->Value.ReadMask(), item->Key, item->Range);
    if (readHint.LocationMask.Empty()) {
        // Reading from range with blocked lsn is forbidden.
        // Caller should wait until PBuffers quorum will be made.
        auto item = Inflight.GetValue(readHint.Lsn);
        Y_ABORT_UNLESS(item);
        result.WaitReady = item->Value.GetReadyFuture();
        Y_ABORT_UNLESS(result.RangeHints.empty());
    } else {
        result.RangeHints.push_back(std::move(readHint));
    }

    return result;
}

TMap<ELocation, TFlushHint> TBlocksDirtyMap::MakeFlushHint(size_t batchSize)
{
    TMap<ELocation, TFlushHint> result;

    if (ReadyToFlush.size() < batchSize) {
        return result;
    }

    THashSet<ui64> readyToFlush;
    readyToFlush.swap(ReadyToFlush);

    for (ui64 lsn: readyToFlush) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;

        if (InflightDDiskReads.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during reading from overlapped range.
            ReadyToFlush.insert(lsn);
            continue;
        }

        for (auto l: PBufferLocations) {
            if (val.RequestFlush(l)) {
                result[l].Segments.push_back(
                    TPBufferSegment{.Lsn = item->Key, .Range = item->Range});
            }
        }
    }

    return result;
}

TMap<ELocation, TEraseHint> TBlocksDirtyMap::MakeEraseHint(size_t batchSize)
{
    TMap<ELocation, TEraseHint> result;

    if (ReadyToErase.size() < batchSize) {
        return result;
    }

    THashSet<ui64> readyToErase;
    readyToErase.swap(ReadyToErase);

    for (ui64 lsn: readyToErase) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

        for (auto l: PBufferLocations) {
            if (val.RequestErase(l)) {
                result[l].Segments.push_back(
                    TPBufferSegment{.Lsn = item->Key, .Range = item->Range});
            }
        }
    }

    return result;
}

void TBlocksDirtyMap::WriteFinished(
    ui64 lsn,
    TBlockRange64 range,
    TLocationMask requested,
    TLocationMask confirmed)
{
    const bool inserted = Inflight.AddRange(
        lsn,
        range,
        TInflightInfo(this, lsn, requested, confirmed));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::FlushFinished(
    ELocation location,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    for (ui64 lsn: flushOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.ConfirmFlush(location);
    }

    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.FlushFailed(location);
    }
}

void TBlocksDirtyMap::EraseFinished(
    ELocation location,
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    for (ui64 lsn: eraseOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        if (inflight.ConfirmErase(location)) {
            const bool removed = Inflight.RemoveRange(item->Key);
            Y_ABORT_UNLESS(removed);
        }
    }

    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.EraseFailed(location);
    }
}

void TBlocksDirtyMap::RestorePBuffer(
    ui64 lsn,
    TBlockRange64 range,
    ELocation location)
{
    if (auto item = Inflight.GetValue(lsn)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(location);
    } else {
        Inflight.AddRange(lsn, range, TInflightInfo(this, lsn, location));
    }
}

size_t TBlocksDirtyMap::GetInflightCount() const
{
    return Inflight.Size();
}

void TBlocksDirtyMap::LockPBuffer(ui64 lsn)
{
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item.has_value());
    item->Value.LockPBuffer();
}

void TBlocksDirtyMap::UnlockPBuffer(ui64 lsn)
{
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item.has_value());
    item->Value.UnlockPBuffer();
}

ILockableRanges::TLockRangeHandle TBlocksDirtyMap::LockDDiskRange(
    TBlockRange64 range)
{
    const TLockRangeHandle handle = ++InflightDDiskReadsGenerator;
    InflightDDiskReads.AddRange(handle, range);
    return handle;
}

void TBlocksDirtyMap::UnLockDDiskRange(TLockRangeHandle handle)
{
    InflightDDiskReads.RemoveRange(handle);
}

void TBlocksDirtyMap::Register(ui64 lsn, EQueueType queueType)
{
    switch (queueType) {
        case IReadyQueue::EQueueType::Clone: {
            ReadyToClone.insert(lsn);

            ReadyToFlush.erase(lsn);
            ReadyToErase.erase(lsn);
            break;
        }
        case IReadyQueue::EQueueType::Flush: {
            ReadyToFlush.insert(lsn);

            ReadyToClone.erase(lsn);
            ReadyToErase.erase(lsn);
            break;
        }
        case IReadyQueue::EQueueType::Erase: {
            ReadyToErase.insert(lsn);

            ReadyToClone.erase(lsn);
            ReadyToFlush.erase(lsn);
            break;
        }
    }
}

void TBlocksDirtyMap::UnRegister(ui64 lsn)
{
    ReadyToErase.erase(lsn);
    ReadyToClone.erase(lsn);
    ReadyToFlush.erase(lsn);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
