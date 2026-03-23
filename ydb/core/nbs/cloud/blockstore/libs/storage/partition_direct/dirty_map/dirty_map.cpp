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

TInflightInfo::TInflightInfo(ELocation location)
{
    WriteRequested.Set(location);
    WriteConfirmed.Set(location);
}

TInflightInfo::TInflightInfo(
    TLocationMask writeRequested,
    TLocationMask writeConfirmed)
    : WriteRequested(writeRequested)
    , WriteConfirmed(writeConfirmed)
{
    Y_ABORT_UNLESS(WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);
}

TInflightInfo::~TInflightInfo()
{
    Y_ABORT_UNLESS(LockCount == 0);
}

NThreading::TFuture<void> TInflightInfo::TInflightInfo::GetReadyFuture()
{
    if (!ReadyPromise.Initialized()) {
        ReadyPromise = NThreading::NewPromise<void>();
    }
    return ReadyPromise.GetFuture();
}

TInflightInfo::EState TInflightInfo::TInflightInfo::GetState() const
{
    if (EraseRequested == FlushConfirmed && !EraseRequested.Empty()) {
        // Started erasing from PBuffers.
        // There should be no readings from them.
        Y_ABORT_UNLESS(LockCount == 0);

        return (EraseRequested == EraseConfirmed) ? EState::PBufferErased
                                                  : EState::PBufferErasing;
    }

    if (FlushRequested == WriteConfirmed && !FlushRequested.Empty()) {
        // Started flushing to DDisk.

        if (FlushRequested != FlushConfirmed) {
            // Not all the flushes have been completed, which means that the
            // flushes are still being executed.
            return EState::PBufferFlushing;
        }

        if (LockCount > 0) {
            // If someone is reading from PBuffer.
            return EState::PBufferEraseLocked;
        }

        return EState::PBufferFlushed;
    }

    return WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount
               ? EState::PBufferWritten
               : EState::PBufferIncompleteWrite;
}

TLocationMask TInflightInfo::ReadMask() const
{
    switch (GetState()) {
        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return TLocationMask::MakeEmpty();

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer.
            return WriteConfirmed;

        case EState::PBufferFlushed:
        case EState::PBufferEraseLocked:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from primary DDisks.
            return TLocationMask::MakePrimaryDDisk();
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

    TReadRangeHint readHint = makeDefaultHint(range);
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const ui64 lsn = item.Key;
            if (readHint.Lsn < lsn) {
                readHint = makeHint(item.Value.ReadMask(), lsn, item.Range);
            }

            return TInflightMap::EEnumerateContinuation::Continue;
        });

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

    for (ui64 lsn: ReadyToFlush) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;

        if (InflightDDiskReads.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during reading from overlapped range.
            continue;
        }

        for (auto l: AllLocations) {
            if (val.WriteRequested.Get(l) && !val.FlushRequested.Get(l)) {
                result[l].Segments.push_back(
                    TPBufferSegment{.Lsn = item->Key, .Range = item->Range});
                val.FlushRequested.Set(l);
            }
        }
    }
    ReadyToFlush.clear();

    return result;
}

TMap<ELocation, TEraseHint> TBlocksDirtyMap::MakeEraseHint(size_t batchSize)
{
    TMap<ELocation, TEraseHint> result;

    if (ReadyToErase.size() < batchSize) {
        return result;
    }

    for (auto it = ReadyToErase.begin(); it != ReadyToErase.end();) {
        const ui64 lsn = *it;
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

        if (val.LockCount) {
            ++it;
            continue;
        }

        for (auto l: AllLocations) {
            if (val.WriteRequested.Get(l) && !val.EraseRequested.Get(l)) {
                result[l].Segments.push_back(
                    TPBufferSegment{.Lsn = item->Key, .Range = item->Range});
                val.EraseRequested.Set(l);
            }
        }

        if (val.EraseRequested == val.WriteRequested) {
            it = ReadyToErase.erase(it);
        } else {
            ++it;
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
    const bool inserted =
        Inflight.AddRange(lsn, range, TInflightInfo(requested, confirmed));
    Y_ABORT_UNLESS(inserted);

    RegisterInQueues(lsn, Inflight.GetValue(lsn)->Value);
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

        inflight.FlushConfirmed.Set(location);
        RegisterInQueues(lsn, inflight);
    }

    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.FlushRequested.Reset(location);
        RegisterInQueues(lsn, inflight);
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

        inflight.EraseConfirmed.Set(location);
        if (inflight.EraseConfirmed == inflight.EraseRequested) {
            const bool removed = Inflight.RemoveRange(item->Key);
            Y_ABORT_UNLESS(removed);
        } else {
            RegisterInQueues(lsn, inflight);
        }
    }

    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.EraseRequested.Reset(location);
        RegisterInQueues(lsn, inflight);
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
        inflight.WriteConfirmed.Set(location);
        inflight.WriteRequested.Set(location);
        RegisterInQueues(lsn, inflight);
    } else {
        Inflight.AddRange(lsn, range, TInflightInfo(location));
        RegisterInQueues(lsn, Inflight.GetValue(lsn)->Value);
    }
}

void TBlocksDirtyMap::PrepareReadyItems()
{
    Inflight.Enumerate(
        [&]   //
        (TInflightMap::TFindItem & item)
        {
            RegisterInQueues(item.Key, item.Value);
            return TInflightMap::EEnumerateContinuation::Continue;
        });
}

size_t TBlocksDirtyMap::GetInflightCount() const
{
    return Inflight.Size();
}

void TBlocksDirtyMap::LockPBuffer(ui64 lsn)
{
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item.has_value());
    ++item->Value.LockCount;
}

void TBlocksDirtyMap::UnlockPBuffer(ui64 lsn)
{
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item.has_value());
    --item->Value.LockCount;
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

void TBlocksDirtyMap::RegisterInQueues(ui64 lsn, const TInflightInfo& inflight)
{
    switch (inflight.GetState()) {
        case TInflightInfo::EState::PBufferIncompleteWrite: {
            ReadyToClone.insert(lsn);

            ReadyToFlush.erase(lsn);
            ReadyToErase.erase(lsn);
        } break;

        case TInflightInfo::EState::PBufferWritten: {
            ReadyToFlush.insert(lsn);

            ReadyToClone.erase(lsn);
            ReadyToErase.erase(lsn);
        } break;

        case TInflightInfo::EState::PBufferFlushed: {
            ReadyToErase.insert(lsn);

            ReadyToClone.erase(lsn);
            ReadyToFlush.erase(lsn);
        } break;

        case TInflightInfo::EState::PBufferFlushing:
        case TInflightInfo::EState::PBufferEraseLocked:
        case TInflightInfo::EState::PBufferErasing:
        case TInflightInfo::EState::PBufferErased: {
            ReadyToClone.erase(lsn);
            ReadyToFlush.erase(lsn);
            ReadyToErase.erase(lsn);
        } break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
