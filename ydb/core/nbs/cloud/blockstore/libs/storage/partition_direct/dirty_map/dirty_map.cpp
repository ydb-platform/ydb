#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    THostMask hostMask,
    bool fromDDisk,
    ui64 lsn,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : HostMask(hostMask)
    , FromDDisk(fromDDisk)
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
           << Lsn << "{" << HostMask.Print() << VChunkRange.Print()
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
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint();
        first = false;
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

void TFlushHints::AddHint(
    THostIndex source,
    THostIndex destination,
    ui64 lsn,
    TBlockRange64 range)
{
    Hints[THostRoute{
              .SourceHostIndex = source,
              .DestinationHostIndex = destination}]
        .Segments.emplace_back(lsn, range);
}

bool TFlushHints::Empty() const
{
    return Hints.empty();
}

const TFlushHints::THints& TFlushHints::GetAllHints() const
{
    return Hints;
}

TFlushHints::THints TFlushHints::TakeAllHints()
{
    return std::move(Hints);
}

TString TFlushHints::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& [r, hint]: Hints) {
        builder << "H" << ui32(r.SourceHostIndex) << "->H"
                << ui32(r.DestinationHostIndex) << ":" << hint.DebugPrint()
                << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString TEraseHint::DebugPrint() const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint();
        first = false;
    }
    return builder;
}

void TEraseHints::AddHint(THostIndex host, ui64 lsn, TBlockRange64 range)
{
    Hints[host].Segments.emplace_back(lsn, range);
}

bool TEraseHints::Empty() const
{
    return Hints.empty();
}

const TEraseHints::THints& TEraseHints::GetAllHints() const
{
    return Hints;
}

TEraseHints::THints TEraseHints::TakeAllHints()
{
    return std::move(Hints);
}

TString TEraseHints::DebugPrint() const
{
    TStringBuilder builder;
    for (const auto& [h, hint]: Hints) {
        builder << "H" << ui32(h) << ":" << hint.DebugPrint() << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TBlocksDirtyMap::TBlocksDirtyMap(ui32 blockSize, size_t hostCount)
    : BlockSize(blockSize)
{
    Y_ABORT_UNLESS(hostCount > 0);
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    PBufferCounters.resize(hostCount);
}

TBlocksDirtyMap::~TBlocksDirtyMap()
{
    Inflight.Enumerate(
        [&](TInflightMap::TFindItem& item)
        {
            item.Value.Detach();

            return TInflightMap::EEnumerateContinuation::Continue;
        });
}

void TBlocksDirtyMap::RestorePBuffer(
    ui64 lsn,
    TBlockRange64 range,
    THostIndex host)
{
    if (auto item = Inflight.GetValue(lsn)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(host);
    } else {
        Inflight.AddRange(
            lsn,
            range,
            TInflightInfo(this, lsn, range.Size() * BlockSize, host));
    }
}

TReadHint TBlocksDirtyMap::MakeReadHint(
    TBlockRange64 range,
    THostMask ddiskReadable,
    THostMask pbufferReadable,
    const TDDiskStateList& ddiskStates)
{
    TReadHint result;

    auto makeDefaultHint = [this, ddiskReadable, &ddiskStates](TBlockRange64 range)
    {
        auto hostMask = ddiskStates.FilterReadable(ddiskReadable, range);
        Y_ABORT_UNLESS(!hostMask.Empty());

        return TReadRangeHint(
            hostMask,
            /*fromDDisk=*/true,
            0,
            TBlockRange64::WithLength(0, range.Size()),
            range,
            TRangeLock(this, range, hostMask));
    };

    auto makeHint =
        [this,
         ddiskReadable,
         pbufferReadable](TReadSource src, ui64 lsn, TBlockRange64 range)
    {
        auto hostMask = src.Mask;
        Y_ABORT_UNLESS(!hostMask.Empty());

        if (src.FromDDisk) {
            hostMask = hostMask.LogicalAnd(ddiskReadable);
        } else {
            hostMask = hostMask.LogicalAnd(pbufferReadable);
        }
        Y_ABORT_UNLESS(!hostMask.Empty());

        return TReadRangeHint(
            hostMask,
            src.FromDDisk,
            lsn,
            TBlockRange64::WithLength(0, range.Size()),
            range,
            src.FromDDisk ? TRangeLock(this, range, hostMask)
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

    if (item->Value.ReadMask(ddiskReadable).Mask.Empty()) {
        // Reading from range without quorum is forbidden.
        // Caller should wait until PBuffers quorum will be made.
        result.WaitReady = item->Value.GetQuorumReadyFuture();
        Y_ABORT_UNLESS(result.RangeHints.empty());
    } else {
        result.RangeHints.push_back(makeHint(
            item->Value.ReadMask(ddiskReadable),
            item->Key,
            item->Range));
    }

    return result;
}

TFlushHints TBlocksDirtyMap::MakeFlushHint(
    size_t batchSize,
    THostMask ddiskFlushTargets,
    const TDDiskStateList& ddiskStates)
{
    TFlushHints result;

    if (ReadyToFlush.size() < batchSize) {
        return result;
    }

    TSet<ui64> readyToFlush;
    readyToFlush.swap(ReadyToFlush);

    auto countReadyToFlush = [&](TBlockRange64 range)
    {
        size_t result = 0;
        for (THostIndex destination: ddiskFlushTargets) {
            result +=
                ddiskStates.NeedFlushToDDisk(destination, range) ? 1 : 0;
        }
        return result;
    };

    for (ui64 lsn: readyToFlush) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;

        if (InflightDDiskReads.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during reading from overlapped range.
            ReadyToFlush.insert(lsn);
            continue;
        }

        if (countReadyToFlush(item->Range) < QuorumDirectBlockGroupHostCount) {
            // Can't flush to DDisk when disks to flush less then quorum.
            ReadyToFlush.insert(lsn);
            continue;
        }

        for (THostIndex destination: ddiskFlushTargets) {
            if (!ddiskStates.NeedFlushToDDisk(destination, item->Range)) {
                continue;
            }

            const THostIndex source = val.RequestFlush(destination);
            if (source != InvalidHostIndex) {
                result.AddHint(source, destination, item->Key, item->Range);
            }
        }
    }

    return result;
}

TEraseHints TBlocksDirtyMap::MakeEraseHint(
    size_t batchSize,
    THostMask pbufferEraseTargets)
{
    TEraseHints result;

    if (ReadyToErase.size() < batchSize) {
        return result;
    }

    TSet<ui64> readyToErase;
    readyToErase.swap(ReadyToErase);

    for (ui64 lsn: readyToErase) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

        for (THostIndex h: pbufferEraseTargets) {
            if (val.RequestErase(h)) {
                result.AddHint(h, item->Key, item->Range);
            }
        }
    }

    return result;
}

void TBlocksDirtyMap::WriteFinished(
    ui64 lsn,
    TBlockRange64 range,
    THostMask requested,
    THostMask confirmed)
{
    if (confirmed.Count() < QuorumDirectBlockGroupHostCount) {
        return;
    }

    const bool inserted = Inflight.AddRange(
        lsn,
        range,
        TInflightInfo(
            this,
            lsn,
            range.Size() * BlockSize,
            requested,
            confirmed));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::FlushFinished(
    THostRoute route,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    for (ui64 lsn: flushOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.ConfirmFlush(route);
    }

    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.FlushFailed(route);
    }
}

void TBlocksDirtyMap::EraseFinished(
    THostIndex host,
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    for (ui64 lsn: eraseOk) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        if (inflight.ConfirmErase(host)) {
            const bool removed = Inflight.RemoveRange(item->Key);
            Y_ABORT_UNLESS(removed);
        }
    }

    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);
        auto& inflight = item->Value;

        inflight.EraseFailed(host);
    }
}

size_t TBlocksDirtyMap::GetInflightCount() const
{
    return Inflight.Size();
}

size_t TBlocksDirtyMap::GetFlushPendingCount() const
{
    return ReadyToFlush.size();
}

size_t TBlocksDirtyMap::GetErasePendingCount() const
{
    return ReadyToErase.size();
}

ui64 TBlocksDirtyMap::GetMinFlushPendingLsn() const
{
    if (ReadyToFlush.empty()) {
        return 0;
    }
    // TSet is ordered, so the first element is the minimum. O(1) access.
    return *ReadyToFlush.begin();
}

ui64 TBlocksDirtyMap::GetMinErasePendingLsn() const
{
    if (ReadyToErase.empty()) {
        return 0;
    }
    // TSet is ordered, so the first element is the minimum. O(1) access.
    return *ReadyToErase.begin();
}

const TPBufferCounters& TBlocksDirtyMap::GetPBufferCounters(
    THostIndex host) const
{
    return PBufferCounters[host];
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
    TBlockRange64 range,
    THostMask mask)
{
    // Checking that there are no inflight flushes for the range in which the
    // reading is being done.
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const auto state = item.Value.GetState();

            if (state != TInflightInfo::EState::PBufferFlushing) {
                Y_ABORT_UNLESS(
                    item.Value.GetRequestedFlushes().LogicalAnd(mask).Empty());
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    const TLockRangeHandle handle = ++InflightDDiskReadsGenerator;
    InflightDDiskReads.AddRange(handle, range, mask);
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

void TBlocksDirtyMap::DataToPBufferAdded(
    THostIndex host,
    EPBufferCounter counter,
    size_t byteCount)
{
    auto& counters = PBufferCounters[host];

    switch (counter) {
        case IReadyQueue::EPBufferCounter::Total: {
            counters.CurrentRecordsCount++;
            counters.CurrentBytesCount += byteCount;
            counters.TotalRecordsCount++;
            counters.TotalBytesCount += byteCount;
            break;
        }
        case IReadyQueue::EPBufferCounter::Locked: {
            counters.CurrentLockedRecordsCount++;
            counters.CurrentLockedBytesCount += byteCount;
            counters.TotalLockedRecordsCount++;
            counters.TotalLockedBytesCount += byteCount;
            break;
        }
    }
}

void TBlocksDirtyMap::DataFromPBufferReleased(
    THostIndex host,
    EPBufferCounter counter,
    size_t byteCount)
{
    auto& counters = PBufferCounters[host];

    switch (counter) {
        case IReadyQueue::EPBufferCounter::Total: {
            Y_ABORT_UNLESS(counters.CurrentRecordsCount > 0);
            Y_ABORT_UNLESS(counters.CurrentBytesCount >= byteCount);

            counters.CurrentRecordsCount--;
            counters.CurrentBytesCount -= byteCount;
            break;
        }
        case IReadyQueue::EPBufferCounter::Locked: {
            Y_ABORT_UNLESS(counters.CurrentLockedRecordsCount > 0);
            Y_ABORT_UNLESS(counters.CurrentLockedBytesCount >= byteCount);

            counters.CurrentLockedRecordsCount--;
            counters.CurrentLockedBytesCount -= byteCount;
            break;
        }
    }
}

TString TBlocksDirtyMap::DebugPrintLockedDDiskRanges()
{
    TBlockRangeSet64 ranges;
    InflightDDiskReads.Enumerate(
        [&](TInflightDDiskReadsMap::TFindItem& item)
        {
            ranges.insert(item.Range);
            return TInflightDDiskReadsMap::EEnumerateContinuation::Continue;
        });
    TStringBuilder result;
    for (const auto& range: ranges) {
        result << range.Print();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
