#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

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
    ELocation source,
    ELocation destination,
    ui64 lsn,
    TBlockRange64 range)
{
    Hints[TRoute{.Source = source, .Destination = destination}]
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
    for (const auto& [l, hint]: Hints) {
        builder << ToString(l.Source) << "->" << ToString(l.Destination) << ":"
                << hint.DebugPrint() << ";";
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

void TEraseHints::AddHint(ELocation location, ui64 lsn, TBlockRange64 range)
{
    Hints[location].Segments.emplace_back(lsn, range);
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
    for (const auto& [l, hint]: Hints) {
        builder << ToString(l) << ":" << hint.DebugPrint() << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

void TDDiskState::Init(ui64 totalBlockCount, ui64 operationalBlockCount)
{
    TotalBlockCount = totalBlockCount;
    SetFlushWatermark(operationalBlockCount);
    SetReadWatermark(operationalBlockCount);
}

TDDiskState::EState TDDiskState::GetState() const
{
    return State;
}

bool TDDiskState::CanReadFromDDisk(TBlockRange64 range) const
{
    return State == EState::Operational || range.End < OperationalBlockCount;
}

bool TDDiskState::NeedFlushToDDisk(TBlockRange64 range) const
{
    return State == EState::Operational || range.Start < FlushableBlockCount;
}

void TDDiskState::SetReadWatermark(ui64 blockCount)
{
    OperationalBlockCount = blockCount;
    UpdateState();
}

void TDDiskState::SetFlushWatermark(ui64 blockCount)
{
    FlushableBlockCount = blockCount;
    UpdateState();
}

ui64 TDDiskState::GetOperationalBlockCount() const
{
    return OperationalBlockCount;
}

TString TDDiskState::DebugPrint() const
{
    return TStringBuilder()
           << "{" << ToString(State) << "," << OperationalBlockCount << ","
           << FlushableBlockCount << "}";
}

void TDDiskState::UpdateState()
{
    Y_ABORT_UNLESS(OperationalBlockCount <= TotalBlockCount);
    Y_ABORT_UNLESS(FlushableBlockCount <= TotalBlockCount);

    State = (OperationalBlockCount == TotalBlockCount &&
             FlushableBlockCount == TotalBlockCount)
                ? EState::Operational
                : EState::Fresh;
}

////////////////////////////////////////////////////////////////////////////////

TInflightInfo::TInflightInfo(
    IReadyQueue* readyQueues,
    ui64 lsn,
    ELocation location)
    : State(EState::PBufferIncompleteWrite)
    , ReadyQueue(readyQueues)
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
    : State(EState::PBufferWritten)
    , ReadyQueue(readyQueue)
    , Lsn(lsn)
    , WriteRequested(writeRequested)
    , WriteConfirmed(writeConfirmed)
{
    Y_ABORT_UNLESS(WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

TInflightInfo::TInflightInfo(TInflightInfo&& other) noexcept
    : State(other.State)
    , ReadyQueue(other.ReadyQueue)
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

void TInflightInfo::RestorePBuffer(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(
        State == EState::PBufferIncompleteWrite ||
        State == EState::PBufferWritten);
    Y_ABORT_UNLESS(!WriteRequested.Get(location));
    Y_ABORT_UNLESS(!WriteConfirmed.Get(location));

    WriteRequested.Set(location);
    WriteConfirmed.Set(location);

    if (WriteConfirmed.Count() >= QuorumDirectBlockGroupHostCount) {
        if (QuorumReadyPromise.Initialized()) {
            QuorumReadyPromise.TrySetValue();
        }

        State = EState::PBufferWritten;
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
    }
}

TInflightInfo::EState TInflightInfo::GetState() const
{
    return State;
}

NThreading::TFuture<void> TInflightInfo::GetQuorumReadyFuture()
{
    if (!QuorumReadyPromise.Initialized()) {
        QuorumReadyPromise = NThreading::NewPromise<void>();
    }
    return QuorumReadyPromise.GetFuture();
}

TLocationMask TInflightInfo::ReadMask() const
{
    switch (State) {
        case EState::PBufferIncompleteWrite:
            // Reading will be possible only after receiving a quorum.
            return TLocationMask::MakeEmpty();

        case EState::PBufferWritten:
        case EState::PBufferFlushing:
            // The data is written to PBuffer, but not transferred to DDisk.
            // Will read from confirmed PBuffer.
            return WriteConfirmed;

        case EState::PBufferFlushed:
        case EState::PBufferErasing:
        case EState::PBufferErased:
            // The data has already been transferred to DDisk.
            // Will read from DDisks.
            // Filter out non-desired or fresh later.
            return TLocationMask::MakeAllDDisks();
    }
}

ELocation TInflightInfo::RequestFlush(ELocation destination)
{
    Y_ABORT_UNLESS(IsDDisk(destination));
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing);

    FlushDesired.Set(destination);

    if (FlushRequested.Get(destination)) {
        return ELocation::Unknown;
    }

    const ELocation bestSource = TranslateDDiskToPBuffer(destination);
    if (WriteConfirmed.Get(bestSource)) {
        State = EState::PBufferFlushing;
        FlushRequested.Set(destination);
        return bestSource;
    }

    for (ELocation source: PBufferLocations) {
        if (WriteConfirmed.Get(source)) {
            State = EState::PBufferFlushing;
            FlushRequested.Set(destination);
            return source;
        }
    }

    Y_ABORT_UNLESS(false);
}

void TInflightInfo::ConfirmFlush(TRoute route)
{
    Y_ABORT_UNLESS(IsDDisk(route.Destination));
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.Destination));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.Destination));

    FlushConfirmed.Set(route.Destination);

    if (FlushDesired == FlushConfirmed) {
        State = EState::PBufferFlushed;
    }

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

void TInflightInfo::FlushFailed(TRoute route)
{
    Y_ABORT_UNLESS(IsDDisk(route.Destination));
    Y_ABORT_UNLESS(State == EState::PBufferFlushing);
    Y_ABORT_UNLESS(FlushRequested.Get(route.Destination));
    Y_ABORT_UNLESS(!FlushConfirmed.Get(route.Destination));

    FlushRequested.Reset(route.Destination);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Flush);
}

TLocationMask TInflightInfo::GetRequestedFlushes() const
{
    return FlushRequested;
}

bool TInflightInfo::RequestErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(
        State == EState::PBufferFlushed || State == EState::PBufferErasing);
    Y_ABORT_UNLESS(FlushConfirmed.Count() >= QuorumDirectBlockGroupHostCount);

    if (WriteRequested.Get(location) && !EraseRequested.Get(location)) {
        State = EState::PBufferErasing;
        EraseRequested.Set(location);
        return true;
    }
    return false;
}

bool TInflightInfo::ConfirmErase(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(EraseRequested.Get(location));
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseConfirmed.Set(location);
    if (EraseConfirmed == EraseRequested) {
        State = EState::PBufferErased;
    }

    return State == EState::PBufferErased;
}

void TInflightInfo::EraseFailed(ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));
    Y_ABORT_UNLESS(State == EState::PBufferErasing);
    Y_ABORT_UNLESS(!EraseConfirmed.Get(location));

    EraseRequested.Reset(location);
    ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
}

void TInflightInfo::LockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);

    ++PBuffersLockCount;

    if (PBuffersLockCount == 1) {
        ReadyQueue->UnRegister(Lsn);
    }
}

void TInflightInfo::UnlockPBuffer()
{
    Y_ABORT_UNLESS(
        State == EState::PBufferWritten || State == EState::PBufferFlushing ||
        State == EState::PBufferFlushed);
    Y_ABORT_UNLESS(PBuffersLockCount > 0);

    --PBuffersLockCount;

    if (State == EState::PBufferFlushed && PBuffersLockCount == 0) {
        ReadyQueue->Register(Lsn, IReadyQueue::EQueueType::Erase);
    }
}

////////////////////////////////////////////////////////////////////////////////

TBlocksDirtyMap::TBlocksDirtyMap(ui32 blockSize, ui64 blockCount)
    : BlockSize(blockSize)
    , BlockCount(blockCount)
{
    for (auto l: DDiskLocations) {
        DDiskStates[l].Init(BlockCount, BlockCount);
    }
}

void TBlocksDirtyMap::UpdateConfig(
    TLocationMask desired,
    TLocationMask disabled)
{
    Y_ABORT_UNLESS(disabled.LogicalAnd(desired).Empty());

    DesiredDDisks = desired.LogicalAnd(TLocationMask::MakeAllDDisks());
    DesiredPBuffers = desired.LogicalAnd(TLocationMask::MakeAllPBuffers());
    DisabledLocations = disabled;
}

void TBlocksDirtyMap::RestorePBuffer(
    ui64 lsn,
    TBlockRange64 range,
    ELocation location)
{
    Y_ABORT_UNLESS(IsPBuffer(location));

    if (auto item = Inflight.GetValue(lsn)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(location);
    } else {
        Inflight.AddRange(lsn, range, TInflightInfo(this, lsn, location));
    }
}

TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    TReadHint result;

    auto makeDefaultHint = [this](TBlockRange64 range)
    {
        // Filter out disabled locations.
        auto locationMask = FilterLocations(DesiredDDisks, range);
        Y_ABORT_UNLESS(!locationMask.Empty());

        return TReadRangeHint(
            locationMask,
            0,
            TBlockRange64::WithLength(0, range.Size()),
            range,
            TRangeLock(this, range, locationMask));
    };

    auto makeHint =
        [this](TLocationMask locationMask, ui64 lsn, TBlockRange64 range)
    {
        Y_ABORT_UNLESS(!locationMask.Empty());

        // Filter out disabled locations.
        if (locationMask.HasDDisk()) {
            locationMask = locationMask.LogicalAnd(DesiredDDisks);
        }
        locationMask = locationMask.Exclude(DisabledLocations);
        Y_ABORT_UNLESS(!locationMask.Empty());

        return TReadRangeHint(
            locationMask,
            lsn,
            TBlockRange64::WithLength(0, range.Size()),
            range,
            locationMask.OnlyDDisk() ? TRangeLock(this, range, locationMask)
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

    if (item->Value.ReadMask().Empty()) {
        // Reading from range without quorum is forbidden.
        // Caller should wait until PBuffers quorum will be made.
        result.WaitReady = item->Value.GetQuorumReadyFuture();
        Y_ABORT_UNLESS(result.RangeHints.empty());
    } else {
        result.RangeHints.push_back(
            makeHint(item->Value.ReadMask(), item->Key, item->Range));
    }

    return result;
}

TFlushHints TBlocksDirtyMap::MakeFlushHint(size_t batchSize)
{
    TFlushHints result;

    if (ReadyToFlush.size() < batchSize) {
        return result;
    }

    THashSet<ui64> readyToFlush;
    readyToFlush.swap(ReadyToFlush);

    auto countReadyToFlush = [&](TBlockRange64 range)
    {
        size_t result = 0;
        for (ELocation destination: DesiredDDisks) {
            result += DDiskStates[destination].NeedFlushToDDisk(range) ? 1 : 0;
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

        for (ELocation destination: DesiredDDisks) {
            if (!DDiskStates[destination].NeedFlushToDDisk(item->Range)) {
                continue;
            }

            const ELocation source = val.RequestFlush(destination);
            if (source != ELocation::Unknown) {
                result.AddHint(source, destination, item->Key, item->Range);
            }
        }
    }

    return result;
}

TEraseHints TBlocksDirtyMap::MakeEraseHint(size_t batchSize)
{
    TEraseHints result;

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
            if (!DisabledLocations.Get(l) && val.RequestErase(l)) {
                result.AddHint(l, item->Key, item->Range);
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
    Y_ABORT_UNLESS(requested.OnlyPBuffer());
    Y_ABORT_UNLESS(confirmed.OnlyPBuffer());

    const bool inserted = Inflight.AddRange(
        lsn,
        range,
        TInflightInfo(this, lsn, requested, confirmed));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::FlushFinished(
    TRoute route,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    Y_ABORT_UNLESS(IsPBuffer(route.Source));
    Y_ABORT_UNLESS(IsDDisk(route.Destination));

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
    ELocation location,
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    Y_ABORT_UNLESS(IsPBuffer(location));

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

void TBlocksDirtyMap::MarkFresh(ELocation location, ui64 bytesOffset)
{
    Y_ABORT_UNLESS(IsDDisk(location));

    DDiskStates[location].SetReadWatermark(bytesOffset / BlockSize);
    DDiskStates[location].SetFlushWatermark(bytesOffset / BlockSize);
}

std::optional<ui64> TBlocksDirtyMap::GetFreshWatermark(ELocation location) const
{
    Y_ABORT_UNLESS(IsDDisk(location));

    if (DDiskStates[location].GetState() == TDDiskState::EState::Operational) {
        return std::nullopt;
    }
    return DDiskStates[location].GetOperationalBlockCount() * BlockSize;
}

void TBlocksDirtyMap::SetReadWatermark(ELocation location, ui64 bytesOffset)
{
    Y_ABORT_UNLESS(IsDDisk(location));

    DDiskStates[location].SetReadWatermark(bytesOffset / BlockSize);
}

void TBlocksDirtyMap::SetFlushWatermark(ELocation location, ui64 bytesOffset)
{
    Y_ABORT_UNLESS(IsDDisk(location));

    DDiskStates[location].SetFlushWatermark(bytesOffset / BlockSize);
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
    TBlockRange64 range,
    TLocationMask mask)
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

TString TBlocksDirtyMap::DebugPrintDDiskState() const
{
    TStringBuilder result;
    for (auto l: DDiskLocations) {
        result << ToString(l) << DDiskStates[l].DebugPrint() << ";";
    }
    return result;
}

TLocationMask TBlocksDirtyMap::FilterLocations(
    TLocationMask mask,
    TBlockRange64 range) const
{
    TLocationMask result = mask.Exclude(DisabledLocations);
    if (!result.HasDDisk()) {
        return result;
    }

    for (ELocation l: result) {
        const auto& state = DDiskStates[l];

        if (!state.CanReadFromDDisk(range)) {
            result.Reset(l);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
