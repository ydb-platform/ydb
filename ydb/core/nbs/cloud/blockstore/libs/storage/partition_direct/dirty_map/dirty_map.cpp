#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_algorithms.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

void AddReadHint(TReadHint& result, TReadRangeHint&& hint)
{
    if (result.RangeHints.empty()) {
        result.RangeHints.push_back(std::move(hint));
        return;
    }

    auto& prevHint = result.RangeHints.back();
    if (prevHint.Lsn == hint.Lsn &&
        prevHint.LocationMask == hint.LocationMask &&
        prevHint.VChunkRange.End + 1 == hint.VChunkRange.Start)
    {
        prevHint.VChunkRange.End = hint.VChunkRange.End;
        prevHint.RequestRelativeRange.End = hint.RequestRelativeRange.End;
    } else {
        result.RangeHints.push_back(std::move(hint));
    }
}

}   // namespace

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

TBlocksDirtyMap::TBlocksDirtyMap(ui32 blockSize, ui64 blockCount)
    : BlockSize(blockSize)
    , BlockCount(blockCount)
{
    for (auto l: DDiskLocations) {
        DDiskStates[l].Init(BlockCount, BlockCount);
    }
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
        Inflight.AddRange(
            lsn,
            range,
            TInflightInfo(this, lsn, range.Size() * BlockSize, location));
    }
}

// Create multiple readRangeHints for specified range with possible overlapping
// with inflight requests
TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    TReadHint result;
    if (!Inflight.HasOverlaps(range)) {   // read from ddisk
        result.RangeHints.push_back(MakeReadRangeHint({}, 0, range, 0));
        return result;
    }

    TVector<TWeightedRange> ranges;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            ranges.push_back({.Key = item.Key, .Range = item.Range});
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    result.RangeHints.reserve(ranges.size());
    auto nonOverlappingRanges = SplitOnNonOverlappingContinuousRanges(
        TBlockRange64::MakeClosedInterval(range.Start, range.End),
        ranges);

    ui64 offsetBlocks{};
    for (auto& nonOverlappingRange: nonOverlappingRanges) {
        auto lsn = nonOverlappingRange.Key;
        auto& range = nonOverlappingRange.Range;

        if (lsn == 0) {
            auto hint = MakeReadRangeHint({}, 0, range, offsetBlocks);
            AddReadHint(result, std::move(hint));
        } else {
            auto item = Inflight.GetValue(lsn);
            Y_ABORT_UNLESS(item);
            auto& inflightInfo = item->Value;
            const auto readMask = inflightInfo.ReadMask();
            if (readMask.Empty()) {
                result.WaitReady = inflightInfo.GetQuorumReadyFuture();
                result.RangeHints.clear();
                return result;
            }

            const ui64 resultLsn = readMask.OnlyDDisk() ? 0 : lsn;
            auto hint =
                MakeReadRangeHint(readMask, resultLsn, range, offsetBlocks);
            AddReadHint(result, std::move(hint));
        }

        offsetBlocks += range.Size();
    }

    return result;
}

TFlushHints TBlocksDirtyMap::MakeFlushHint(size_t batchSize)
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

    TSet<ui64> readyToErase;
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
    ELocation pbuffer) const
{
    return PBufferCounters[pbuffer];
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

void TBlocksDirtyMap::DataToPBufferAdded(
    ELocation location,
    EPBufferCounter counter,
    size_t byteCount)
{
    auto& counters = PBufferCounters[location];

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
    ELocation location,
    EPBufferCounter counter,
    size_t byteCount)
{
    auto& counters = PBufferCounters[location];

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

TString TBlocksDirtyMap::DebugPrintDDiskState() const
{
    TStringBuilder result;
    for (auto l: DDiskLocations) {
        result << ToString(l) << DDiskStates[l].DebugPrint() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToFlush() const
{
    TStringBuilder result;
    for (auto lsn: ReadyToFlush) {
        result << ToString(lsn) << "; ";
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

TReadRangeHint TBlocksDirtyMap::MakeReadRangeHint(
    TLocationMask locationMask,
    ui64 lsn,
    TBlockRange64 range,
    ui64 offsetBlocks)
{
    if (locationMask.Empty()) {
        locationMask = FilterLocations(DesiredDDisks, range);
    } else if (locationMask.HasDDisk()) {
        locationMask = locationMask.LogicalAnd(DesiredDDisks);
        locationMask = FilterLocations(locationMask, range);
    }

    locationMask = locationMask.Exclude(DisabledLocations);
    Y_ABORT_UNLESS(!locationMask.Empty());

    return {
        locationMask,
        lsn,
        TBlockRange64::WithLength(offsetBlocks, range.Size()),
        range,
        locationMask.OnlyDDisk() ? TRangeLock(this, range, locationMask)
                                 : TRangeLock(this, lsn)};
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
