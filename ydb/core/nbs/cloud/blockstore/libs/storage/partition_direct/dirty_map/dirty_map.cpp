#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_algorithms.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_status.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    THostMask hostMask,
    ui64 lsn,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : HostMask(hostMask)
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
    for (const auto& [route, hint]: Hints) {
        builder << "H" << ui32(route.SourceHostIndex) << "->H"
                << ui32(route.DestinationHostIndex) << ":" << hint.DebugPrint()
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
    for (const auto& [host, hint]: Hints) {
        builder << "H" << ui32(host) << ":" << hint.DebugPrint() << ";";
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

TString TPBufferCounters::DebugPrint() const
{
    TStringBuilder result;

    result << "{CurrentRecordsCount:" << CurrentRecordsCount << ", "
           << "CurrentBytesCount" << CurrentBytesCount << ", "
           << "TotalRecordsCount:" << TotalRecordsCount << ", "
           << "TotalBytesCount:" << TotalBytesCount << ", "
           << "CurrentLockedRecordsCount:" << CurrentLockedRecordsCount << ", "
           << "CurrentLockedBytesCount:" << CurrentLockedBytesCount << ", "
           << "TotalLockedRecordsCount:" << TotalLockedRecordsCount << ", "
           << "TotalLockedBytesCount:" << TotalLockedBytesCount << "}";

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TBlocksDirtyMap::TBlocksDirtyMap(
    const TVChunkConfig& vChunkConfig,
    ui32 blockSize,
    ui64 blockCount)
    : BlockSize(blockSize)
    , BlockCount(blockCount)
    , DesiredPBuffers(vChunkConfig.PBufferHosts.GetPrimary())
    , DesiredDDisks(vChunkConfig.DDiskHosts.GetPrimary())
{
    const size_t pbufferHostCount = vChunkConfig.PBufferHosts.HostCount();
    const size_t ddiskHostCount = vChunkConfig.DDiskHosts.HostCount();
    Y_ABORT_UNLESS(pbufferHostCount > 0);
    Y_ABORT_UNLESS(pbufferHostCount <= MaxHostCount);
    Y_ABORT_UNLESS(ddiskHostCount > 0);
    Y_ABORT_UNLESS(ddiskHostCount <= MaxHostCount);
    PBufferCounters.resize(pbufferHostCount);

    DDiskStates.resize(ddiskHostCount);
    for (auto& state: DDiskStates) {
        state.Init(BlockCount, BlockCount);
    }
}

void TBlocksDirtyMap::UpdateConfig(
    THostMask desiredPBuffers,
    THostMask desiredDDisks,
    THostMask disabled)
{
    Y_ABORT_UNLESS(disabled.LogicalAnd(desiredPBuffers).Empty());
    Y_ABORT_UNLESS(disabled.LogicalAnd(desiredDDisks).Empty());

    DesiredPBuffers = desiredPBuffers;
    DesiredDDisks = desiredDDisks;
    DisabledHosts = disabled;
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
    Y_ABORT_UNLESS(host < PBufferCounters.size());

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

// Create multiple readRangeHints for specified range with possible overlapping
// with inflight requests
TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    TReadHint result;
    if (!Inflight.HasOverlaps(range)) {   // read from ddisk
        result.RangeHints.push_back(MakeReadRangeHint({}, 0, range, 0));
        return result;
    }

    bool shouldWaitQuorum = false;
    TStackVec<TWeightedRange> ranges;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const auto readMask = item.Value.ReadMask();
            if (readMask.Empty()) {
                shouldWaitQuorum = true;
                result.WaitReady = item.Value.GetQuorumReadyFuture();
                result.RangeHints.clear();
                return TInflightMap::EEnumerateContinuation::Stop;
            }

            if (!readMask.OnlyDDisk()) {
                ranges.push_back({.Key = item.Key, .Range = item.Range});
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });
    if (shouldWaitQuorum) {
        return result;
    }

    auto nonOverlappingRanges = SplitOnNonOverlappingContinuousRanges(
        TBlockRange64::MakeClosedInterval(range.Start, range.End),
        ranges);
    result.RangeHints.reserve(nonOverlappingRanges.size());

    ui64 offsetBlocks{};
    for (auto& nonOverlappingRange: nonOverlappingRanges) {
        auto lsn = nonOverlappingRange.Key;

        if (lsn == 0) {
            auto hint = MakeReadRangeHint(
                {},
                0,
                nonOverlappingRange.Range,
                offsetBlocks);
            result.RangeHints.push_back(std::move(hint));
        } else {
            auto item = Inflight.GetValue(lsn);
            Y_ABORT_UNLESS(item);
            const auto readMask = item->Value.ReadMask();
            Y_DEBUG_ABORT_UNLESS(!readMask.Empty());

            auto hint = MakeReadRangeHint(
                readMask.Mask,
                lsn,
                nonOverlappingRange.Range,
                offsetBlocks);
            result.RangeHints.push_back(std::move(hint));
        }

        offsetBlocks += nonOverlappingRange.Range.Size();
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
        for (THostIndex destination: DesiredDDisks) {
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

        for (THostIndex destination: DesiredDDisks) {
            if (!DDiskStates[destination].NeedFlushToDDisk(item->Range)) {
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

        for (THostIndex host: DesiredPBuffers) {
            if (val.RequestErase(host)) {
                result.AddHint(host, item->Key, item->Range);
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

void TBlocksDirtyMap::MarkFresh(THostIndex host, ui64 bytesOffset)
{
    DDiskStates[host].SetReadWatermark(bytesOffset / BlockSize);
    DDiskStates[host].SetFlushWatermark(bytesOffset / BlockSize);
}

std::optional<ui64> TBlocksDirtyMap::GetFreshWatermark(THostIndex host) const
{
    if (DDiskStates[host].GetState() == TDDiskState::EState::Operational) {
        return std::nullopt;
    }
    return DDiskStates[host].GetOperationalBlockCount() * BlockSize;
}

void TBlocksDirtyMap::SetReadWatermark(THostIndex host, ui64 bytesOffset)
{
    DDiskStates[host].SetReadWatermark(bytesOffset / BlockSize);
}

void TBlocksDirtyMap::SetFlushWatermark(THostIndex host, ui64 bytesOffset)
{
    DDiskStates[host].SetFlushWatermark(bytesOffset / BlockSize);
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

            if (state == TInflightInfo::EState::PBufferFlushing) {
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

TString TBlocksDirtyMap::DebugPrintPBuffers()
{
    TInstant now = TInstant::Now();
    TStringBuilder result;
    Inflight.Enumerate(
        [&](TInflightMap::TFindItem& item)
        {
            result << "  " << item.Key << item.Range.Print()
                   << item.Value.DebugPrint(now) << "\n";
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    return result;
}

TString TBlocksDirtyMap::DebugPrintPBuffersUsage() const
{
    TStringBuilder result;
    for (size_t h = 0; h < PBufferCounters.size(); ++h) {
        result << "  H" << h << PBufferCounters[h].DebugPrint() << ";\n";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintLockedDDiskRanges()
{
    TStringBuilder result;
    InflightDDiskReads.Enumerate(
        [&](TInflightDDiskReadsMap::TFindItem& item)
        {
            result << item.Range.Print() << item.Value.Print() << ";";
            return TInflightDDiskReadsMap::EEnumerateContinuation::Continue;
        });
    return result;
}

TString TBlocksDirtyMap::DebugPrintDDiskState() const
{
    TStringBuilder result;
    for (THostIndex h = 0; h < DDiskStates.size(); ++h) {
        result << "H" << ui32(h) << DDiskStates[h].DebugPrint() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToClone() const
{
    TStringBuilder result;
    for (auto lsn: ReadyToClone) {
        result << ToString(lsn) << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToFlush() const
{
    TStringBuilder result;
    for (auto lsn: ReadyToFlush) {
        result << ToString(lsn) << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToErase() const
{
    TStringBuilder result;
    for (auto lsn: ReadyToErase) {
        result << ToString(lsn) << ";";
    }
    return result;
}

THostMask TBlocksDirtyMap::FilterLocations(
    THostMask mask,
    TBlockRange64 range) const
{
    THostMask result = mask.Exclude(DisabledHosts);
    for (THostIndex h: result) {
        if (!DDiskStates[h].CanReadFromDDisk(range)) {
            result.Reset(h);
        }
    }
    return result;
}

TReadRangeHint TBlocksDirtyMap::MakeReadRangeHint(
    THostMask mask,
    ui64 lsn,
    TBlockRange64 range,
    ui64 offsetBlocks)
{
    if (mask.Empty()) {
        mask = FilterLocations(DesiredDDisks, range);
    } else if (lsn == 0) {
        mask = mask.LogicalAnd(DesiredDDisks);
        mask = FilterLocations(mask, range);
    }
    mask = mask.Exclude(DisabledHosts);
    Y_ABORT_UNLESS(!mask.Empty());

    return TReadRangeHint(
        mask,
        lsn,
        TBlockRange64::WithLength(offsetBlocks, range.Size()),
        range,
        lsn == 0 ? TRangeLock(this, range, mask) : TRangeLock(this, lsn));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
