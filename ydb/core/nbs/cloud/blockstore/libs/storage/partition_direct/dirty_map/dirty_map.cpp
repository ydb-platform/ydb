#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_algorithms.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {
template <typename T>
TVector<TRecordId> DoMakeRecordIds(std::span<const T> segments)
{
    TVector<TRecordId> result;
    result.reserve(segments.size());
    for (const auto& segment: segments) {
        result.push_back(segment.RecordId);
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadRangeHint::TReadRangeHint(
    THostMask hostMask,
    TRecordId recordId,
    TBlockRange64 requestRelativeRange,
    TBlockRange64 vchunkRange,
    TRangeLock&& lock)
    : HostMask(hostMask)
    , RecordId(recordId)
    , RequestRelativeRange(requestRelativeRange)
    , VChunkRange(vchunkRange)
    , Lock(std::move(lock))
{}

TReadRangeHint::TReadRangeHint(TReadRangeHint&& other) noexcept = default;
TReadRangeHint& TReadRangeHint::operator=(
    TReadRangeHint&& other) noexcept = default;

TString TReadRangeHint::DebugPrint() const
{
    TStringBuilder result;
    if (RecordId.Lsn == 0) {
        result << "0";
    } else {
        result << RecordId.Print();
    }
    result << "{" << HostMask.Print() << VChunkRange.Print()
           << RequestRelativeRange.Print() << "};";
    return result;
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

// static
TVector<TRecordId> TPBufferSegment::MakeRecordIds(
    std::span<const TPBufferSegment> segments)
{
    return DoMakeRecordIds(segments);
}

TString TPBufferSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(RecordId.Lsn);
    }
    return TStringBuilder() << RecordId.Print() << Range.Print();
}

////////////////////////////////////////////////////////////////////////////////

TString TFlushHint::DebugPrint(bool brief) const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint(brief);
        first = false;
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

void TFlushHints::AddHint(
    THostIndex source,
    THostIndex destination,
    TRecordId recordId,
    TBlockRange64 range)
{
    Hints[THostRoute{
              .SourceHostIndex = source,
              .DestinationHostIndex = destination}]
        .Segments.emplace_back(recordId, range);
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
        builder << route.DebugPrint() << ":" << hint.DebugPrint(false) << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString TEraseSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(RecordId.Lsn);
    }
    return RecordId.Print();
}

TString TEraseHint::DebugPrint(bool brief) const
{
    TStringBuilder builder;
    bool first = true;
    for (const auto& segment: Segments) {
        if (!first) {
            builder << ",";
        }
        builder << segment.DebugPrint(brief);
        first = false;
    }
    return builder;
}

void TEraseHints::AddHint(THostIndex host, TRecordId recordId)
{
    Hints[host].Segments.emplace_back(recordId);
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
        builder << PrintHostIndex(host) << ":" << hint.DebugPrint(false) << ";";
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

void TDDiskState::SwitchOffline()
{
    State = EState::Disabled;
    FlushableBlockCount = 0;
    OperationalBlockCount = 0;
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
           << "CurrentBytesCount:" << CurrentBytesCount << ", "
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
    , DDiskStates(vChunkConfig.GetHostCount())
    , PBufferCounters(vChunkConfig.GetHostCount())
{
    UpdateConfig(vChunkConfig);
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

void TBlocksDirtyMap::UpdateConfig(const TVChunkConfig& vChunkConfig)
{
    ResizeHosts(vChunkConfig.GetHostCount());

    const THostMask added = vChunkConfig.GetDDisks().Exclude(DesiredDDisks);
    const THostMask removed = DesiredDDisks.Exclude(vChunkConfig.GetDDisks());

    DesiredDDisks = vChunkConfig.GetDDisks();
    DisabledHosts = vChunkConfig.GetDisabledHosts();

    // When a new disk appears, it doesn't have all the data. Need to set its
    // watermark level.
    for (auto indx: added) {
        const auto watermark = vChunkConfig.GetWatermark(indx);
        DDiskStates[indx].Init(
            BlockCount,
            watermark ? *watermark / BlockSize : BlockCount);
    }

    if (removed.Empty()) {
        return;
    }

    for (auto indx: removed) {
        DDiskStates[indx].SwitchOffline();
    }

    TVector<TRecordId> erased;
    Inflight.Enumerate(
        [&](TInflightMap::TFindItem& item)
        {
            TInflightInfo& inflightItem = item.Value;
            inflightItem.RemoveHosts(removed);
            if (inflightItem.GetState() == TInflightInfo::EState::PBufferErased)
            {
                erased.push_back(item.Key);
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    for (auto recordId: erased) {
        Inflight.RemoveRange(recordId);
        ReadyToErase.erase(recordId);
        ReadyToFlush.erase(recordId);
    }
}

void TBlocksDirtyMap::RestorePBuffer(
    TRecordId recordId,
    TBlockRange64 range,
    THostIndex host)
{
    Y_ABORT_UNLESS(host < PBufferCounters.size());

    if (auto item = Inflight.GetValue(recordId)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(host);
    } else {
        Inflight.AddRange(
            recordId,
            range,
            TInflightInfo(this, recordId, range.Size() * BlockSize, host));
    }
}

// Create multiple readRangeHints for specified range with possible overlapping
// with inflight requests
TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange64 range)
{
    TReadHint result;
    if (!Inflight.HasOverlaps(range)) {   // read from ddisk
        result.RangeHints.push_back(MakeReadRangeHint({}, {}, range, 0));
        return result;
    }

    struct TOverlappingRecord
    {
        TRecordId RecordId;
        TBlockRange64 Range;
        TReadSource ReadSource;
    };

    bool shouldWaitQuorum = false;
    // The split helper weighs ranges with plain ui64 keys where the bigger key
    // wins the overlap and 0 marks a hole. Collect the readable overlapping
    // records, order them by record id (the lexicographic order matches real
    // time across generations), and use dense 1-based positions as weights.
    TStackVec<TOverlappingRecord> overlapping;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const auto readSource = item.Value.ReadMask();
            if (readSource.Empty()) {
                shouldWaitQuorum = true;
                result.WaitReady = item.Value.GetQuorumReadyFuture();
                result.RangeHints.clear();
                return TInflightMap::EEnumerateContinuation::Stop;
            }

            if (!readSource.OnlyDDisk()) {
                overlapping.push_back(
                    {.RecordId = item.Key,
                     .Range = item.Range,
                     .ReadSource = readSource});
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });
    if (shouldWaitQuorum) {
        return result;
    }

    Sort(
        overlapping,
        [](const auto& lhs, const auto& rhs)
        { return lhs.RecordId < rhs.RecordId; });

    TStackVec<TWeightedRange> ranges;
    ranges.reserve(overlapping.size());
    for (size_t i = 0; i < overlapping.size(); ++i) {
        ranges.push_back({.Key = i + 1, .Range = overlapping[i].Range});
    }

    auto nonOverlappingRanges =
        SplitOnNonOverlappingContinuousRanges(range, ranges);
    result.RangeHints.reserve(nonOverlappingRanges.size());

    ui64 offsetBlocks{};
    for (auto& nonOverlappingRange: nonOverlappingRanges) {
        const ui64 weight = nonOverlappingRange.Key;

        if (weight == 0) {
            auto hint = MakeReadRangeHint(
                {},
                {},
                nonOverlappingRange.Range,
                offsetBlocks);
            result.RangeHints.push_back(std::move(hint));
        } else {
            const auto& record = overlapping[weight - 1];
            auto hint = MakeReadRangeHint(
                record.ReadSource.Mask,
                record.RecordId,
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

    if (!DesiredDDisks.LogicalAnd(DisabledHosts).Empty()) {
        // We can't make a flush while DDisk is unavailable. Will wait until it
        // becomes available or is excluded.
        return result;
    }

    TSet<TRecordId> readyToFlush;
    readyToFlush.swap(ReadyToFlush);

    for (TRecordId recordId: readyToFlush) {
        auto item = Inflight.GetValue(recordId);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;

        if (InflightDDiskReads.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during reading from overlapped range.
            ReadyToFlush.insert(recordId);
            continue;
        }

        for (THostIndex destination: DesiredDDisks) {
            if (!DDiskStates[destination].NeedFlushToDDisk(item->Range)) {
                continue;
            }

            const THostIndex source =
                val.RequestFlush(destination, DisabledHosts);
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

    TSet<TRecordId> readyToErase;
    readyToErase.swap(ReadyToErase);

    for (TRecordId recordId: readyToErase) {
        auto item = Inflight.GetValue(recordId);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

        for (THostIndex host: val.GetEraseNeeded()) {
            val.RequestErase(host);

            if (DisabledHosts.Get(host)) {
                // We can't handle this situation properly. Barrier cleanup
                // will help us.
                if (val.ConfirmErase(host)) {
                    const bool removed = Inflight.RemoveRange(item->Key);
                    Y_ABORT_UNLESS(removed);
                    break;
                }
            } else {
                result.AddHint(host, item->Key);
            }
        }
    }

    return result;
}

TEraseHints TBlocksDirtyMap::MakeEraseBelatedHint()
{
    TEraseHints result;

    TSet<TInfoEraseBelated> readyToEraseBelated;
    readyToEraseBelated.swap(ReadyToEraseBelated);
    for (const auto& item: readyToEraseBelated) {
        auto hostMask = item.Hosts;
        for (auto host: hostMask) {
            result.AddHint(host, item.RecordId);
        }
    }

    return result;
}

void TBlocksDirtyMap::RegisterInflightWrite(
    TRecordId recordId,
    TBlockRange64 range)
{
    const bool inserted = Inflight.AddRange(
        recordId,
        range,
        TInflightInfo(this, recordId, range.Size() * BlockSize));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::WriteFinished(
    TRecordId recordId,
    TBlockRange64 range,
    THostMask requested,
    THostMask confirmed)
{
    // Every write is pre-registered as pending at generation time (see
    // RegisterInflightWrite), so the entry always exists here.
    auto item = Inflight.GetValue(recordId);
    Y_ABORT_UNLESS(item);
    Y_ABORT_UNLESS(item->Range == range);

    if (confirmed.Count() < QuorumDirectBlockGroupHostCount) {
        const bool removed = Inflight.RemoveRange(recordId);
        Y_ABORT_UNLESS(removed);
        return;
    }

    item->Value.OnWritten(requested, confirmed);
}

void TBlocksDirtyMap::FlushFinished(
    THostRoute route,
    const TVector<TRecordId>& flushOk,
    const TVector<TRecordId>& flushFailed)
{
    if (DisabledHosts.Get(route.DestinationHostIndex)) {
        // No processing is required, all inflight operations have been updated
        // when transition to disabled state occurs.
        return;
    }

    for (TRecordId recordId: flushOk) {
        auto item = Inflight.GetValue(recordId);
        if (!item) {
            // The item was deleted when the host was disabled.
            continue;
        }
        auto& inflight = item->Value;

        inflight.ConfirmFlush(route.DestinationHostIndex);
    }

    for (TRecordId recordId: flushFailed) {
        auto item = Inflight.GetValue(recordId);
        if (!item) {
            // The item was deleted when the host was disabled.
            continue;
        }
        auto& inflight = item->Value;

        inflight.FlushFailed(route.DestinationHostIndex);
    }
}

void TBlocksDirtyMap::EraseFinished(
    THostIndex host,
    const TVector<TRecordId>& eraseOk,
    const TVector<TRecordId>& eraseFailed)
{
    for (TRecordId recordId: eraseOk) {
        auto item = Inflight.GetValue(recordId);
        if (!item) {
            // The record already left the inflight map: deleted when the host
            // was disabled, or this is a belated ack (for example a duplicate
            // response after a retry). Nothing to do.
            continue;
        }
        auto& inflight = item->Value;

        if (inflight.ConfirmErase(host)) {
            const bool removed = Inflight.RemoveRange(item->Key);
            Y_ABORT_UNLESS(removed);
        }
    }

    for (TRecordId recordId: eraseFailed) {
        auto item = Inflight.GetValue(recordId);
        if (!item) {
            // The record already left the inflight map: deleted when the host
            // was disabled, or this is a belated failure. Nothing to track
            // anymore.
            continue;
        }
        auto& inflight = item->Value;

        inflight.EraseFailed(host);
    }
}

void TBlocksDirtyMap::UpdateBelatedEraseQueue(
    THostMask completedWrites,
    TRecordId recordId)
{
    const auto item = Inflight.GetValue(recordId);
    const bool unknownRecord = item == std::nullopt;
    const bool erasingInProgress =
        item &&
        (item->Value.GetState() == TInflightInfo::EState::PBufferErasing ||
         item->Value.GetState() == TInflightInfo::EState::PBufferErased);

    if (unknownRecord || erasingInProgress) {
        ReadyToEraseBelated.emplace(
            TInfoEraseBelated{.RecordId = recordId, .Hosts = completedWrites});
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

size_t TBlocksDirtyMap::GetEraseBelatedCount() const
{
    return ReadyToEraseBelated.size();
}

ui64 TBlocksDirtyMap::GetMinFlushPendingLsn() const
{
    if (ReadyToFlush.empty()) {
        return 0;
    }
    // TSet is ordered, so the first element is the minimum. O(1) access.
    return ReadyToFlush.begin()->Lsn;
}

ui64 TBlocksDirtyMap::GetMinErasePendingLsn() const
{
    if (ReadyToErase.empty()) {
        return 0;
    }
    // TSet is ordered, so the first element is the minimum. O(1) access.
    return ReadyToErase.begin()->Lsn;
}

std::optional<TRecordId> TBlocksDirtyMap::GetSafeBarrierForErase() const
{
    return Inflight.GetMinKey();
}

const TPBufferCounters& TBlocksDirtyMap::GetPBufferCounters(
    THostIndex host) const
{
    Y_ABORT_UNLESS(host < PBufferCounters.size());
    return PBufferCounters[host];
}

ui64 TBlocksDirtyMap::GetPBufferUsedSize(THostIndex host) const
{
    if (host >= PBufferCounters.size()) {
        return 0;
    }

    return PBufferCounters[host].CurrentBytesCount;
}

void TBlocksDirtyMap::LockPBuffer(TRecordId recordId)
{
    auto item = Inflight.GetValue(recordId);
    Y_ABORT_UNLESS(item.has_value());
    item->Value.LockPBuffer();
}

void TBlocksDirtyMap::UnlockPBuffer(TRecordId recordId)
{
    auto item = Inflight.GetValue(recordId);
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

void TBlocksDirtyMap::Register(TRecordId recordId, EQueueType queueType)
{
    switch (queueType) {
        case IReadyQueue::EQueueType::Clone: {
            ReadyToClone.insert(recordId);

            ReadyToFlush.erase(recordId);
            ReadyToErase.erase(recordId);
            break;
        }
        case IReadyQueue::EQueueType::Flush: {
            ReadyToFlush.insert(recordId);

            ReadyToClone.erase(recordId);
            ReadyToErase.erase(recordId);
            break;
        }
        case IReadyQueue::EQueueType::Erase: {
            ReadyToErase.insert(recordId);

            ReadyToClone.erase(recordId);
            ReadyToFlush.erase(recordId);
            break;
        }
    }
}

void TBlocksDirtyMap::UnRegister(TRecordId recordId)
{
    ReadyToErase.erase(recordId);
    ReadyToClone.erase(recordId);
    ReadyToFlush.erase(recordId);
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

bool TBlocksDirtyMap::NeedFlush() const
{
    return !ReadyToFlush.empty();
}

bool TBlocksDirtyMap::NeedErase() const
{
    return !ReadyToErase.empty() || !ReadyToEraseBelated.empty();
}

TString TBlocksDirtyMap::DebugPrintPBuffers()
{
    TInstant now = TInstant::Now();
    TStringBuilder result;
    Inflight.Enumerate(
        [&](TInflightMap::TFindItem& item)
        {
            result << "  " << item.Key.Print() << item.Range.Print()
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
        result << PrintHostIndex(h);

        const bool disabled = DisabledHosts.Get(h);
        const bool desired = DesiredDDisks.Get(h);
        if (disabled) {
            result << "-";
        } else if (desired) {
            result << "*";
        } else {
            result << "+";
        }

        result << DDiskStates[h].DebugPrint() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToClone() const
{
    TStringBuilder result;
    for (auto recordId: ReadyToClone) {
        result << recordId.Print() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToFlush() const
{
    TStringBuilder result;
    for (auto recordId: ReadyToFlush) {
        result << recordId.Print() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToErase() const
{
    TStringBuilder result;
    for (auto recordId: ReadyToErase) {
        result << recordId.Print() << ";";
    }
    return result;
}

void TBlocksDirtyMap::ResizeHosts(size_t newHostCount)
{
    Y_ABORT_UNLESS(newHostCount <= MaxHostCount);
    Y_ABORT_UNLESS(DDiskStates.size() == PBufferCounters.size());

    if (newHostCount <= PBufferCounters.size()) {
        return;
    }

    PBufferCounters.resize(newHostCount);
    DDiskStates.resize(newHostCount);
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
    TRecordId recordId,
    TBlockRange64 range,
    ui64 offsetBlocks)
{
    if (mask.Empty()) {
        mask = FilterLocations(DesiredDDisks, range);
    } else if (recordId.Lsn == 0) {
        mask = mask.LogicalAnd(DesiredDDisks);
        mask = FilterLocations(mask, range);
    }
    mask = mask.Exclude(DisabledHosts);
    if (mask.Empty()) {
        mask = mask.Include(DesiredDDisks);
        // If we don't have enabled hosts, we can return error or fail on
        // assert. Or we can try to use disabled hosts because it could return
        // to life. We choose 2 option and try to read from desired hosts.
    }
    Y_ABORT_UNLESS(!mask.Empty(), "MakeReadRangeHint empty mask");

    return TReadRangeHint(
        mask,
        recordId,
        TBlockRange64::WithLength(offsetBlocks, range.Size()),
        range,
        recordId.Lsn == 0 ? TRangeLock(this, range, mask)
                          : TRangeLock(this, recordId));
}

bool TBlocksDirtyMap::TInfoEraseBelated::operator<(
    const TInfoEraseBelated& other) const
{
    auto makeTuple = [](const TInfoEraseBelated& info)
    {
        return std::tie(info.RecordId, info.Hosts);
    };

    return makeTuple(*this) < makeTuple(other);
}

////////////////////////////////////////////////////////////////////////////////

TVector<TRecordId> MakeRecordIds(std::span<const TPBufferSegment> segments)
{
    return DoMakeRecordIds(segments);
}

TVector<TRecordId> MakeRecordIds(std::span<const TEraseSegment> segments)
{
    return DoMakeRecordIds(segments);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
