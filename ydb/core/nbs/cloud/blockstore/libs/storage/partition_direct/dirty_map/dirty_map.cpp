#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_algorithms.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {
template <typename T>
TVector<ui64> DoMakeLsnVector(std::span<const T> segments)
{
    TVector<ui64> result;
    result.reserve(segments.size());
    for (const auto& segment: segments) {
        result.push_back(segment.Lsn);
    }
    return result;
}

}   // namespace

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

// static
TVector<ui64> TPBufferSegment::MakeLsnVector(
    std::span<const TPBufferSegment> segments)
{
    TVector<ui64> result;
    result.reserve(segments.size());
    for (const auto& segment: segments) {
        result.push_back(segment.Lsn);
    }
    return result;
}

TString TPBufferSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(Lsn);
    }
    return TStringBuilder() << Lsn << Range.Print();
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
        builder << route.DebugPrint() << ":" << hint.DebugPrint(false) << ";";
    }
    return builder;
}

////////////////////////////////////////////////////////////////////////////////

TString TEraseSegment::DebugPrint(bool brief) const
{
    if (brief) {
        return ToString(Lsn);
    }
    return TStringBuilder() << Generation << ":" << Lsn;
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

void TEraseHints::AddHint(THostIndex host, ui64 lsn)
{
    Hints[host].Segments.emplace_back(
        0,   // TODO(drbasic)
        lsn);
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
    OperationalBlockCount = operationalBlockCount;
    UpdateState(true);
}

void TDDiskState::SwitchOffline()
{
    State = EState::Disabled;
    OperationalBlockCount = 0;
}

bool TDDiskState::IsLagging() const
{
    return Lagging;
}

void TDDiskState::StartLagging()
{
    Lagging = true;
}

void TDDiskState::StopLagging()
{
    Lagging = false;
}

bool TDDiskState::IsTrackingEnabled() const
{
    return State != EState::Disabled && (Lagging || IsFresh());
}

void TDDiskState::OnRangeFlushed(TBlockRange64 range, EFlushCompletion flush)
{
    if (!IsTrackingEnabled()) {
        return;
    }

    // The replica is lagging and data has not been written. Adding the range to
    // the behind map. Due to lagging switching races with notifications, it is
    // possible to receive successful flush confirmation on a lagging replica.
    // We will ignore such ranges for safety.
    if (Lagging && flush == EFlushCompletion::Missed) {
        BehindField.Add(range);
    }

    // The replica is not lagging and data has been written. Adding the range to
    // the ahead map.
    if (!Lagging && flush == EFlushCompletion::Completed) {
        AddAhead(range);
    }

    UpdateState(false);
}

TDDiskState::EState TDDiskState::GetState() const
{
    return State;
}

bool TDDiskState::CanReadFromDDisk(TBlockRange64 range) const
{
    if (State == EState::Disabled) {
        return false;
    }
    if (State == EState::Operational) {
        return true;
    }

    // if (AheadField.Contains(range))
    //    return true;
    if (BehindField.Overlaps(range)) {
        return false;
    }

    return range.End < OperationalBlockCount;
}

std::optional<TBlockRange64> TDDiskState::GetFreshRange() const
{
    std::optional<TBlockRange64> result;

    if (GetState() == TDDiskState::EState::Operational ||
        GetState() == TDDiskState::EState::Disabled)
    {
        return result;
    }

    if (!BehindField.Empty()) {
        BehindField.Enumerate(
            [&](TBlockRange64 range)
            {
                result = range;
                return TBlockRangeField::EEnumerateContinuation::Stop;
            });
        return result;
    }

    result = TBlockRange64::WithLength(
        OperationalBlockCount,
        TotalBlockCount - OperationalBlockCount);

    return result;
}

void TDDiskState::RangeSynced(TBlockRange64 range)
{
    BehindField.Remove(range);
    AheadField.Remove(range);

    const ui64 newWatermark = range.End + 1;
    if (OperationalBlockCount < newWatermark &&
        !BehindField.Overlaps(TBlockRange64::WithLength(0, newWatermark)))
    {
        OperationalBlockCount = newWatermark;
    }
    UpdateState(false);
}

void TDDiskState::UpdateWatermarkDebugOnly(ui64 blockCount)
{
    Y_ABORT_UNLESS(blockCount <= TotalBlockCount);

    OperationalBlockCount = blockCount;
    UpdateState(false);
}

TString TDDiskState::DebugPrint() const
{
    TStringBuilder result;
    result << "{" << ToString(State);
    if (State == EState::Fresh) {
        result << (Lagging ? "-" : "+");
    }
    result << "," << OperationalBlockCount << "}";
    return result;
}

TString TDDiskState::DebugPrintAhead() const
{
    return AheadField.Print();
}

TString TDDiskState::DebugPrintBehind() const
{
    return BehindField.Print();
}

bool TDDiskState::IsFresh() const
{
    return OperationalBlockCount != TotalBlockCount || !BehindField.Empty();
}

void TDDiskState::UpdateState(bool force)
{
    if (!force && State == EState::Disabled) {
        return;
    }

    State = IsFresh() ? EState::Fresh : EState::Operational;
}

void TDDiskState::AddAhead(TBlockRange64 range)
{
    Y_ABORT_UNLESS(!Lagging);

    BehindField.Remove(range);
    AheadField.Add(range);
    if (OperationalBlockCount) {
        AheadField.Remove(TBlockRange64::WithLength(0, OperationalBlockCount));
    }
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

    for (THostIndex h = 0; h < GetHostCount(); ++h) {
        const bool isDDiskDisabled =
            DisabledHosts.Get(h) && DesiredDDisks.Get(h);
        if (removed.Get(h)) {
            DDiskStates[h].SwitchOffline();
        } else if (isDDiskDisabled) {
            DDiskStates[h].StartLagging();
        } else {
            DDiskStates[h].StopLagging();
        }
    }

    TVector<ui64> erased;
    Inflight.Enumerate(
        [&](TInflightMap::TFindItem& item)
        {
            TInflightInfo& inflightItem = item.Value;
            inflightItem.UpdateHosts(added, removed, DisabledHosts);
            if (inflightItem.GetState() == TInflightInfo::EState::PBufferErased)
            {
                erased.push_back(item.Key);
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });

    for (auto lsn: erased) {
        Inflight.RemoveRange(lsn);
        ReadyToErase.erase(lsn);
        ReadyToFlush.erase(lsn);
    }
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
            TInflightInfo(
                this,
                DesiredDDisks,
                DisabledHosts,
                lsn,
                range.Size() * BlockSize,
                host));
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

    auto nonOverlappingRanges =
        SplitOnNonOverlappingContinuousRanges(range, ranges);
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

    if (DesiredDDisks.Exclude(DisabledHosts).Count() <
        QuorumDirectBlockGroupHostCount)
    {
        // We can't make a flush while DDisk quorum is unavailable. Will wait
        // until it becomes available.
        return result;
    }

    TSet<ui64> readyToFlush;
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

        if (InflightDDiskSyncMap.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during sync of overlapped range.
            ReadyToFlush.insert(lsn);
            continue;
        }

        for (THostIndex destination: DesiredDDisks.Exclude(DisabledHosts)) {
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
            result.AddHint(host, item.Lsn);
        }
    }

    return result;
}

void TBlocksDirtyMap::RegisterInflightWrite(ui64 lsn, TBlockRange64 range)
{
    const bool inserted = Inflight.AddRange(
        lsn,
        range,
        TInflightInfo(
            this,
            DesiredDDisks,
            DisabledHosts,
            lsn,
            range.Size() * BlockSize));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::WriteFinished(
    ui64 lsn,
    TBlockRange64 range,
    THostMask requested,
    THostMask confirmed)
{
    // Every write is pre-registered as pending at generation time (see
    // RegisterInflightWrite), so the entry always exists here.
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item);
    Y_ABORT_UNLESS(item->Range == range);

    auto& inflightItem = item->Value;

    if (confirmed.Count() < QuorumDirectBlockGroupHostCount) {
        // The write request did not reach the quorum. We responded to the
        // client with an error. The written PBuffers will be cleared through a
        // barrier garbage collection later. For now, we will forget about this
        // request as if it never existed.
        const bool removed = Inflight.RemoveRange(lsn);
        Y_ABORT_UNLESS(removed);
        return;
    }

    inflightItem.OnWritten(requested, confirmed);
    /*
    const auto demotedHosts = DisabledHosts.Exclude(DesiredDDisks);
    if (!demotedHosts.Empty()) {
        inflightItem.UpdateHosts(
            THostMask::MakeEmpty(),
            demotedHosts,
            DisabledHosts);
    }
            */
}

void TBlocksDirtyMap::FlushFinished(
    THostRoute route,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    if (DisabledHosts.Get(route.DestinationHostIndex)) {
        // No processing is required, all inflight operations have been updated
        // when transition to disabled state occurs.
        return;
    }

    for (ui64 lsn: flushOk) {
        auto item = Inflight.GetValue(lsn);
        if (!item) {
            // The item was deleted when the host was disabled.
            continue;
        }
        auto& inflight = item->Value;

        inflight.ConfirmFlush(route.DestinationHostIndex);
        InflightFlushFinished(item->Range);
    }

    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
        if (!item) {
            // The item was deleted when the host was disabled.
            continue;
        }
        auto& inflight = item->Value;

        inflight.FlushFailed(route.DestinationHostIndex);
        InflightFlushFinished(item->Range);
    }
}

void TBlocksDirtyMap::EraseFinished(
    THostIndex host,
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    for (ui64 lsn: eraseOk) {
        auto item = Inflight.GetValue(lsn);
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

    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
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
    ui64 lsn)
{
    const auto item = Inflight.GetValue(lsn);
    const bool unknownLsn = item == std::nullopt;
    const bool erasingInProgress =
        item &&
        (item->Value.GetState() == TInflightInfo::EState::PBufferErasing ||
         item->Value.GetState() == TInflightInfo::EState::PBufferErased);

    if (unknownLsn || erasingInProgress) {
        ReadyToEraseBelated.emplace(
            TInfoEraseBelated{.Lsn = lsn, .Hosts = completedWrites});
    }
}

void TBlocksDirtyMap::UpdateWatermarkDebugOnly(
    THostIndex host,
    ui64 bytesOffset)
{
    DDiskStates[host].UpdateWatermarkDebugOnly(bytesOffset / BlockSize);
}

std::optional<TBlockRange64> TBlocksDirtyMap::GetFreshRange(
    THostIndex host) const
{
    return DDiskStates[host].GetFreshRange();
}

NThreading::TFuture<void> TBlocksDirtyMap::GetRangeSyncStartTrigger(
    THostIndex host,
    TBlockRange64 range)
{
    TInflightDDiskSync sync{
        .DestinationHost = host,
        .SyncStartTrigger = NThreading::NewPromise<void>()};

    if (!HasInflightFlush(host, range)) {
        sync.SyncStartTrigger.SetValue();
    }

    auto result = sync.SyncStartTrigger.GetFuture();
    InflightDDiskSyncMap.AddRange(
        ++InflightDDiskSyncIdGenerator,
        range,
        std::move(sync));

    return result;
}

void TBlocksDirtyMap::RangeSynced(THostIndex host, TBlockRange64 range)
{
    DDiskStates[host].RangeSynced(range);

    ui64 syncId = 0;
    InflightDDiskSyncMap.EnumerateOverlapping(
        range,
        [&](TInflightDDiskSyncMap::TFindItem& item)
        {
            if (item.Value.DestinationHost == host && item.Range == range) {
                syncId = item.Key;
                return TInflightDDiskSyncMap::EEnumerateContinuation::Stop;
            }

            return TInflightDDiskSyncMap::EEnumerateContinuation::Continue;
        });
    Y_ABORT_UNLESS(syncId != 0);
    InflightDDiskSyncMap.RemoveRange(syncId);
}

void TBlocksDirtyMap::ClearRangeSyncs(THostIndex host)
{
    TVector<ui64> syncIds;
    InflightDDiskSyncMap.Enumerate(
        [&](TInflightDDiskSyncMap::TFindItem& item)
        {
            if (item.Value.DestinationHost == host) {
                item.Value.SyncStartTrigger.TrySetValue();
                syncIds.push_back(item.Key);
            }

            return TInflightDDiskSyncMap::EEnumerateContinuation::Continue;
        });
    for (ui64 syncId: syncIds) {
        InflightDDiskSyncMap.RemoveRange(syncId);
    }
}

size_t TBlocksDirtyMap::GetHostCount() const
{
    return DDiskStates.size();
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

std::optional<ui64> TBlocksDirtyMap::GetSafeBarrierForErase() const
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
                    item.Value.GetInflightFlushes().LogicalAnd(mask).Empty());
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

void TBlocksDirtyMap::UnRegister(ui64 lsn, EQueueType queueType)
{
    switch (queueType) {
        case IReadyQueue::EQueueType::Clone: {
            ReadyToClone.erase(lsn);
            break;
        }
        case IReadyQueue::EQueueType::Flush: {
            ReadyToFlush.erase(lsn);
            break;
        }
        case IReadyQueue::EQueueType::Erase: {
            ReadyToErase.erase(lsn);
            break;
        }
    }
}

void TBlocksDirtyMap::FlushCompleted(ui64 lsn, THostMask ddisks)
{
    AddToAheadAndBehind(lsn, ddisks);
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

TString TBlocksDirtyMap::DebugPrintAhead() const
{
    TStringBuilder result;
    for (THostIndex h = 0; h < GetHostCount(); ++h) {
        auto ahead = DDiskStates[h].DebugPrintAhead();
        if (ahead.empty()) {
            continue;
        }
        result << "  " << PrintHostIndex(h) << ": " << ahead << "\n";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintBehind() const
{
    TStringBuilder result;
    for (THostIndex h = 0; h < GetHostCount(); ++h) {
        auto behind = DDiskStates[h].DebugPrintBehind();
        if (behind.empty()) {
            continue;
        }
        result << "  " << PrintHostIndex(h) << ": " << behind << "\n";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintInflightSync()
{
    TStringBuilder result;
    InflightDDiskSyncMap.Enumerate(
        [&](TInflightDDiskSyncMap::TFindItem& item)
        {
            result << PrintHostIndex(item.Value.DestinationHost) << item.Range
                   << (item.Value.SyncStartTrigger.IsReady() ? "ready" : "wait")
                   << ";";

            return TInflightDDiskSyncMap::EEnumerateContinuation::Continue;
        });
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
    if (mask.Empty()) {
        mask = mask.Include(DesiredDDisks);
        // If we don't have enabled hosts, we can return error or fail on
        // assert. Or we can try to use disabled hosts because it could return
        // to life. We choose 2 option and try to read from desired hosts.
    }
    Y_ABORT_UNLESS(!mask.Empty(), "MakeReadRangeHint empty mask");

    return TReadRangeHint(
        mask,
        lsn,
        TBlockRange64::WithLength(offsetBlocks, range.Size()),
        range,
        lsn == 0 ? TRangeLock(weak_from_this(), range, mask)
                 : TRangeLock(weak_from_this(), lsn));
}

void TBlocksDirtyMap::AddToAheadAndBehind(ui64 lsn, THostMask ddisks)
{
    // Check that one of the ddisks is lagging or aheading, in this case it
    // needs to be notified about the data flush to ddisk.
    bool needNotify = AnyOf(
        DDiskStates,
        [](const TDDiskState& ddisk) { return ddisk.IsTrackingEnabled(); });

    if (!needNotify) {
        return;
    }

    auto inflight = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(inflight);
    const auto state = inflight->Value.GetState();
    Y_ABORT_UNLESS(
        state == TInflightInfo::EState::PBufferFlushed ||
        state == TInflightInfo::EState::PBufferErasing);

    for (THostIndex host = 0; host < GetHostCount(); ++host) {
        DDiskStates[host].OnRangeFlushed(
            inflight->Range,
            ddisks.Get(host) ? TDDiskState::EFlushCompletion::Completed
                             : TDDiskState::EFlushCompletion::Missed);
    }
}

bool TBlocksDirtyMap::HasInflightFlush(THostIndex host, TBlockRange64 range)
{
    bool hasOverlaps = false;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& item)
        {
            const auto state = item.Value.GetState();

            if (state == TInflightInfo::EState::PBufferFlushing &&
                item.Value.GetInflightFlushes().Get(host))
            {
                hasOverlaps = true;
                return TInflightMap::EEnumerateContinuation::Stop;
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });
    return hasOverlaps;
}

void TBlocksDirtyMap::InflightFlushFinished(TBlockRange64 range)
{
    InflightDDiskSyncMap.EnumerateOverlapping(
        range,
        [&](TInflightDDiskSyncMap::TFindItem& item)
        {
            auto& sync = item.Value;
            if (!HasInflightFlush(sync.DestinationHost, item.Range)) {
                sync.SyncStartTrigger.TrySetValue();
            }

            return TInflightDDiskSyncMap::EEnumerateContinuation::Continue;
        });
}

////////////////////////////////////////////////////////////////////////////////

bool TBlocksDirtyMap::TInfoEraseBelated::operator<(
    const TInfoEraseBelated& other) const
{
    auto makeTuple = [](const TInfoEraseBelated& info)
    {
        return std::tie(info.Lsn, info.Hosts);
    };

    return makeTuple(*this) < makeTuple(other);
}

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> MakeLsnVector(std::span<const TPBufferSegment> segments)
{
    return DoMakeLsnVector<TPBufferSegment>(segments);
}

TVector<ui64> MakeLsnVector(std::span<const TEraseSegment> segments)
{
    return DoMakeLsnVector<TEraseSegment>(segments);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
