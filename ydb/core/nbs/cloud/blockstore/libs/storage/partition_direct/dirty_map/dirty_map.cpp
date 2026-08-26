#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_algorithms.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TString TPBufferCounters::DebugPrint() const
{
    TStringBuilder result;

    result << "{Current:" << Current.Print(true) << ", "
           << "Total:" << Total.Print(true) << ", "
           << "CurrentLocked:" << CurrentLocked.Print(true) << ", "
           << "TotalLocked:" << TotalLocked.Print(true) << "}";

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

void TBlocksDirtyMap::Load(const TDirtyMapStateProto& proto)
{
    size_t ddisk = 0;   // TODO (drbasic). Reliable ddisk matching.
    for (const auto& ddiskState: proto.GetDDiskStates()) {
        DDiskStates[ddisk].Load(ddiskState);
        ++ddisk;
    }
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
            this,
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

    TVector<TPBufferKey> erased;
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

    for (auto pBufferKey: erased) {
        Inflight.RemoveRange(pBufferKey);
        ReadyToErase.erase(pBufferKey);
        ReadyToFlush.erase(pBufferKey);
    }
}

void TBlocksDirtyMap::RestorePBuffer(
    TPBufferKey pBufferKey,
    TBlockRange64 range,
    THostIndex host)
{
    Y_ABORT_UNLESS(host < PBufferCounters.size());

    if (auto item = Inflight.GetValue(pBufferKey)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(host);
    } else {
        Inflight.AddRange(
            pBufferKey,
            range,
            TInflightInfo(
                this,
                DesiredDDisks,
                DisabledHosts,
                pBufferKey,
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
        result.RangeHints.push_back(MakeReadRangeHint({}, {}, range, 0));
        return result;
    }

    bool shouldWaitQuorum = false;
    // Greatest TPBufferKey wins an overlap (lexicographic order matches real
    // time). A default key is a hole and is read from DDisk.
    TStackVec<TWeightedRange> ranges;
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
        if (nonOverlappingRange.Key == TPBufferKey{}) {
            auto hint = MakeReadRangeHint(
                {},
                {},
                nonOverlappingRange.Range,
                offsetBlocks);
            result.RangeHints.push_back(std::move(hint));
        } else {
            auto item = Inflight.GetValue(nonOverlappingRange.Key);
            Y_ABORT_UNLESS(item);
            const auto readMask = item->Value.ReadMask();
            Y_DEBUG_ABORT_UNLESS(!readMask.Empty());

            auto hint = MakeReadRangeHint(
                readMask.Mask,
                nonOverlappingRange.Key,
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

    TSet<TPBufferKey> readyToFlush;
    readyToFlush.swap(ReadyToFlush);

    for (TPBufferKey pBufferKey: readyToFlush) {
        auto item = Inflight.GetValue(pBufferKey);
        Y_ABORT_UNLESS(item);
        auto& val = item->Value;

        if (InflightDDiskReads.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during reading from overlapped range.
            ReadyToFlush.insert(pBufferKey);
            continue;
        }

        if (InflightDDiskSyncMap.HasOverlaps(item->Range)) {
            // Can't flush to DDisk during sync of overlapped range.
            ReadyToFlush.insert(pBufferKey);
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

    TSet<TPBufferKey> readyToErase;
    readyToErase.swap(ReadyToErase);

    for (TPBufferKey pBufferKey: readyToErase) {
        auto item = Inflight.GetValue(pBufferKey);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

        if (!CheckEraseAbility(item->Range, val)) {
            ReadyToErase.insert(pBufferKey);
            continue;
        }

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
            result.AddHint(host, item.PBufferKey);
        }
    }

    return result;
}

void TBlocksDirtyMap::RegisterInflightWrite(
    TPBufferKey pBufferKey,
    TBlockRange64 range)
{
    const bool inserted = Inflight.AddRange(
        pBufferKey,
        range,
        TInflightInfo(
            this,
            DesiredDDisks,
            DisabledHosts,
            pBufferKey,
            range.Size() * BlockSize));
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::WriteFinished(
    TPBufferKey pBufferKey,
    TBlockRange64 range,
    THostMask requested,
    THostMask confirmed)
{
    // Every write is pre-registered as pending at generation time (see
    // RegisterInflightWrite), so the entry always exists here.
    auto item = Inflight.GetValue(pBufferKey);
    Y_ABORT_UNLESS(item);
    Y_ABORT_UNLESS(item->Range == range);

    auto& inflightItem = item->Value;

    if (confirmed.Count() < QuorumDirectBlockGroupHostCount) {
        // The write request did not reach the quorum. We responded to the
        // client with an error. The written PBuffers will be cleared through a
        // barrier garbage collection later. For now, we will forget about this
        // request as if it never existed.
        const bool removed = Inflight.RemoveRange(pBufferKey);
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
    const TVector<TPBufferKey>& flushOk,
    const TVector<TPBufferKey>& flushFailed)
{
    if (DisabledHosts.Get(route.DestinationHostIndex)) {
        // No processing is required, all inflight operations have been updated
        // when transition to disabled state occurs.
        return;
    }

    for (TPBufferKey pBufferKey: flushOk) {
        auto item = Inflight.GetValue(pBufferKey);
        if (!item) {
            // The item was deleted when the host was disabled.
            continue;
        }
        auto& inflight = item->Value;

        inflight.ConfirmFlush(route.DestinationHostIndex);
        InflightFlushFinished(item->Range);
    }

    for (TPBufferKey pBufferKey: flushFailed) {
        auto item = Inflight.GetValue(pBufferKey);
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
    const TVector<TPBufferKey>& eraseOk,
    const TVector<TPBufferKey>& eraseFailed)
{
    for (TPBufferKey pBufferKey: eraseOk) {
        auto item = Inflight.GetValue(pBufferKey);
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

    for (TPBufferKey pBufferKey: eraseFailed) {
        auto item = Inflight.GetValue(pBufferKey);
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
    TPBufferKey pBufferKey)
{
    const auto item = Inflight.GetValue(pBufferKey);
    const bool unknownLsn = item == std::nullopt;
    const bool erasingInProgress =
        item &&
        (item->Value.GetState() == TInflightInfo::EState::PBufferErasing ||
         item->Value.GetState() == TInflightInfo::EState::PBufferErased);

    if (unknownLsn || erasingInProgress) {
        ReadyToEraseBelated.emplace(TInfoEraseBelated{
            .PBufferKey = pBufferKey,
            .Hosts = completedWrites});
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

TSyncHint TBlocksDirtyMap::BeginRangeSync(THostIndex host, TBlockRange64 range)
{
    TInflightDDiskSync inflightSync{.DestinationHost = host};

    TSyncHint result{
        .SyncId = ++InflightDDiskSyncIdGenerator,
        .Host = host,
        .Range = range,
        .ReadyToStart = inflightSync.SyncStartTrigger.GetFuture()};

    if (!HasInflightFlush(host, range)) {
        inflightSync.SyncStartTrigger.SetValue();
    }

    InflightDDiskSyncMap.AddRange(
        result.SyncId,
        range,
        std::move(inflightSync));

    return result;
}

void TBlocksDirtyMap::EndRangeSync(ui64 syncId, bool success)
{
    auto inflightSync = InflightDDiskSyncMap.ExtractRange(syncId);
    if (!inflightSync) {
        return;
    }

    if (success) {
        DDiskStates[inflightSync->Value.DestinationHost].RangeSynced(
            inflightSync->Range);
    }
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

std::optional<TPBufferKey> TBlocksDirtyMap::GetSafeBarrierForErase() const
{
    return Inflight.GetMinKey();
}

const TPBufferCounters& TBlocksDirtyMap::GetPBufferCounters(
    THostIndex host) const
{
    Y_ABORT_UNLESS(host < PBufferCounters.size());
    return PBufferCounters[host];
}

TCountAndSize TBlocksDirtyMap::GetPBuffersUsage(THostIndex host) const
{
    if (host >= PBufferCounters.size()) {
        return {};
    }

    return PBufferCounters[host].Current;
}

TCountAndSize TBlocksDirtyMap::GetAheadBlocks(THostIndex host) const
{
    if (host >= DDiskStates.size()) {
        return {};
    }

    TCountAndSize result = DDiskStates[host].GetAheadSegmentsStat();
    result.Size *= BlockSize;
    return result;
}

TCountAndSize TBlocksDirtyMap::GetBehindBlocks(THostIndex host) const
{
    if (host >= DDiskStates.size()) {
        return {};
    }

    TCountAndSize result = DDiskStates[host].GetBehindSegmentsStat();
    result.Size *= BlockSize;
    return result;
}

void TBlocksDirtyMap::LockPBuffer(TPBufferKey pBufferKey)
{
    auto item = Inflight.GetValue(pBufferKey);
    Y_ABORT_UNLESS(item.has_value());
    item->Value.LockPBuffer();
}

void TBlocksDirtyMap::UnlockPBuffer(TPBufferKey pBufferKey)
{
    auto item = Inflight.GetValue(pBufferKey);
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

void TBlocksDirtyMap::Register(TPBufferKey pBufferKey, EQueueType queueType)
{
    switch (queueType) {
        case IReadyQueue::EQueueType::Clone: {
            ReadyToClone.insert(pBufferKey);

            ReadyToFlush.erase(pBufferKey);
            ReadyToErase.erase(pBufferKey);
            break;
        }
        case IReadyQueue::EQueueType::Flush: {
            ReadyToFlush.insert(pBufferKey);

            ReadyToClone.erase(pBufferKey);
            ReadyToErase.erase(pBufferKey);
            break;
        }
        case IReadyQueue::EQueueType::Erase: {
            ReadyToErase.insert(pBufferKey);

            ReadyToClone.erase(pBufferKey);
            ReadyToFlush.erase(pBufferKey);
            break;
        }
    }
}

void TBlocksDirtyMap::UnRegister(TPBufferKey pBufferKey, EQueueType queueType)
{
    switch (queueType) {
        case IReadyQueue::EQueueType::Clone: {
            ReadyToClone.erase(pBufferKey);
            break;
        }
        case IReadyQueue::EQueueType::Flush: {
            ReadyToFlush.erase(pBufferKey);
            break;
        }
        case IReadyQueue::EQueueType::Erase: {
            ReadyToErase.erase(pBufferKey);
            break;
        }
    }
}

void TBlocksDirtyMap::FlushCompleted(TPBufferKey pBufferKey, THostMask ddisks)
{
    AddToAheadAndBehindOnFlushCompleted(pBufferKey, ddisks);
}

void TBlocksDirtyMap::DataToPBufferAdded(
    THostIndex host,
    EPBufferCounter counter,
    size_t byteCount)
{
    auto& counters = PBufferCounters[host];

    switch (counter) {
        case IReadyQueue::EPBufferCounter::Total: {
            counters.Current.Add(byteCount);
            counters.Total.Add(byteCount);
            break;
        }
        case IReadyQueue::EPBufferCounter::Locked: {
            counters.CurrentLocked.Add(byteCount);
            counters.TotalLocked.Add(byteCount);
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
            counters.Current.Sub(byteCount);
            break;
        }
        case IReadyQueue::EPBufferCounter::Locked: {
            counters.CurrentLocked.Sub(byteCount);
            break;
        }
    }
}

void TBlocksDirtyMap::OnBehindAheadChanged()
{
    ++BehindAheadGeneration;
}

bool TBlocksDirtyMap::NeedFlush() const
{
    return !ReadyToFlush.empty();
}

bool TBlocksDirtyMap::NeedErase() const
{
    return !ReadyToErase.empty() || !ReadyToEraseBelated.empty();
}

bool TBlocksDirtyMap::NeedPersist() const
{
    return BehindAheadGeneration > PersistedGeneration;
}

TDirtyMapStateProto TBlocksDirtyMap::GetStateForPersist() const
{
    TDirtyMapStateProto result;
    result.SetStateGeneration(GetCurrentGeneration());
    for (const auto& ddiskState: DDiskStates) {
        ddiskState.Save(result.AddDDiskStates());
    }
    return result;
}

void TBlocksDirtyMap::StatePersisted(ui32 persistGeneration)
{
    Y_ABORT_UNLESS(persistGeneration > PersistedGeneration);
    Y_ABORT_UNLESS(persistGeneration <= BehindAheadGeneration);

    PersistedGeneration = persistGeneration;
}

ui32 TBlocksDirtyMap::GetCurrentGeneration() const
{
    return BehindAheadGeneration;
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
    for (auto pBufferKey: ReadyToClone) {
        result << pBufferKey.Print() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToFlush() const
{
    TStringBuilder result;
    for (auto pBufferKey: ReadyToFlush) {
        result << pBufferKey.Print() << ";";
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintReadyToErase() const
{
    TStringBuilder result;
    for (auto pBufferKey: ReadyToErase) {
        result << pBufferKey.Print() << ";";
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

TString TBlocksDirtyMap::DebugPrintAheadBehindBrief() const
{
    TStringBuilder result;
    result << "gen:" << GetCurrentGeneration() << "/" << PersistedGeneration
           << " ";
    for (THostIndex h = 0; h < GetHostCount(); ++h) {
        auto brief = DDiskStates[h].DebugPrintAheadBehindBrief();
        if (brief) {
            result << PrintHostIndex(h) << ":" << brief;
        }
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
    TPBufferKey pBufferKey,
    TBlockRange64 range,
    ui64 offsetBlocks)
{
    if (mask.Empty()) {
        mask = FilterLocations(DesiredDDisks, range);
    } else if (pBufferKey.Lsn == 0) {
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
        pBufferKey,
        TBlockRange64::WithLength(offsetBlocks, range.Size()),
        range,
        pBufferKey.Lsn == 0 ? TRangeLock(weak_from_this(), range, mask)
                            : TRangeLock(weak_from_this(), pBufferKey));
}

void TBlocksDirtyMap::AddToAheadAndBehindOnFlushCompleted(
    TPBufferKey pBufferKey,
    THostMask ddisks)
{
    // Check that one of the ddisks is lagging or aheading, in this case it
    // needs to be notified about the data flush to ddisk.
    bool needNotify = AnyOf(
        DDiskStates,
        [](const TDDiskState& ddisk) { return ddisk.IsTrackingEnabled(); });

    if (!needNotify) {
        return;
    }

    auto inflight = Inflight.GetValue(pBufferKey);
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

bool TBlocksDirtyMap::CheckEraseAbility(
    TBlockRange64 range,
    TInflightInfo& inflightInfo)
{
    if (BehindAheadGeneration == 0) {
        // There is not a single red block.
        return true;
    }

    if (inflightInfo.GetPersistGeneration() &&
        inflightInfo.GetPersistGeneration() <= PersistedGeneration)
    {
        // Red blocks already persisted. Can erase.
        return true;
    }

    const bool eraseBlocked = AnyOf(
        DDiskStates,
        [&](const TDDiskState& ddiskState)
        {
            return ddiskState.IsTrackingEnabled() &&
                   ddiskState.HasBehindOverlapping(range);
        });

    if (!eraseBlocked) {
        // Don't overlaps with red blocks. Can erase.
        return true;
    }

    if (!inflightInfo.GetPersistGeneration()) {
        // The red blocks from this inflightInfo are already in the current
        // generation. Start waiting for data with the current or newer
        // generation to be persisted.
        inflightInfo.SetPersistGeneration(BehindAheadGeneration);
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

bool TBlocksDirtyMap::TInfoEraseBelated::operator<(
    const TInfoEraseBelated& other) const
{
    auto makeTuple = [](const TInfoEraseBelated& info)
    {
        return std::tie(info.PBufferKey, info.Hosts);
    };

    return makeTuple(*this) < makeTuple(other);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
