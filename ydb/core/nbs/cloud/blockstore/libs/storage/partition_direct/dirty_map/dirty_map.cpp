#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range/block_range_algorithms.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_roles.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <cstddef>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD
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

=======
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
TBlocksDirtyMap::TBlocksDirtyMap(
    TArenaAllocatorPoolPtr arenaAllocatorPool,
    const TVChunkConfig& vChunkConfig,
    bool isTouched,
    const TDirtyMapStateProto& state,
    ui32 blockSize,
    ui16 blockCount)
    : ArenaAllocatorPool(std::move(arenaAllocatorPool))
    , ArenaAllocator(ArenaAllocatorPool->GetAllocator())
    , BlockSize(blockSize)
    , BlockCount(blockCount)
    , Inflight(ArenaAllocatorPool.get())
    , PBufferCounters(vChunkConfig.GetHostCount())
{
    Y_ABORT_UNLESS(ArenaAllocator);
    DDiskStates.reserve(vChunkConfig.GetHostCount());
    while (DDiskStates.size() < vChunkConfig.GetHostCount()) {
        DDiskStates.emplace_back(ArenaAllocator, blockCount);
    }

    UpdateConfig(vChunkConfig, isTouched);

    size_t ddisk = 0;   // TODO (drbasic). Reliable ddisk matching.
    for (const auto& ddiskState: state.GetDDiskStates()) {
        DDiskStates[ddisk].Load(ddiskState);
        ++ddisk;
    }
}

<<<<<<< HEAD
void TBlocksDirtyMap::UpdateConfig(const TVChunkConfig& vChunkConfig)
=======
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
    const TVChunkConfig& vChunkConfig,
    bool isTouched)
>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
{
    const THostMask added = vChunkConfig.GetDDisks().Exclude(DesiredDDisks);
    const THostMask removed = DesiredDDisks.Exclude(vChunkConfig.GetDDisks());

    DesiredDDisks = vChunkConfig.GetDDisks();
    DisabledHosts = vChunkConfig.GetDisabledHosts();

    // When a new disk appears, it doesn't have all the data. Need to set its
    // watermark level.
    for (auto indx: added) {
        const auto watermark =
            isTouched ? vChunkConfig.GetWatermark(indx) : std::nullopt;
        DDiskStates[indx].Init(
            BlockCount,
            watermark ? IntegerCast<ui16>(*watermark / BlockSize) : BlockCount);
    }

    if (removed.Empty()) {
        return;
    }

    for (auto indx: removed) {
        DDiskStates[indx].SwitchOffline();
    }

    TVector<ui64> erased;
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

    for (auto lsn: erased) {
        Inflight.RemoveRange(lsn);
        ReadyToErase.erase(lsn);
        ReadyToFlush.erase(lsn);
    }
}

void TBlocksDirtyMap::ResizeHosts(size_t newHostCount)
{
    Y_ABORT_UNLESS(newHostCount <= MaxHostCount);
    Y_ABORT_UNLESS(newHostCount >= PBufferCounters.size());
    Y_ABORT_UNLESS(newHostCount >= DDiskStates.size());

    PBufferCounters.resize(newHostCount);
    DDiskStates.resize(newHostCount);
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
<<<<<<< HEAD
    ui64 lsn,
    TBlockRange64 range,
=======
    TPBufferKey pBufferKey,
    TBlockRange16 range,
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    THostIndex host)
{
    Y_ABORT_UNLESS(host < PBufferCounters.size());

    if (auto item = Inflight.GetValue(lsn)) {
        Y_ABORT_UNLESS(item->Range == range);

        auto& inflight = item->Value;
        inflight.RestorePBuffer(host);
<<<<<<< HEAD
    } else {
        Inflight.AddRange(
            lsn,
            range,
            TInflightInfo(this, lsn, range.Size() * BlockSize, host));
=======
        return;
>>>>>>> 78a20188184 (Field map set optimization (#52565))
    }

    Inflight.AddRange(
        pBufferKey,
        range,
        TInflightInfo(this, DesiredDDisks, DisabledHosts));
    auto item = Inflight.GetValue(pBufferKey);
    Y_ABORT_UNLESS(item);
    auto& inflight = item->Value;
    inflight.RestorePBuffer(host);
}

// Create multiple readRangeHints for specified range with possible overlapping
// with inflight requests
TReadHint TBlocksDirtyMap::MakeReadHint(TBlockRange16 range)
{
    ++Stats.ReadRequestCount;
    TReadHint result;
    if (!Inflight.HasOverlaps(range)) {   // read from ddisk
<<<<<<< HEAD
        result.RangeHints.push_back(MakeReadRangeHint({}, 0, range, 0));
=======
        ++Stats.ReadFromDDiskCount;
        result.RangeHints.push_back(MakeReadRangeHint({}, {}, range, 0));
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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

    ui16 offsetBlocks{};
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

    for (const auto& hint: result.RangeHints) {
        if (hint.PBufferKey.Lsn == 0) {
            ++Stats.ReadFromDDiskCount;
        } else {
            ++Stats.ReadFromPBufferCount;
        }
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

<<<<<<< HEAD
<<<<<<< HEAD
    TSet<ui64> readyToFlush;
=======
    TPBufferKeySet readyToFlush{&ArenaAllocatorPool};
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
    TPBufferKeySet readyToFlush{ArenaAllocatorPool.get()};
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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

<<<<<<< HEAD
<<<<<<< HEAD
    TSet<ui64> readyToErase;
=======
    TPBufferKeySet readyToErase{&ArenaAllocatorPool};
>>>>>>> 78a20188184 (Field map set optimization (#52565))
=======
    TPBufferKeySet readyToErase{ArenaAllocatorPool.get()};
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    readyToErase.swap(ReadyToErase);

    for (ui64 lsn: readyToErase) {
        auto item = Inflight.GetValue(lsn);
        Y_ABORT_UNLESS(item);

        auto& val = item->Value;

<<<<<<< HEAD
=======
        if (HasOlderOverlap(pBufferKey, item->Range)) {
            // Overlapping records are erased in ascending order, one at a
            // time. Otherwise the older record may survive on a PBuffer after
            // the newer one is erased everywhere, and after a restart it is
            // restored and flushed over the newer data.
            ReadyToErase.insert(pBufferKey);
            continue;
        }

        if (!CheckEraseAbility(item->Range, val)) {
            ReadyToErase.insert(pBufferKey);
            continue;
        }

>>>>>>> 7f270b59ea6 (NBS-7459: erase overlapping PBuffer records in TPBufferKey order (#53425))
        for (THostIndex host: val.GetEraseNeeded()) {
            val.RequestErase(host);

            if (DisabledHosts.Get(host)) {
                // We can't handle this situation properly. Barrier cleanup
                // will help us.
                val.ConfirmErase(host);
                if (val.GetState() == TInflightInfo::EState::PBufferErased) {
                    RemovePBuffer(pBufferKey);
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

    TInfoEraseBelatedSet readyToEraseBelated{ArenaAllocatorPool.get()};
    readyToEraseBelated.swap(ReadyToEraseBelated);
    for (const auto& item: readyToEraseBelated) {
        auto hostMask = item.Hosts;
        for (auto host: hostMask) {
            result.AddHint(host, item.Lsn);
        }
    }

    return result;
}

<<<<<<< HEAD
void TBlocksDirtyMap::RegisterInflightWrite(ui64 lsn, TBlockRange64 range)
=======
void TBlocksDirtyMap::RegisterInflightWrite(
    TPBufferKey pBufferKey,
    TBlockRange16 range)
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
{
    const bool inserted = Inflight.AddRange(
        lsn,
        range,
<<<<<<< HEAD
        TInflightInfo(this, lsn, range.Size() * BlockSize));
=======
        TInflightInfo(this, DesiredDDisks, DisabledHosts));
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    Y_ABORT_UNLESS(inserted);
}

void TBlocksDirtyMap::WriteFinished(
<<<<<<< HEAD
    ui64 lsn,
    TBlockRange64 range,
=======
    TPBufferKey pBufferKey,
    TBlockRange16 range,
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    THostMask requested,
    THostMask confirmed)
{
    // Every write is pre-registered as pending at generation time (see
    // RegisterInflightWrite), so the entry always exists here.
    auto item = Inflight.GetValue(lsn);
    Y_ABORT_UNLESS(item);
    Y_ABORT_UNLESS(item->Range == range);

    if (confirmed.Count() < QuorumDirectBlockGroupHostCount) {
<<<<<<< HEAD
        const bool removed = Inflight.RemoveRange(lsn);
        Y_ABORT_UNLESS(removed);
=======
        // The write request did not reach the quorum. We responded to the
        // client with an error. The written PBuffers will be cleared through a
        // barrier garbage collection later. For now, we will forget about this
        // request as if it never existed.
        RemovePBuffer(pBufferKey);
>>>>>>> 78a20188184 (Field map set optimization (#52565))
        return;
    }

    item->Value.OnWritten(requested, confirmed);
}

void TBlocksDirtyMap::FlushFinished(
    THostRoute route,
    const TVector<ui64>& flushOk,
    const TVector<ui64>& flushFailed)
{
    if (route.SourceHostIndex != route.DestinationHostIndex) {
        ++Stats.CrossNodeFlushCount;
    } else {
        ++Stats.InNodeFlushCount;
    }

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
    }

    for (ui64 lsn: flushFailed) {
        auto item = Inflight.GetValue(lsn);
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
    const TVector<ui64>& eraseOk,
    const TVector<ui64>& eraseFailed)
{
    for (ui64 lsn: eraseOk) {
        auto item = Inflight.GetValue(lsn);
        if (!item) {
            // The item was deleted when the host was disabled.
            continue;
        }
        auto& inflight = item->Value;
        inflight.ConfirmErase(host);
        if (inflight.GetState() == TInflightInfo::EState::PBufferErased) {
            ReadyToErase.erase(pBufferKey);
            RemovePBuffer(pBufferKey);
        }
    }

    for (ui64 lsn: eraseFailed) {
        auto item = Inflight.GetValue(lsn);
        if (!item) {
            // The item was deleted when the host was disabled.
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

void TBlocksDirtyMap::MarkFresh(THostIndex host, ui64 bytesOffset)
{
<<<<<<< HEAD
    DDiskStates[host].SetReadWatermark(bytesOffset / BlockSize);
    DDiskStates[host].SetFlushWatermark(bytesOffset / BlockSize);
=======
    Y_ABORT_UNLESS(bytesOffset / BlockSize < Max<ui16>());

    DDiskStates[host].UpdateWatermarkDebugOnly(
        IntegerCast<ui16>(bytesOffset / BlockSize));
>>>>>>> 78a20188184 (Field map set optimization (#52565))
}

<<<<<<< HEAD
std::optional<ui64> TBlocksDirtyMap::GetFreshWatermark(THostIndex host) const
{
<<<<<<< HEAD
    if (DDiskStates[host].GetState() == TDDiskState::EState::Operational) {
        return std::nullopt;
=======
    const auto range = DDiskStates[host].GetFreshRange();
    if (!range) {
        return std::nullopt;
    }
    return TBlockRange64::MakeClosedInterval(range->Start, range->End);
=======
std::optional<TBlockRange16> TBlocksDirtyMap::GetFreshRange(
    THostIndex host) const
{
    return DDiskStates[host].GetFreshRange();
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
}

TSyncHint TBlocksDirtyMap::BeginRangeSync(THostIndex host, TBlockRange16 range)
{
    TInflightDDiskSync inflightSync{.DestinationHost = host};

    TSyncHint result{
        .SyncId = ++InflightDDiskSyncIdGenerator,
        .Host = host,
        .Range = range,
        .ReadyToStart = inflightSync.SyncStartTrigger.GetFuture()};

    if (!HasInflightFlush(host, range)) {
        inflightSync.SyncStartTrigger.SetValue();
>>>>>>> 78a20188184 (Field map set optimization (#52565))
    }
    return DDiskStates[host].GetOperationalBlockCount() * BlockSize;
}

void TBlocksDirtyMap::SetReadWatermark(THostIndex host, ui64 bytesOffset)
{
<<<<<<< HEAD
    DDiskStates[host].SetReadWatermark(bytesOffset / BlockSize);
=======
    auto inflightSync = InflightDDiskSyncMap.ExtractRange(syncId);
    if (!inflightSync) {
        return;
    }

    if (success) {
        DDiskStates[inflightSync->Value.DestinationHost].RangeSynced(
            inflightSync->Range);
    }
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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

<<<<<<< HEAD
const TPBufferCounters& TBlocksDirtyMap::GetPBufferCounters(
    THostIndex host) const
{
    Y_ABORT_UNLESS(host < PBufferCounters.size());
    return PBufferCounters[host];
}

void TBlocksDirtyMap::LockPBuffer(ui64 lsn)
{
<<<<<<< HEAD
    auto item = Inflight.GetValue(lsn);
=======
    if (host >= PBufferCounters.size()) {
        return {};
    }

    return PBufferCounters[host].Current;
}

ui64 TBlocksDirtyMap::GetFreshTotalBytes(THostIndex host) const
{
    if (host >= DDiskStates.size()) {
        return 0;
    }
    return static_cast<ui64>(DDiskStates[host].GetFreshBlockCount()) *
           BlockSize;
}

ui64 TBlocksDirtyMap::GetRottenTotalBytes(THostIndex host) const
{
    if (host >= DDiskStates.size()) {
        return 0;
    }
    return static_cast<ui64>(DDiskStates[host].GetRottenBlockCount()) *
           BlockSize;
}

=======
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
void TBlocksDirtyMap::LockPBuffer(TPBufferKey pBufferKey)
{
    auto item = Inflight.GetValue(pBufferKey);
>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
    TBlockRange16 range,
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

<<<<<<< HEAD
void TBlocksDirtyMap::Register(ui64 lsn, EQueueType queueType)
=======
TPBufferKey TBlocksDirtyMap::GetPBufferKey(const TInflightInfo& inflight) const
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
{
    return TInflightMap::GetKeyByValue(inflight);
}

void TBlocksDirtyMap::Register(
    const TInflightInfo& inflight,
    EQueueType queueType)
{
    auto pBufferKey = TInflightMap::GetKeyByValue(inflight);

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

<<<<<<< HEAD
void TBlocksDirtyMap::UnRegister(ui64 lsn)
{
<<<<<<< HEAD
    ReadyToErase.erase(lsn);
    ReadyToClone.erase(lsn);
    ReadyToFlush.erase(lsn);
=======
=======
void TBlocksDirtyMap::UnRegister(
    const TInflightInfo& inflight,
    EQueueType queueType)
{
    auto pBufferKey = TInflightMap::GetKeyByValue(inflight);

>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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

void TBlocksDirtyMap::InflightFlushFinished(
    const TInflightInfo& inflight,
    THostIndex host)
{
    if (InflightDDiskSyncMap.Empty()) {
        return;
    }

    const auto range = TInflightMap::GetRangeByValue(inflight);

    InflightDDiskSyncMap.EnumerateOverlapping(
        range,
        [&](TInflightDDiskSyncMap::TFindItem& item)
        {
            auto& sync = item.Value;
            if (!sync.SyncStartTrigger.IsReady() &&
                sync.DestinationHost == host &&
                !HasInflightFlush(host, item.Range))
            {
                sync.SyncStartTrigger.TrySetValue();
            }

            return TInflightDDiskSyncMap::EEnumerateContinuation::Continue;
        });
}

void TBlocksDirtyMap::FlushCompleted(
    const TInflightInfo& inflight,
    THostMask ddisks)
{
<<<<<<< HEAD
    Y_DEBUG_ABORT_UNLESS(Inflight.GetValue(pBufferKey));

    AddToAheadAndBehindOnFlushCompleted(pBufferKey, ddisks);
>>>>>>> 670e22678de ([NBS-7383] Fixed ddisk sync hung (#52938))
=======
    AddToAheadAndBehindOnFlushCompleted(inflight, ddisks);
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
}

void TBlocksDirtyMap::DataToPBufferAdded(
    const TInflightInfo& inflight,
    THostIndex host,
    EPBufferCounter counter)
{
    const auto range = TInflightMap::GetRangeByValue(inflight);
    const size_t byteCount = static_cast<size_t>(range.Size()) * BlockSize;
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
    const TInflightInfo& inflight,
    THostIndex host,
    EPBufferCounter counter)
{
    const auto range = TInflightMap::GetRangeByValue(inflight);
    const size_t byteCount = static_cast<size_t>(range.Size()) * BlockSize;

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

<<<<<<< HEAD
=======
void TBlocksDirtyMap::OnBehindAheadChanged()
{
    ++StateGeneration;
}

>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
bool TBlocksDirtyMap::NeedFlush() const
{
    return !ReadyToFlush.empty();
}

bool TBlocksDirtyMap::NeedErase() const
{
    return !ReadyToErase.empty() || !ReadyToEraseBelated.empty();
}

<<<<<<< HEAD
=======
bool TBlocksDirtyMap::NeedPersist() const
{
    const bool needPersistAheadBehind =
        StateGeneration > PersistedStateGeneration;
    return needPersistAheadBehind;
}

TDirtyMapStateProto TBlocksDirtyMap::GetStateForPersist() const
{
    TDirtyMapStateProto result;

    for (const auto& ddiskState: DDiskStates) {
        ddiskState.Save(result.AddDDiskStates());
    }

    return result;
}

void TBlocksDirtyMap::StatePersisted(ui32 persistGeneration)
{
    Y_ABORT_UNLESS(persistGeneration > PersistedStateGeneration);
    Y_ABORT_UNLESS(persistGeneration <= StateGeneration);

    PersistedStateGeneration = persistGeneration;
}

ui32 TBlocksDirtyMap::GetCurrentGeneration() const
{
    return StateGeneration;
}

void TBlocksDirtyMap::Trim()
{
    Inflight.Trim();
}

TDirtyMapStats TBlocksDirtyMap::GetStats() const
{
    TArenaPoolStats memStats;

    for (const auto& ddiskState: DDiskStates) {
        memStats.Aggregate(ddiskState.GetMemoryStats());
    }

    return {
        .InflightCount = GetInflightCount(),
        .ReadyToFlushCount = GetFlushPendingCount(),
        .ReadyToEraseCount = GetErasePendingCount(),
        .ReadRequestCount = Stats.ReadRequestCount,
        .ReadFromDDiskCount = Stats.ReadFromDDiskCount,
        .ReadFromPBufferCount = Stats.ReadFromPBufferCount,
        .CrossNodeFlushCount = Stats.CrossNodeFlushCount,
        .InNodeFlushCount = Stats.InNodeFlushCount,
        .DDisksMemoryStats = memStats,
    };
}

<<<<<<< HEAD
>>>>>>> 78a20188184 (Field map set optimization (#52565))
TString TBlocksDirtyMap::DebugPrintPBuffers()
=======
TDirtyMapHostStats TBlocksDirtyMap::GetHostStats(THostIndex host) const
{
    const auto ddiskTotalBytes =
        host < DDiskStates.size() && DesiredDDisks.Get(host)
            ? BlockCount * BlockSize
            : 0;
    const auto freshTotalBytes =
        host < DDiskStates.size()
            ? static_cast<ui64>(DDiskStates[host].GetFreshBlockCount()) *
                  BlockSize
            : 0;
    const auto rottenTotalBytes =
        host < DDiskStates.size()
            ? static_cast<ui64>(DDiskStates[host].GetRottenBlockCount()) *
                  BlockSize
            : 0;

    return {
        .PBufferCounters = host < PBufferCounters.size() ? PBufferCounters[host]
                                                         : TPBufferCounters(),
        .PBuffersUsage = GetPBuffersUsage(host),
        .DDiskTotalBytes = ddiskTotalBytes,
        .FreshTotalBytes = freshTotalBytes,
        .RottenTotalBytes = rottenTotalBytes,
    };
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

TString TBlocksDirtyMap::DebugPrintPBuffers() const
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
{
    TInstant now = TInstant::Now();
    TStringBuilder result;
    Inflight.Enumerate(
        [&](const TInflightMap::TFindItem& item)
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

<<<<<<< HEAD
=======
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
    result << "gen:" << GetCurrentGeneration() << "/"
           << PersistedStateGeneration << " ";
    for (THostIndex h = 0; h < GetHostCount(); ++h) {
        auto brief = DDiskStates[h].DebugPrintAheadBehindBrief();
        if (brief) {
            result << PrintHostIndex(h) << ":" << brief;
        }
    }
    return result;
}

TString TBlocksDirtyMap::DebugPrintInflightSync() const
{
    TStringBuilder result;
    InflightDDiskSyncMap.Enumerate(
        [&](const TInflightDDiskSyncMap::TFindItem& item)
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
    while (DDiskStates.size() < newHostCount) {
        DDiskStates.emplace_back(ArenaAllocator, BlockCount);
    }
}

>>>>>>> 78a20188184 (Field map set optimization (#52565))
THostMask TBlocksDirtyMap::FilterLocations(
    THostMask mask,
    TBlockRange16 range) const
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
<<<<<<< HEAD
    ui64 lsn,
    TBlockRange64 range,
    ui64 offsetBlocks)
=======
    TPBufferKey pBufferKey,
    TBlockRange16 range,
    ui16 offsetBlocks)
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
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
<<<<<<< HEAD
        lsn,
        TBlockRange64::WithLength(offsetBlocks, range.Size()),
=======
        pBufferKey,
        TBlockRange16::WithLength(offsetBlocks, range.Size()),
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
        range,
        lsn == 0 ? TRangeLock(this, range, mask) : TRangeLock(this, lsn));
}

<<<<<<< HEAD
=======
void TBlocksDirtyMap::AddToAheadAndBehindOnFlushCompleted(
    const TInflightInfo& inflight,
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

    const auto state = inflight.GetState();
    const auto range = TInflightMap::GetRangeByValue(inflight);
    Y_ABORT_UNLESS(
        state == TInflightInfo::EState::PBufferFlushed ||
        state == TInflightInfo::EState::PBufferErasing);

    for (THostIndex host = 0; host < GetHostCount(); ++host) {
        DDiskStates[host].OnRangeFlushed(
            range,
            ddisks.Get(host) ? TDDiskState::EFlushCompletion::Completed
                             : TDDiskState::EFlushCompletion::Missed);
    }
}

bool TBlocksDirtyMap::HasOlderUnflushedOverlap(
    TPBufferKey pBufferKey,
    TBlockRange16 range)
{
    bool found = false;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& overlap)
        {
            const auto state = overlap.Value.GetState();
            const bool onDDisk =
                state == TInflightInfo::EState::PBufferFlushed ||
                state == TInflightInfo::EState::PBufferErasing ||
                state == TInflightInfo::EState::PBufferErased;
            if (overlap.Key < pBufferKey && !onDDisk) {
                found = true;
                return TInflightMap::EEnumerateContinuation::Stop;
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });
    return found;
}

bool TBlocksDirtyMap::HasOlderOverlap(
    TPBufferKey pBufferKey,
    TBlockRange16 range)
{
    bool found = false;
    Inflight.EnumerateOverlapping(
        range,
        [&](TInflightMap::TFindItem& overlap)
        {
            if (overlap.Key < pBufferKey) {
                found = true;
                return TInflightMap::EEnumerateContinuation::Stop;
            }
            return TInflightMap::EEnumerateContinuation::Continue;
        });
    return found;
}

bool TBlocksDirtyMap::HasInflightFlush(THostIndex host, TBlockRange16 range)
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

bool TBlocksDirtyMap::CheckEraseAbility(
    TBlockRange16 range,
    TInflightInfo& inflightInfo)
{
    if (StateGeneration == 0) {
        // There is not a single red block.
        return true;
    }

    if (inflightInfo.GetPersistGeneration() &&
        inflightInfo.GetPersistGeneration() <= PersistedStateGeneration)
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
        inflightInfo.SetPersistGeneration(StateGeneration);
    }
    return false;
}

void TBlocksDirtyMap::RemovePBuffer(TPBufferKey pBufferKey)
{
    Y_ABORT_UNLESS(!ReadyToFlush.contains(pBufferKey));
    Y_ABORT_UNLESS(!ReadyToErase.contains(pBufferKey));

    const bool removed = Inflight.RemoveRange(pBufferKey);
    Y_ABORT_UNLESS(removed);
}

////////////////////////////////////////////////////////////////////////////////

>>>>>>> 78a20188184 (Field map set optimization (#52565))
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
