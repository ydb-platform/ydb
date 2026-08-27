#include "ddisk_state.h"

#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

void TDDiskState::Init(
    IBehindAheadMonitor* behindAheadMonitor,
    ui64 totalBlockCount,
    ui64 operationalBlockCount)
{
    BehindAheadMonitor = behindAheadMonitor;
    TotalBlockCount = totalBlockCount;
    OperationalBlockCount = operationalBlockCount;
    UpdateState(true);
}

void TDDiskState::Save(TDDiskStateProto* proto) const
{
    SaveBlockField(AheadField, TotalBlockCount, proto->MutableAhead());
    SaveBlockField(BehindField, TotalBlockCount, proto->MutableBehind());
}

void TDDiskState::Load(const TDDiskStateProto& proto)
{
    LoadBlockField(proto.GetAhead(), &AheadField);
    LoadBlockField(proto.GetBehind(), &BehindField);
}

void TDDiskState::SwitchOffline()
{
    State = EState::Disabled;
    OperationalBlockCount = 0;
    AheadField.Clear();
    BehindField.Clear();
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
        AddBehind(range);
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

    // Don't allow reading from "green" blocks for now.
    // if (AheadField.Contains(range))
    //    return true;

    if (BehindField.Overlaps(range)) {
        return false;
    }

    return range.End < OperationalBlockCount;
}

bool TDDiskState::HasBehindOverlapping(TBlockRange64 range) const
{
    return BehindField.Overlaps(range);
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
    const bool behindChanged = BehindField.Remove(range);
    const bool aheadChanged = AheadField.Remove(range);
    if (behindChanged || aheadChanged) {
        BehindAheadMonitor->OnBehindAheadChanged();
    }

    const ui64 newWatermark = range.End + 1;
    if (OperationalBlockCount < newWatermark &&
        !BehindField.Overlaps(TBlockRange64::WithLength(0, newWatermark)))
    {
        OperationalBlockCount = newWatermark;
    }
    UpdateState(false);
}

TCountAndSize TDDiskState::GetAheadSegmentsStat() const
{
    return TCountAndSize{
        .Count = AheadField.GetSegmentCount(),
        .Size = AheadField.GetBlockCount()};
}

TCountAndSize TDDiskState::GetBehindSegmentsStat() const
{
    return TCountAndSize{
        .Count = BehindField.GetSegmentCount(),
        .Size = BehindField.GetBlockCount()};
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

TString TDDiskState::DebugPrintAheadBehindBrief() const
{
    if (AheadField.Empty() && BehindField.Empty()) {
        return {};
    }

    TStringBuilder result;
    result << "a" << AheadField.GetSegmentCount() << "/"
           << AheadField.GetBlockCount() << ";";
    result << "b" << BehindField.GetSegmentCount() << "/"
           << BehindField.GetBlockCount() << ";";
    return result;
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

    const bool behindChanged = BehindField.Remove(range);
    const bool aheadChanged = AheadField.Add(range);
    if (behindChanged || aheadChanged) {
        BehindAheadMonitor->OnBehindAheadChanged();
    }

    if (OperationalBlockCount) {
        AheadField.Remove(TBlockRange64::WithLength(0, OperationalBlockCount));
    }
}

void TDDiskState::AddBehind(TBlockRange64 range)
{
    const bool behindChanged = BehindField.Add(range);
    if (behindChanged) {
        BehindAheadMonitor->OnBehindAheadChanged();
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
