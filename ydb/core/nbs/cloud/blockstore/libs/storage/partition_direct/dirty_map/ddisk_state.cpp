#include "ddisk_state.h"

#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TDDiskState::TDDiskState(IArenaAllocatorPtr arenaAllocator, ui16 maxBlockCount)
    : ArenaAllocator(std::move(arenaAllocator))
    , MaxBlockCount(maxBlockCount)
    , BehindField(ArenaAllocator, maxBlockCount)
    , AheadField(ArenaAllocator, maxBlockCount)
{}

void TDDiskState::Init(
    IBehindAheadMonitor* behindAheadMonitor,
    ui16 totalBlockCount,
    ui16 operationalBlockCount)
{
    BehindAheadMonitor = behindAheadMonitor;
    TotalBlockCount = totalBlockCount;
    OperationalBlockCount = operationalBlockCount;

    // Mark all blocks over watermark as behind.
    if (OperationalBlockCount < TotalBlockCount) {
        BehindField.Add(TBlockRange16::MakeClosedInterval(
            OperationalBlockCount,
            TotalBlockCount - 1));
    }
    UpdateState(true);
    CheckInvariants();
}

void TDDiskState::Save(TDDiskStateProto* proto) const
{
    CheckInvariants();
    SaveBlockField(AheadField, proto->MutableAhead());
    SaveBlockField(BehindField, proto->MutableBehind());
}

void TDDiskState::Load(const TDDiskStateProto& proto)
{
    LoadBlockField(proto.GetAhead(), &AheadField);
    BehindField.Remove(AheadField);

    TBlockRangeField loadedBehind(ArenaAllocator, MaxBlockCount);
    LoadBlockField(proto.GetBehind(), &loadedBehind);

    // A non-empty persisted map is more accurate than the initial map
    // reconstructed from the watermark. An empty persisted map may belong to
    // an older state that did not store the initial fresh range, so keep the
    // map prepared by Init in that case.
    if (!loadedBehind.Empty()) {
        BehindField.Clear();
        BehindField.Add(loadedBehind);
    }

    UpdateState(false);
}

void TDDiskState::SwitchOffline()
{
    State = EState::Disabled;
    OperationalBlockCount = 0;
    AheadField.Clear();
    BehindField.Clear();
    CheckInvariants();
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

void TDDiskState::OnRangeFlushed(TBlockRange16 range, EFlushCompletion flush)
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

bool TDDiskState::CanReadFromDDisk(TBlockRange16 range) const
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

bool TDDiskState::HasBehindOverlapping(TBlockRange16 range) const
{
    return BehindField.Overlaps(range);
}

std::optional<TBlockRange16> TDDiskState::GetFreshRange() const
{
    if (GetState() == TDDiskState::EState::Operational ||
        GetState() == TDDiskState::EState::Disabled)
    {
        return std::nullopt;
    }

    if (!BehindField.Empty()) {
        return BehindField.GetFirstRange();
    }

    return TBlockRange16::WithLength(
        OperationalBlockCount,
        TotalBlockCount - OperationalBlockCount);
}

void TDDiskState::RangeSynced(TBlockRange16 range)
{
    if (IsLagging()) {
        return;
    }

    // Update behind.
    const bool behindChanged = BehindField.Remove(range);

    // Update watermark.
    const ui16 newWatermark = IntegerCast<ui16>(range.End + 1);
    if (OperationalBlockCount < newWatermark &&
        !BehindField.Overlaps(TBlockRange16::WithLength(0, newWatermark)))
    {
        OperationalBlockCount = newWatermark;
    }

    // Update ahead.
    bool aheadChanged = AheadField.Remove(range);
    if (auto operational = GetOperationalRange()) {
        aheadChanged |= AheadField.Remove(*operational);
    }

    UpdateState(false);

    if (behindChanged || aheadChanged) {
        BehindAheadMonitor->OnBehindAheadChanged();
    }
}

ui16 TDDiskState::GetFreshBlockCount() const
{
    if (State == EState::Disabled || Lagging) {
        return 0;
    }
    return BehindField.GetBlockCount();
}

ui16 TDDiskState::GetRottenBlockCount() const
{
    return Lagging ? BehindField.GetBlockCount() : 0;
}

size_t TDDiskState::GetAllocatedSize() const
{
    return AheadField.GetAllocatedSize() + BehindField.GetAllocatedSize();
}

size_t TDDiskState::GetUsedSize() const
{
    return AheadField.GetUsedSize() + BehindField.GetUsedSize();
}

void TDDiskState::UpdateWatermarkDebugOnly(ui16 blockCount)
{
    Y_ABORT_UNLESS(blockCount <= TotalBlockCount);

    OperationalBlockCount = blockCount;
    UpdateState(false);
}

void TDDiskState::CheckInvariants() const
{
    if (State == EState::Operational) {
        Y_ABORT_UNLESS(OperationalBlockCount == TotalBlockCount);
        Y_ABORT_UNLESS(BehindField.Empty());
        Y_ABORT_UNLESS(AheadField.Empty());
        return;
    }

    Y_ABORT_UNLESS(!BehindField.Overlaps(AheadField));

    if (auto operation = GetOperationalRange()) {
        Y_ABORT_UNLESS(!AheadField.Overlaps(*operation));
    }
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
    result << "a" << AheadField.GetBlockCount() << ";";
    result << "b" << BehindField.GetBlockCount() << ";";
    return result;
}

bool TDDiskState::IsFresh() const
{
    return OperationalBlockCount != TotalBlockCount || !BehindField.Empty() ||
           !AheadField.Empty();
}

void TDDiskState::UpdateState(bool force)
{
    if (!force && State == EState::Disabled) {
        return;
    }

    State = IsFresh() ? EState::Fresh : EState::Operational;
    CheckInvariants();
}

void TDDiskState::AddAhead(TBlockRange16 range)
{
    Y_ABORT_UNLESS(!Lagging);

    const bool behindChanged = BehindField.Remove(range);
    bool aheadChanged = false;
    if (range.Start > OperationalBlockCount) {
        // Range outside operational blocks.
        aheadChanged = AheadField.Add(range);
    } else if (range.End <= OperationalBlockCount) {
        // Range inside operational blocks.
    } else {
        // Range on operational blocks border.
        aheadChanged = AheadField.Add(TBlockRange16::MakeClosedInterval(
            OperationalBlockCount + 1,
            range.End));
    }

    if (behindChanged || aheadChanged) {
        BehindAheadMonitor->OnBehindAheadChanged();
    }

    CheckInvariants();
}

void TDDiskState::AddBehind(TBlockRange16 range)
{
    const bool aheadChanged = AheadField.Remove(range);
    const bool behindChanged = BehindField.Add(range);
    if (aheadChanged || behindChanged) {
        UpdateState(false);
        BehindAheadMonitor->OnBehindAheadChanged();
    }
}

std::optional<TBlockRange16> TDDiskState::GetOperationalRange() const
{
    if (!OperationalBlockCount) {
        return std::nullopt;
    }
    return TBlockRange16::MakeClosedInterval(0, OperationalBlockCount - 1);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
