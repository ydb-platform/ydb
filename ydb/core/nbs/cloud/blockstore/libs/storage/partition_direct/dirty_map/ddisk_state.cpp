#include "ddisk_state.h"

#include "block_field_serializer.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/protos/dirty_map.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TDDiskState::TDDiskState(IArenaAllocatorPtr arenaAllocator, ui16 maxBlockCount)
    : ArenaAllocator(std::move(arenaAllocator))
    , BehindField(ArenaAllocator, maxBlockCount)
{}

void TDDiskState::Init(
    IBehindMonitor* behindMonitor,
    ui16 totalBlockCount,
    ui16 operationalBlockCount)
{
    BehindMonitor = behindMonitor;
    TotalBlockCount = totalBlockCount;

    // Mark all blocks after the readable prefix as behind.
    if (operationalBlockCount < TotalBlockCount) {
        BehindField.Add(TBlockRange16::MakeClosedInterval(
            operationalBlockCount,
            TotalBlockCount - 1));
    }
    UpdateState(true);
    CheckInvariants();
}

void TDDiskState::Save(TDDiskStateProto* proto) const
{
    CheckInvariants();
    SaveBlockField(BehindField, proto->MutableBehind());
}

void TDDiskState::Load(const TDDiskStateProto& proto)
{
    BehindField.Clear();
    LoadBlockField(proto.GetBehind(), &BehindField);
    UpdateState(false);
}

void TDDiskState::SwitchOffline()
{
    State = EState::Disabled;
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

    // The replica is not lagging and data has been written. The range is now
    // up to date and no longer behind.
    if (!Lagging && flush == EFlushCompletion::Completed) {
        RemoveBehind(range);
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

    return range.End < GetReadableBlockCount();
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

    return BehindField.GetFirstRange();
}

void TDDiskState::RangeSynced(TBlockRange16 range)
{
    if (IsLagging()) {
        return;
    }

    RemoveBehind(range);
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

TArenaPoolStats TDDiskState::GetMemoryStats() const
{
    return BehindField.GetMemoryStats();
}

void TDDiskState::SetReadablePrefixDebugOnly(ui16 readableBlockCount)
{
    Y_ABORT_UNLESS(readableBlockCount <= TotalBlockCount);

    BehindField.Clear();
    if (readableBlockCount < TotalBlockCount) {
        BehindField.Add(TBlockRange16::MakeClosedInterval(
            readableBlockCount,
            TotalBlockCount - 1));
    }
    UpdateState(false);
}

void TDDiskState::CheckInvariants() const
{
    if (State == EState::Operational) {
        Y_ABORT_UNLESS(BehindField.Empty());
    }
}

TString TDDiskState::DebugPrint() const
{
    TStringBuilder result;
    result << "{" << ToString(State);
    if (State == EState::Fresh) {
        result << (Lagging ? "-" : "+");
    }
    result << "," << (State == EState::Disabled ? 0 : GetReadableBlockCount())
           << "}";
    return result;
}

TString TDDiskState::DebugPrintBehind() const
{
    return BehindField.Print();
}

TString TDDiskState::DebugPrintBehindBrief() const
{
    if (BehindField.Empty()) {
        return {};
    }

    TStringBuilder result;
    result << "b" << BehindField.GetBlockCount() << ";";
    return result;
}

bool TDDiskState::IsFresh() const
{
    return !BehindField.Empty();
}

ui16 TDDiskState::GetReadableBlockCount() const
{
    return BehindField.Empty() ? TotalBlockCount
                               : BehindField.GetFirstRange()->Start;
}

void TDDiskState::UpdateState(bool force)
{
    if (!force && State == EState::Disabled) {
        return;
    }

    State = IsFresh() ? EState::Fresh : EState::Operational;
    CheckInvariants();
}

void TDDiskState::RemoveBehind(TBlockRange16 range)
{
    Y_ABORT_UNLESS(!Lagging);

    const bool behindChanged = BehindField.Remove(range);
    if (behindChanged) {
        UpdateState(false);
        BehindMonitor->OnBehindChanged();
    }
}

void TDDiskState::AddBehind(TBlockRange16 range)
{
    const bool behindChanged = BehindField.Add(range);
    if (behindChanged) {
        UpdateState(false);
        BehindMonitor->OnBehindChanged();
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
