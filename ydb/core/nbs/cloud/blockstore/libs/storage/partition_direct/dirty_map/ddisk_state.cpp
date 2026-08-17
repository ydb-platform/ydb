#include "ddisk_state.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
