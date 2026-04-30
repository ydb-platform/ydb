#include "ddisk_state.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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

TDDiskStateList::TDDiskStateList(
    size_t hostCount,
    ui32 blockSize,
    ui64 totalBlockCount)
    : BlockSize(blockSize)
    , States(hostCount)
{
    Y_ABORT_UNLESS(hostCount > 0);
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    Y_ABORT_UNLESS(blockSize > 0);
    for (auto& s: States) {
        s.Init(totalBlockCount, totalBlockCount);
    }
}

size_t TDDiskStateList::HostCount() const
{
    return States.size();
}

void TDDiskStateList::MarkFresh(THostIndex h, ui64 bytesOffset)
{
    Y_ABORT_UNLESS(h < States.size());
    States[h].SetReadWatermark(bytesOffset / BlockSize);
    States[h].SetFlushWatermark(bytesOffset / BlockSize);
}

void TDDiskStateList::SetReadWatermark(THostIndex h, ui64 bytesOffset)
{
    Y_ABORT_UNLESS(h < States.size());
    States[h].SetReadWatermark(bytesOffset / BlockSize);
}

void TDDiskStateList::SetFlushWatermark(THostIndex h, ui64 bytesOffset)
{
    Y_ABORT_UNLESS(h < States.size());
    States[h].SetFlushWatermark(bytesOffset / BlockSize);
}

std::optional<ui64> TDDiskStateList::GetFreshWatermark(THostIndex h) const
{
    Y_ABORT_UNLESS(h < States.size());
    if (States[h].GetState() == TDDiskState::EState::Operational) {
        return std::nullopt;
    }
    return States[h].GetOperationalBlockCount() * BlockSize;
}

bool TDDiskStateList::CanReadFromDDisk(THostIndex h, TBlockRange64 range) const
{
    Y_ABORT_UNLESS(h < States.size());
    return States[h].CanReadFromDDisk(range);
}

bool TDDiskStateList::NeedFlushToDDisk(THostIndex h, TBlockRange64 range) const
{
    Y_ABORT_UNLESS(h < States.size());
    return States[h].NeedFlushToDDisk(range);
}

THostMask TDDiskStateList::FilterReadable(
    THostMask mask,
    TBlockRange64 range) const
{
    THostMask result = mask;
    for (auto h: result) {
        if (!States[h].CanReadFromDDisk(range)) {
            result.Reset(h);
        }
    }
    return result;
}

TString TDDiskStateList::DebugPrint() const
{
    TStringBuilder result;
    for (size_t h = 0; h < States.size(); ++h) {
        result << "H" << h << States[h].DebugPrint() << ";";
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
