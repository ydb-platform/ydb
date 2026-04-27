#include "host_status.h"

#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <bit>
#include <bitset>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

THostMask::THostMask(ui32 bits)
    : Bits(bits)
{}

// static
THostMask THostMask::MakeEmpty()
{
    return {};
}

// static
THostMask THostMask::MakeOne(THostIndex h)
{
    Y_ABORT_UNLESS(h < MaxHostCount);
    return THostMask(static_cast<ui32>(1) << h);
}

// static
THostMask THostMask::MakeAll(size_t hostCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    if (hostCount == MaxHostCount) {
        return THostMask(~ui32(0));
    }
    return THostMask((ui32(1) << hostCount) - 1);
}

void THostMask::Set(THostIndex h)
{
    Y_ABORT_UNLESS(h < MaxHostCount);
    Bits |= ui32(1) << h;
}

void THostMask::Reset(THostIndex h)
{
    Y_ABORT_UNLESS(h < MaxHostCount);
    Bits &= ~(ui32(1) << h);
}

bool THostMask::Test(THostIndex h) const
{
    Y_ABORT_UNLESS(h < MaxHostCount);
    return (Bits & (ui32(1) << h)) != 0;
}

bool THostMask::Empty() const
{
    return Bits == 0;
}

size_t THostMask::Count() const
{
    return std::bitset<32>(Bits).count();
}

THostMask THostMask::LogicalAnd(THostMask other) const
{
    return THostMask(Bits & other.Bits);
}

THostMask THostMask::Include(THostMask other) const
{
    return THostMask(Bits | other.Bits);
}

THostMask THostMask::Exclude(THostMask other) const
{
    return THostMask(Bits & ~other.Bits);
}

bool THostMask::Contains(THostMask other) const
{
    return (Bits & other.Bits) == other.Bits;
}

THostMask::TIterator::TIterator(ui32 bits)
    : Remaining(bits)
{}

bool THostMask::TIterator::operator==(const TIterator& other) const
{
    return Remaining == other.Remaining;
}

bool THostMask::TIterator::operator!=(const TIterator& other) const
{
    return Remaining != other.Remaining;
}

THostMask::TIterator& THostMask::TIterator::operator++()
{
    Remaining &= Remaining - 1;   // clear lowest set bit
    return *this;
}

THostIndex THostMask::TIterator::operator*() const
{
    Y_ABORT_UNLESS(Remaining != 0);
    return static_cast<THostIndex>(std::countr_zero(Remaining));
}

THostMask::TIterator THostMask::begin() const
{
    return TIterator(Bits);
}

THostMask::TIterator THostMask::end() const
{
    return TIterator(0);
}

std::optional<THostIndex> THostMask::First() const
{
    if (Bits == 0) {
        return std::nullopt;
    }
    return static_cast<THostIndex>(std::countr_zero(Bits));
}

TString THostMask::Print() const
{
    TStringBuilder result;
    result << "[";
    bool first = true;
    for (auto h: *this) {
        if (!first) {
            result << ",";
        }
        result << "H" << ui32(h);
        first = false;
    }
    result << "]";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

THostStatusList::THostStatusList(size_t hostCount)
    : Statuses(hostCount, EHostStatus::Disabled)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
}

// static
THostStatusList THostStatusList::MakeRotating(
    size_t hostCount,
    ui32 vChunkIndex,
    size_t primaryCount)
{
    Y_ABORT_UNLESS(hostCount <= MaxHostCount);
    Y_ABORT_UNLESS(primaryCount <= hostCount);

    THostStatusList result;
    result.Statuses.assign(hostCount, EHostStatus::HandOff);
    for (size_t i = 0; i < primaryCount; ++i) {
        const size_t idx = (i + vChunkIndex) % hostCount;
        result.Statuses[idx] = EHostStatus::Primary;
    }
    return result;
}

size_t THostStatusList::HostCount() const
{
    return Statuses.size();
}

EHostStatus THostStatusList::Get(THostIndex h) const
{
    Y_ABORT_UNLESS(h < Statuses.size());
    return Statuses[h];
}

void THostStatusList::Set(THostIndex h, EHostStatus status)
{
    Y_ABORT_UNLESS(h < Statuses.size());
    Statuses[h] = status;
}

THostMask THostStatusList::Primary() const
{
    THostMask result;
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (Statuses[i] == EHostStatus::Primary) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostStatusList::HandOff() const
{
    THostMask result;
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (Statuses[i] == EHostStatus::HandOff) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

THostMask THostStatusList::Active() const
{
    return Primary().Include(HandOff());
}

THostMask THostStatusList::Disabled() const
{
    THostMask result;
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (Statuses[i] == EHostStatus::Disabled) {
            result.Set(static_cast<THostIndex>(i));
        }
    }
    return result;
}

TString THostStatusList::Print() const
{
    TStringBuilder result;
    result << "[";
    for (size_t i = 0; i < Statuses.size(); ++i) {
        if (i > 0) {
            result << ",";
        }
        result << "H" << i << ":";
        switch (Statuses[i]) {
            case EHostStatus::Primary:
                result << "P";
                break;
            case EHostStatus::HandOff:
                result << "H";
                break;
            case EHostStatus::Disabled:
                result << "D";
                break;
        }
    }
    result << "]";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool THostRoute::operator<(const THostRoute& other) const
{
    if (SourceHostIndex != other.SourceHostIndex) {
        return SourceHostIndex < other.SourceHostIndex;
    }
    return DestinationHostIndex < other.DestinationHostIndex;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
