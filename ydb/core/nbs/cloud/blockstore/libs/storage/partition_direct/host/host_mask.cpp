#include "host_mask.h"

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

bool THostRoute::operator<(const THostRoute& other) const
{
    if (SourceHostIndex != other.SourceHostIndex) {
        return SourceHostIndex < other.SourceHostIndex;
    }
    return DestinationHostIndex < other.DestinationHostIndex;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
