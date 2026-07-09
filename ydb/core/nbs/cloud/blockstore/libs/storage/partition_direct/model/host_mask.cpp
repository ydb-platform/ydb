#include "host_mask.h"

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
THostMask THostMask::MakeOne(THostIndex host)
{
    Y_ABORT_UNLESS(host < MaxHostCount);
    return THostMask(static_cast<ui32>(1) << host);
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

// static
THostMask THostMask::MakeFromRoute(const THostRoute& route)
{
    THostMask result;
    result.Set(route.SourceHostIndex);
    result.Set(route.DestinationHostIndex);
    return result;
}

void THostMask::Set(THostIndex host)
{
    Y_ABORT_UNLESS(host < MaxHostCount);
    Bits |= ui32(1) << host;
}

void THostMask::Reset(THostIndex host)
{
    Y_ABORT_UNLESS(host < MaxHostCount);
    Bits &= ~(ui32(1) << host);
}

bool THostMask::Get(THostIndex host) const
{
    Y_ABORT_UNLESS(host < MaxHostCount);
    return (Bits & (ui32(1) << host)) != 0;
}

void THostMask::RemoveHost(THostIndex host)
{
    Y_ABORT_UNLESS(host < MaxHostCount);
    const ui32 low = Bits & ((ui32(1) << host) - 1);
    // The wide shift keeps `host + 1 == 32` (removing host 31) defined.
    const ui32 high =
        static_cast<ui32>((static_cast<ui64>(Bits) >> (host + 1)) << host);
    Bits = low | high;
}

bool THostMask::Empty() const
{
    return Bits == 0;
}

size_t THostMask::Count() const
{
    return std::bitset<32>(Bits).count();
}

THostMask THostMask::LogicalNot() const
{
    return THostMask(~Bits);
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

std::optional<THostIndex> THostMask::Nth(size_t index) const
{
    ui32 remaining = Bits;
    for (size_t i = 0; i < index; ++i) {
        if (remaining == 0) {
            return std::nullopt;
        }
        remaining &= remaining - 1;
    }
    if (remaining == 0) {
        return std::nullopt;
    }
    return static_cast<THostIndex>(std::countr_zero(remaining));
}

TVector<THostIndex> THostMask::Hosts() const
{
    TVector<THostIndex> result;
    result.reserve(Count());
    for (auto host: *this) {
        result.push_back(host);
    }
    return result;
}

TString THostMask::Print() const
{
    TStringBuilder result;
    result << "[";
    bool first = true;
    for (auto host: *this) {
        if (!first) {
            result << ",";
        }
        result << PrintHostIndex(host);
        first = false;
    }
    result << "]";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
