#include "location.h"

#include <util/string/cast.h>

#include <bitset>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

bool IsDDisk(ELocation location)
{
    switch (location) {
        case ELocation::PBuffer0:
        case ELocation::PBuffer1:
        case ELocation::PBuffer2:
        case ELocation::HOPBuffer0:
        case ELocation::HOPBuffer1:
            return false;
        case ELocation::DDisk0:
        case ELocation::DDisk1:
        case ELocation::DDisk2:
        case ELocation::HODDisk0:
        case ELocation::HODDisk1:
            return true;
    }
}

bool IsPBuffer(ELocation location)
{
    switch (location) {
        case ELocation::PBuffer0:
        case ELocation::PBuffer1:
        case ELocation::PBuffer2:
        case ELocation::HOPBuffer0:
        case ELocation::HOPBuffer1:
            return true;
        case ELocation::DDisk0:
        case ELocation::DDisk1:
        case ELocation::DDisk2:
        case ELocation::HODDisk0:
        case ELocation::HODDisk1:
            return false;
    }
}

// static
TLocationMask TLocationMask::MakePBuffer(
    bool pBuffer0,
    bool pBuffer1,
    bool pBuffer2,
    bool handOff0,
    bool handOff1)
{
    return TLocationMask{
        .Mask = static_cast<ui16>(
            (pBuffer0 ? static_cast<ui16>(ELocation::PBuffer0) : 0) +
            (pBuffer1 ? static_cast<ui16>(ELocation::PBuffer1) : 0) +
            (pBuffer2 ? static_cast<ui16>(ELocation::PBuffer2) : 0) +
            (handOff0 ? static_cast<ui16>(ELocation::HOPBuffer0) : 0) +
            (handOff1 ? static_cast<ui16>(ELocation::HOPBuffer1) : 0)),
    };
}

// static
TLocationMask TLocationMask::MakeDDisk(
    bool dDisk0,
    bool dDisk1,
    bool dDisk2,
    bool handOff0,
    bool handOff1)
{
    return TLocationMask{
        .Mask = static_cast<ui16>(
            (dDisk0 ? static_cast<ui16>(ELocation::DDisk0) : 0) +
            (dDisk1 ? static_cast<ui16>(ELocation::DDisk1) : 0) +
            (dDisk2 ? static_cast<ui16>(ELocation::DDisk2) : 0) +
            (handOff0 ? static_cast<ui16>(ELocation::HODDisk0) : 0) +
            (handOff1 ? static_cast<ui16>(ELocation::HODDisk1) : 0)),
    };
}

// static
TLocationMask TLocationMask::MakePrimaryPBuffers()
{
    return MakePBuffer(true, true, true, false, false);
}

bool TLocationMask::Get(ELocation location) const
{
    return (Mask & static_cast<ui16>(location)) != 0;
}

void TLocationMask::Set(ELocation location)
{
    Mask |= static_cast<ui16>(location);
}

void TLocationMask::Reset(ELocation location)
{
    Mask &= ~static_cast<ui16>(location);
}

bool TLocationMask::Empty() const
{
    return Mask == 0;
}

size_t TLocationMask::Count() const
{
    return std::bitset<sizeof(Mask) * 8>{Mask}.count();
}

bool TLocationMask::HasDDisk() const
{
    return (Mask & AllDDisks) != 0;
}

bool TLocationMask::HasPBuffer() const
{
    return (Mask & AllPBuffers) != 0;
}

std::optional<ELocation> TLocationMask::GetLocation(size_t tryNumber) const
{
    size_t skip = 0;
    for (auto location: AllLocations) {
        if (Get(location)) {
            if (skip == tryNumber) {
                return location;
            }
            ++skip;
        }
    }
    return std::nullopt;
}

bool TLocationMask::operator==(const TLocationMask& other) const
{
    return Mask == other.Mask;
}

TString TLocationMask::Print() const
{
    // ToDo
    return ToString(Mask);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
