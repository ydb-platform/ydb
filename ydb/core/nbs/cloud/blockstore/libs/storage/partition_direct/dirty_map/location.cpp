#include "location.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <bit>
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
        case ELocation::Unknown:
            return false;
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
        case ELocation::Unknown:
            return false;
    }
}

ELocation TranslateDDiskToPBuffer(ELocation location)
{
    switch (location) {
        case ELocation::PBuffer0:
        case ELocation::PBuffer1:
        case ELocation::PBuffer2:
        case ELocation::HOPBuffer0:
        case ELocation::HOPBuffer1:
        case ELocation::Unknown:
            Y_ABORT_UNLESS(false);

        case ELocation::DDisk0:
            return ELocation::PBuffer0;
        case ELocation::DDisk1:
            return ELocation::PBuffer1;
        case ELocation::DDisk2:
            return ELocation::PBuffer2;
        case ELocation::HODDisk0:
            return ELocation::HOPBuffer0;
        case ELocation::HODDisk1:
            return ELocation::HOPBuffer1;
    }
}

ELocation TranslatePBufferToDDisk(ELocation location)
{
    switch (location) {
        case ELocation::PBuffer0:
            return ELocation::DDisk0;
        case ELocation::PBuffer1:
            return ELocation::DDisk1;
        case ELocation::PBuffer2:
            return ELocation::DDisk2;
        case ELocation::HOPBuffer0:
            return ELocation::HODDisk0;
        case ELocation::HOPBuffer1:
            return ELocation::HODDisk1;

        case ELocation::DDisk0:
        case ELocation::DDisk1:
        case ELocation::DDisk2:
        case ELocation::HODDisk0:
        case ELocation::HODDisk1:
        case ELocation::Unknown:
            Y_ABORT_UNLESS(false);
    }
}

size_t GetLocationIndex(ELocation location)
{
    switch (location) {
        case ELocation::PBuffer0:
            return 0;
        case ELocation::PBuffer1:
            return 1;
        case ELocation::PBuffer2:
            return 2;
        case ELocation::HOPBuffer0:
            return 3;
        case ELocation::HOPBuffer1:
            return 4;
        case ELocation::DDisk0:
            return 5;
        case ELocation::DDisk1:
            return 6;
        case ELocation::DDisk2:
            return 7;
        case ELocation::HODDisk0:
            return 8;
        case ELocation::HODDisk1:
            return 9;
        case ELocation::Unknown:
            Y_ABORT_UNLESS(false);
            return 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TLocationMask::TIterator::TIterator(const TLocationMask& mask)
    : Location(mask.FirstLocation())
    , Mask(&mask)
{}

bool TLocationMask::TIterator::operator==(const TIterator& other) const
{
    return Location == other.Location;
}

bool TLocationMask::TIterator::operator!=(const TIterator& other) const
{
    return Location != other.Location;
}

TLocationMask::TIterator& TLocationMask::TIterator::operator++()
{
    const int currentBit = std::countr_zero(std::bit_cast<ui16>(Location));
    const int lastLocationBit =
        std::countr_zero(std::bit_cast<ui16>(ELocation::HODDisk1));

    for (int i = currentBit + 1; i < 16; ++i) {
        if (i > lastLocationBit) {
            Location = ELocation::Unknown;
            break;
        }
        const ui16 rawLocation = 1 << i;
        Location = std::bit_cast<ELocation>(rawLocation);
        if (Mask->Get(Location)) {
            break;
        }
    }
    return *this;
}

ELocation TLocationMask::TIterator::operator*() const
{
    return Location;
}

ELocation TLocationMask::TIterator::operator->() const
{
    return Location;
}

////////////////////////////////////////////////////////////////////////////////

// static
TLocationMask TLocationMask::MakeEmpty()
{
    return {};
}

// static
TLocationMask TLocationMask::MakeOne(ELocation location)
{
    TLocationMask mask;
    mask.Set(location);
    return mask;
}

//    static
TLocationMask TLocationMask::Make(
    bool primary0,
    bool primary1,
    bool primary2,
    bool handOff0,
    bool handOff1)
{
    return MakePBuffer(primary0, primary1, primary2, handOff0, handOff1)
        .Include(MakeDDisk(primary0, primary1, primary2, handOff0, handOff1));
}

// static
TLocationMask TLocationMask::MakePBuffer(
    bool pBuffer0,
    bool pBuffer1,
    bool pBuffer2,
    bool handOff0,
    bool handOff1)
{
    return TLocationMask(static_cast<ui16>(
        (pBuffer0 ? static_cast<ui16>(ELocation::PBuffer0) : 0) +
        (pBuffer1 ? static_cast<ui16>(ELocation::PBuffer1) : 0) +
        (pBuffer2 ? static_cast<ui16>(ELocation::PBuffer2) : 0) +
        (handOff0 ? static_cast<ui16>(ELocation::HOPBuffer0) : 0) +
        (handOff1 ? static_cast<ui16>(ELocation::HOPBuffer1) : 0)));
}

// static
TLocationMask TLocationMask::MakeDDisk(
    bool dDisk0,
    bool dDisk1,
    bool dDisk2,
    bool handOff0,
    bool handOff1)
{
    return TLocationMask(static_cast<ui16>(
        (dDisk0 ? static_cast<ui16>(ELocation::DDisk0) : 0) +
        (dDisk1 ? static_cast<ui16>(ELocation::DDisk1) : 0) +
        (dDisk2 ? static_cast<ui16>(ELocation::DDisk2) : 0) +
        (handOff0 ? static_cast<ui16>(ELocation::HODDisk0) : 0) +
        (handOff1 ? static_cast<ui16>(ELocation::HODDisk1) : 0)));
}

// static
TLocationMask TLocationMask::MakePrimary()
{
    return TLocationMask(PrimaryDDisks | PrimaryPBuffers);
}

// static
TLocationMask TLocationMask::MakePrimaryDDisks()
{
    return TLocationMask(PrimaryDDisks);
}

// static
TLocationMask TLocationMask::MakePrimaryPBuffers()
{
    return TLocationMask(PrimaryPBuffers);
}

// static
TLocationMask TLocationMask::MakeAllDDisks()
{
    return TLocationMask(AllDDisks);
}

// static
TLocationMask TLocationMask::MakeAllPBuffers()
{
    return TLocationMask(AllPBuffers);
}

TLocationMask TLocationMask::Exclude(const TLocationMask& other) const
{
    return TLocationMask(Mask & ~other.Mask);
}

TLocationMask TLocationMask::Include(const TLocationMask& other) const
{
    return TLocationMask(Mask | other.Mask);
}

TLocationMask TLocationMask::LogicalAnd(const TLocationMask& other) const
{
    return TLocationMask(Mask & other.Mask);
}

TLocationMask TLocationMask::PBuffers() const
{
    return TLocationMask(Mask & AllPBuffers);
}

TLocationMask TLocationMask::DDisks() const
{
    return TLocationMask(Mask & AllDDisks);
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

bool TLocationMask::OnlyDDisk() const
{
    return (Mask != 0) && ((Mask & AllDDisks) == Mask);
}

bool TLocationMask::HasPBuffer() const
{
    return (Mask & AllPBuffers) != 0;
}

bool TLocationMask::OnlyPBuffer() const
{
    return (Mask != 0) && (Mask & AllPBuffers) == Mask;
}

std::optional<ELocation> TLocationMask::GetLocation(size_t tryNumber) const
{
    size_t locationIndex = 0;
    for (auto location: AllLocations) {
        if (Get(location)) {
            if (locationIndex == tryNumber) {
                return location;
            }

            locationIndex++;
        }
    }
    return std::nullopt;
}

bool TLocationMask::operator==(const TLocationMask& other) const
{
    return Mask == other.Mask;
}

ELocation TLocationMask::FirstLocation() const
{
    const ui32 firstSetBit = std::countr_zero(Mask);
    const ui32 rawLocation = static_cast<ui16>(1) << firstSetBit;
    if (rawLocation > std::bit_cast<ui16>(ELocation::HODDisk1)) {
        return ELocation::Unknown;
    }
    return static_cast<ELocation>(rawLocation);
}

TLocationMask::TIterator TLocationMask::begin() const
{
    return TIterator(*this);
}

TLocationMask::TIterator TLocationMask::end() const
{
    return TIterator{};
}

TString TLocationMask::Print() const
{
    TStringBuilder result;
    result << "[D";
    result << (Get(ELocation::DDisk0) ? "+" : ".");
    result << (Get(ELocation::DDisk1) ? "+" : ".");
    result << (Get(ELocation::DDisk2) ? "+" : ".");
    result << (Get(ELocation::HODDisk0) ? "*" : ".");
    result << (Get(ELocation::HODDisk1) ? "*" : ".");
    result << "P";
    result << (Get(ELocation::PBuffer0) ? "+" : ".");
    result << (Get(ELocation::PBuffer1) ? "+" : ".");
    result << (Get(ELocation::PBuffer2) ? "+" : ".");
    result << (Get(ELocation::HOPBuffer0) ? "*" : ".");
    result << (Get(ELocation::HOPBuffer1) ? "*" : ".");
    result << "]";
    return result;
}

TLocationMask::TLocationMask(ui16 mask)
    : Mask(mask)
{}

////////////////////////////////////////////////////////////////////////////////

bool TRoute::operator==(const TRoute& other) const
{
    return Source == other.Source && Destination == other.Destination;
}

bool TRoute::operator<(const TRoute& other) const
{
    return Source < other.Source ||
           (Source == other.Source && Destination < other.Destination);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
