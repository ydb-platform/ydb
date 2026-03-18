#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <array>
#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class ELocation: ui16
{
    PBuffer0 = 1U << 0U,
    PBuffer1 = 1U << 1U,
    PBuffer2 = 1U << 2U,
    HOPBuffer0 = 1U << 3U,
    HOPBuffer1 = 1U << 4U,

    DDisk0 = 1U << 5U,
    DDisk1 = 1U << 6U,
    DDisk2 = 1U << 7U,
    HODDisk0 = 1U << 8U,
    HODDisk1 = 1U << 9U,
};

constexpr ui16 PrimaryPBuffers = static_cast<ui16>(ELocation::PBuffer0) |
                                 static_cast<ui16>(ELocation::PBuffer1) |
                                 static_cast<ui16>(ELocation::PBuffer2);
constexpr ui16 HandOffPBuffers = static_cast<ui16>(ELocation::HOPBuffer0) |
                                 static_cast<ui16>(ELocation::HOPBuffer1);
constexpr ui16 AllPBuffers = PrimaryPBuffers | HandOffPBuffers;

constexpr ui16 PrimaryDDisks = static_cast<ui16>(ELocation::DDisk0) |
                               static_cast<ui16>(ELocation::DDisk1) |
                               static_cast<ui16>(ELocation::DDisk2);
constexpr ui16 HandOffDDisks = static_cast<ui16>(ELocation::HODDisk0) |
                               static_cast<ui16>(ELocation::HODDisk1);
constexpr ui16 AllDDisks = PrimaryDDisks | HandOffDDisks;

constexpr std::array<ELocation, 10> AllLocations{
    ELocation::DDisk0,
    ELocation::DDisk1,
    ELocation::DDisk2,
    ELocation::HODDisk0,
    ELocation::HODDisk1,
    ELocation::PBuffer0,
    ELocation::PBuffer1,
    ELocation::PBuffer2,
    ELocation::HOPBuffer0,
    ELocation::HOPBuffer1};

bool IsDDisk(ELocation location);
bool IsPBuffer(ELocation location);

////////////////////////////////////////////////////////////////////////////////

struct TLocationMask
{
    ui16 Mask = 0;

    static TLocationMask MakePBuffer(
        bool pBuffer0,
        bool pBuffer1,
        bool pBuffer2,
        bool handOff0,
        bool handOff1);
    static TLocationMask MakeDDisk(
        bool dDisk0,
        bool dDisk1,
        bool dDisk2,
        bool handOff0,
        bool handOff1);
    static TLocationMask MakePrimaryPBuffers();

    [[nodiscard]] bool Get(ELocation location) const;

    void Set(ELocation location);
    void Reset(ELocation location);
    void And(ui16 mask);

    [[nodiscard]] bool Empty() const;
    [[nodiscard]] size_t Count() const;
    [[nodiscard]] bool HasDDisk() const;
    [[nodiscard]] bool HasPBuffer() const;
    [[nodiscard]] std::optional<ELocation> GetLocation(size_t tryNumber) const;

    bool operator==(const TLocationMask& other) const;

    [[nodiscard]] TString Print() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
