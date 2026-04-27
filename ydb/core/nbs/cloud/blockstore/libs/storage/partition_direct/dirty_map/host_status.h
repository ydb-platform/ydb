#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

using THostIndex = ui8;

constexpr THostIndex InvalidHostIndex = 0xFF;
constexpr size_t MaxHostCount = 32;

enum class EHostStatus: ui8
{
    Primary = 0,
    HandOff = 1,
    Disabled = 2,
};

////////////////////////////////////////////////////////////////////////////////

class THostMask
{
public:
    class TIterator
    {
        ui32 Remaining = 0;

    public:
        TIterator() = default;
        explicit TIterator(ui32 bits);

        bool operator==(const TIterator& other) const;
        bool operator!=(const TIterator& other) const;

        TIterator& operator++();
        THostIndex operator*() const;
    };

    THostMask() = default;

    static THostMask MakeEmpty();
    static THostMask MakeOne(THostIndex h);
    static THostMask MakeAll(size_t hostCount);

    void Set(THostIndex h);
    void Reset(THostIndex h);
    [[nodiscard]] bool Test(THostIndex h) const;

    [[nodiscard]] bool Empty() const;
    [[nodiscard]] size_t Count() const;

    [[nodiscard]] THostMask LogicalAnd(THostMask other) const;
    [[nodiscard]] THostMask Include(THostMask other) const;
    [[nodiscard]] THostMask Exclude(THostMask other) const;
    [[nodiscard]] bool Contains(THostMask other) const;

    [[nodiscard]] TIterator begin() const;
    [[nodiscard]] TIterator end() const;
    [[nodiscard]] std::optional<THostIndex> First() const;

    bool operator==(const THostMask& other) const = default;

    [[nodiscard]] TString Print() const;

private:
    explicit THostMask(ui32 bits);

    ui32 Bits = 0;
};

////////////////////////////////////////////////////////////////////////////////

class THostStatusList
{
public:
    THostStatusList() = default;
    explicit THostStatusList(size_t hostCount);

    static THostStatusList
    MakeRotating(size_t hostCount, ui32 vChunkIndex, size_t primaryCount);

    [[nodiscard]] size_t HostCount() const;
    [[nodiscard]] EHostStatus Get(THostIndex h) const;
    void Set(THostIndex h, EHostStatus status);

    [[nodiscard]] THostMask Primary() const;
    [[nodiscard]] THostMask HandOff() const;
    [[nodiscard]] THostMask Active() const;
    [[nodiscard]] THostMask Disabled() const;

    bool operator==(const THostStatusList& other) const = default;

    [[nodiscard]] TString Print() const;

private:
    TVector<EHostStatus> Statuses;
};

////////////////////////////////////////////////////////////////////////////////

struct THostRoute
{
    THostIndex SourceHostIndex = InvalidHostIndex;
    THostIndex DestinationHostIndex = InvalidHostIndex;

    bool operator==(const THostRoute& other) const = default;
    bool operator<(const THostRoute& other) const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
