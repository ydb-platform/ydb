#pragma once

#include "host_mask.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EHostStatus: ui8
{
    Primary = 0,
    HandOff = 1,
    Disabled = 2,
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

    [[nodiscard]] THostMask GetPrimary() const;
    [[nodiscard]] THostMask GetHandOff() const;
    [[nodiscard]] THostMask GetActive() const;
    [[nodiscard]] THostMask GetDisabled() const;

    bool operator==(const THostStatusList& other) const = default;

    [[nodiscard]] TString Print() const;

private:
    TVector<EHostStatus> Statuses;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
