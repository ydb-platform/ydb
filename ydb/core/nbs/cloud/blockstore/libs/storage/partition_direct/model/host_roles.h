#pragma once

#include "host_mask.h"

#include <array>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class THostRoles
{
public:
    THostRoles() = default;
    explicit THostRoles(size_t hostCount);

    static THostRoles
    MakeRotating(size_t hostCount, ui32 vChunkIndex, size_t primaryCount);

    [[nodiscard]] size_t HostCount() const;
    [[nodiscard]] EHostRole GetRole(THostIndex host) const;
    void SetRole(THostIndex host, EHostRole assignment);

    [[nodiscard]] THostMask GetPrimary() const;
    [[nodiscard]] THostMask GetHandOff() const;
    [[nodiscard]] THostMask GetActive() const;

    bool operator==(const THostRoles& other) const = default;

    [[nodiscard]] TString DebugPrint() const;

private:
    std::array<EHostRole, MaxHostCount> Assignments{};
    size_t Count = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
