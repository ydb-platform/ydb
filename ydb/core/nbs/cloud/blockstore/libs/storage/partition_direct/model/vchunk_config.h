#pragma once

#include "host_roles.h"

#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunkConfig
{
public:
    static TVChunkConfig
    MakeDefault(ui32 vChunkIndex, size_t hostCount, size_t primaryCount);

    static TVChunkConfig Make(
        ui32 vChunkIndex,
        THostRoles pbufferHosts,
        THostRoles ddiskHosts,
        THostMask enabledHosts,
        TVector<std::optional<ui64>> watermarks);

    [[nodiscard]] size_t GetHostCount() const;
    [[nodiscard]] ui32 GetVChunkIndex() const;

    // Enables the host to work. If the total count of ddisks is not enough to
    // reach the quorum, then can make a promotion for ddisk.
    void EnableHost(THostIndex hostIndex);
    // Temporarily disables the host. Does not change the ddisk status for the
    // host.
    void DisableHost(THostIndex hostIndex);

    // Disables the host. Demote ddisk and pbuffer. If possible, adds ddisk on
    // the new host. Returns the text of the error or message to be logged.
    TString EvacuateHost(THostIndex hostIndex);

    // Removes ddisk from the host. Demote pbuffer to handoff. Returns error
    // message or empty string when demote executed successfully.
    TString DemoteHost(THostIndex hostIndex);
    // Adds ddisk to the host.
    void PromoteHost(THostIndex hostIndex);

    [[nodiscard]] EHostRole GetPBufferRole(THostIndex hostIndex) const;
    [[nodiscard]] EHostRole GetDDiskRole(THostIndex hostIndex) const;

    // Get a list of desired PBuffers. They are located next to DDisk and their
    // use is most effective.
    [[nodiscard]] THostMask GetDesiredPBuffers() const;
    // Get a list of secondary PBuffers. They are not located near DDisk and
    // their use is not effective.
    [[nodiscard]] THostMask GetSecondaryPBuffers() const;
    // Get a list of temporarily offline PBuffers. They may not work, but they
    // need to be checked in order to be transferred online.
    [[nodiscard]] THostMask GetTemporaryOfflinePBuffers() const;

    // Get a list of all DDisks (enabled and disabled).
    [[nodiscard]] THostMask GetDDisks() const;
    // Get a list of all DDisks with full data (enabled or not).
    [[nodiscard]] THostMask GetFullDDisks() const;
    // Get a list of all healthy DDisks (enabled and full filed).
    [[nodiscard]] THostMask GetHealthyDDisks() const;

    void SetWatermark(THostIndex hostIndex, std::optional<ui64> watermark);
    [[nodiscard]] std::optional<ui64> GetWatermark(THostIndex hostIndex) const;

    [[nodiscard]] THostMask GetDisabledHosts() const;

    [[nodiscard]] bool IsValid() const;

    [[nodiscard]] TString DebugPrint() const;

private:
    size_t HostCount = 0;
    ui32 VChunkIndex = 0;
    THostRoles PBufferHosts;
    THostRoles DDiskHosts;
    THostMask EnabledHosts;
    TVector<std::optional<ui64>> Watermarks;
};

////////////////////////////////////////////////////////////////////////////////

// Vchunk index -> persisted config override. Vchunks without an entry fall
// back to TVChunkConfig::Make().
using TVChunkConfigByIndex = THashMap<ui32, TVChunkConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
