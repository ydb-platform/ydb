#include "vchunk_config.h"

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

THostMask GetDesired(const THostRoles& hosts, THostMask enabledHosts)
{
    THostMask result;
    for (THostIndex hostIndex = 0; hostIndex < hosts.HostCount(); ++hostIndex) {
        if (hosts.GetRole(hostIndex) == EHostRole::Primary &&
            enabledHosts.Get(hostIndex))
        {
            result.Set(hostIndex);
        }
    }
    return result;
}

}   // namespace

// static
TVChunkConfig
TVChunkConfig::Make(ui32 vChunkIndex, size_t hostCount, size_t primaryCount)
{
    return TVChunkConfig{
        .VChunkIndex = vChunkIndex,
        .PBufferHosts =
            THostRoles::MakeRotating(hostCount, vChunkIndex, primaryCount),
        .DDiskHosts =
            THostRoles::MakeRotating(hostCount, vChunkIndex, primaryCount),
        .EnabledHosts = THostMask::MakeAll(hostCount),
    };
}

void TVChunkConfig::EnableHost(THostIndex hostIndex)
{
    EnabledHosts.Set(hostIndex);
}

void TVChunkConfig::DisableHost(THostIndex hostIndex)
{
    EnabledHosts.Reset(hostIndex);
}

THostMask TVChunkConfig::GetDesiredPBuffers() const
{
    return GetDesired(PBufferHosts, EnabledHosts);
}

THostMask TVChunkConfig::GetDesiredDDisks() const
{
    return GetDesired(DDiskHosts, EnabledHosts);
}

THostMask TVChunkConfig::GetDisabledHosts() const
{
    return EnabledHosts.LogicalNot();
}

bool TVChunkConfig::IsValid() const
{
    if (PBufferHosts.HostCount() != DDiskHosts.HostCount()) {
        return false;
    }
    if (PBufferHosts.HostCount() == 0 ||
        PBufferHosts.HostCount() > MaxHostCount)
    {
        return false;
    }
    return !PBufferHosts.GetActive().Empty() && !DDiskHosts.GetActive().Empty();
}

TString TVChunkConfig::DebugPrint() const
{
    TStringBuilder result;
    result << "[" << VChunkIndex << "] PBuffer{" << PBufferHosts.DebugPrint()
           << "} DDisk{" << DDiskHosts.DebugPrint() << "} Enabled{";
    for (THostIndex hostIndex = 0; hostIndex < PBufferHosts.HostCount();
         ++hostIndex)
    {
        result << (EnabledHosts.Get(hostIndex) ? "+" : "-");
    }
    result << "}";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
