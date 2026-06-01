#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

THostMask
Filter(const THostRoles& hosts, THostMask enabledHosts, EHostRole role)
{
    THostMask result;
    for (THostIndex hostIndex = 0; hostIndex < hosts.HostCount(); ++hostIndex) {
        if (hosts.GetRole(hostIndex) == role && enabledHosts.Get(hostIndex)) {
            result.Set(hostIndex);
        }
    }
    return result;
}

THostIndex GetPrimaryCandidate(
    const THostRoles& ddiskHosts,
    const THostMask& enabledHosts)
{
    for (THostIndex indx: enabledHosts) {
        if (ddiskHosts.GetRole(indx) != EHostRole::Primary) {
            return indx;
        }
    }
    return InvalidHostIndex;
}

}   // namespace

// static
TVChunkConfig TVChunkConfig::MakeDefault(
    ui32 vChunkIndex,
    size_t hostCount,
    size_t primaryCount)
{
    TVChunkConfig result;
    result.HostCount = hostCount;
    result.VChunkIndex = vChunkIndex;
    result.PBufferHosts = THostRoles::MakeRotating(
        hostCount,
        vChunkIndex,
        primaryCount,
        EHostRole::HandOff);
    result.DDiskHosts = THostRoles::MakeRotating(
        hostCount,
        vChunkIndex,
        primaryCount,
        EHostRole::None);
    result.EnabledHosts = THostMask::MakeAll(hostCount);
    result.Watermarks = TVector<std::optional<ui64>>(hostCount);
    return result;
}

// static
TVChunkConfig TVChunkConfig::Make(
    ui32 vChunkIndex,
    THostRoles pbufferHosts,
    THostRoles ddiskHosts,
    THostMask enabledHosts,
    TVector<std::optional<ui64>> watermarks)
{
    TVChunkConfig result;
    result.HostCount = pbufferHosts.HostCount();
    result.VChunkIndex = vChunkIndex;
    result.PBufferHosts = pbufferHosts;
    result.DDiskHosts = ddiskHosts;
    result.EnabledHosts = enabledHosts;
    watermarks.resize(result.HostCount);
    result.Watermarks = std::move(watermarks);
    return result;
}

size_t TVChunkConfig::GetHostCount() const
{
    return HostCount;
}

ui32 TVChunkConfig::GetVChunkIndex() const
{
    return VChunkIndex;
}

void TVChunkConfig::EnableHost(THostIndex hostIndex)
{
    EnabledHosts.Set(hostIndex);
    const auto ddisks = GetDDisks();
    if (!ddisks.Get(hostIndex) &&
        ddisks.Count() < QuorumDirectBlockGroupHostCount)
    {
        DDiskHosts.SetRole(hostIndex, EHostRole::Primary);
        Watermarks[hostIndex] = 0;
    }
}

void TVChunkConfig::DisableHost(THostIndex hostIndex)
{
    EnabledHosts.Reset(hostIndex);
}

TString TVChunkConfig::EvacuateHost(THostIndex hostIndex)
{
    DisableHost(hostIndex);

    if (DDiskHosts.GetRole(hostIndex) == EHostRole::None ||
        DDiskHosts.GetRole(hostIndex) == EHostRole::HandOff)
    {
        return {};
    }

    TString error = DemoteHost(hostIndex);
    if (!error.empty()) {
        return error;
    }

    const THostIndex to = GetPrimaryCandidate(DDiskHosts, EnabledHosts);
    if (to == InvalidHostIndex) {
        return TStringBuilder() << "Can't find new primary candidate for "
                                << PrintHostIndex(hostIndex);
    }

    PromoteHost(to);
    Y_ABORT_UNLESS(EnabledHosts.Get(to) == true);

    return TStringBuilder()
           << "Host " << PrintHostIndex(hostIndex) << " demoted, host "
           << PrintHostIndex(to) << " promoted";
}

TString TVChunkConfig::DemoteHost(THostIndex hostIndex)
{
    const auto fullDDisks = GetFullDDisks();
    if (fullDDisks.Count() == 1 && fullDDisks.Get(hostIndex)) {
        return TStringBuilder() << "Can't demote last healthy ddisk "
                                << PrintHostIndex(hostIndex);
    }

    DDiskHosts.SetRole(hostIndex, EHostRole::None);
    PBufferHosts.SetRole(hostIndex, EHostRole::HandOff);
    Watermarks[hostIndex] = std::nullopt;
    return {};
}

void TVChunkConfig::PromoteHost(THostIndex hostIndex)
{
    PBufferHosts.SetRole(hostIndex, EHostRole::Primary);
    if (DDiskHosts.GetRole(hostIndex) != EHostRole::Primary) {
        DDiskHosts.SetRole(hostIndex, EHostRole::Primary);
        Watermarks[hostIndex] = 0;
    }
}

EHostRole TVChunkConfig::GetPBufferRole(THostIndex hostIndex) const
{
    return PBufferHosts.GetRole(hostIndex);
}

EHostRole TVChunkConfig::GetDDiskRole(THostIndex hostIndex) const
{
    return DDiskHosts.GetRole(hostIndex);
}

THostMask TVChunkConfig::GetDesiredPBuffers() const
{
    return Filter(PBufferHosts, EnabledHosts, EHostRole::Primary);
}

THostMask TVChunkConfig::GetSecondaryPBuffers() const
{
    return Filter(PBufferHosts, EnabledHosts, EHostRole::HandOff);
}

THostMask TVChunkConfig::GetTemporaryOfflinePBuffers() const
{
    THostMask result;
    for (size_t i = 0; i < HostCount; ++i) {
        if (!EnabledHosts.Get(i) && PBufferHosts.GetRole(i) != EHostRole::None)
        {
            result.Set(i);
        }
    }
    return result;
}

THostMask TVChunkConfig::GetDDisks() const
{
    return Filter(
        DDiskHosts,
        THostMask::MakeAll(HostCount),
        EHostRole::Primary);
}

THostMask TVChunkConfig::GetFullDDisks() const
{
    THostMask result;
    for (THostIndex hostIndex: GetDDisks()) {
        if (Watermarks[hostIndex] == std::nullopt) {
            result.Set(hostIndex);
        }
    }
    return result;
}

THostMask TVChunkConfig::GetDisabledHosts() const
{
    return EnabledHosts.LogicalNot();
}

THostMask TVChunkConfig::GetHealthyDDisks() const
{
    THostMask result;
    for (THostIndex hostIndex: EnabledHosts) {
        if (DDiskHosts.GetRole(hostIndex) == EHostRole::Primary &&
            Watermarks[hostIndex] == std::nullopt)
        {
            result.Set(hostIndex);
        }
    }
    return result;
}

void TVChunkConfig::SetWatermark(
    THostIndex hostIndex,
    std::optional<ui64> watermark)
{
    Watermarks[hostIndex] = watermark;
}

std::optional<ui64> TVChunkConfig::GetWatermark(THostIndex hostIndex) const
{
    return Watermarks[hostIndex];
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
        if (Watermarks[hostIndex] != std::nullopt) {
            result << "[" << *Watermarks[hostIndex] << "]";
        }
    }
    result << "}";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
