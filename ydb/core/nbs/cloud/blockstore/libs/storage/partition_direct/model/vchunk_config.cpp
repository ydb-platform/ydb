#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

<<<<<<< HEAD
=======
TVChunkConfig::EHostHumanReadableState CalcHostHumanReadableState(
    EHostRole ddisk,
    EHostRole pbuffer,
    bool enabled,
    bool fresh)
{
    Y_ABORT_UNLESS(ddisk != EHostRole::HandOff);

    if (ddisk == EHostRole::Primary && enabled) {
        Y_ABORT_UNLESS(pbuffer == EHostRole::Primary);
        return fresh ? TVChunkConfig::EHostHumanReadableState::Fresh
                     : TVChunkConfig::EHostHumanReadableState::Primary;
    }
    if (ddisk == EHostRole::Primary && !enabled) {
        Y_ABORT_UNLESS(pbuffer == EHostRole::Primary);
        return TVChunkConfig::EHostHumanReadableState::Rotten;
    }

    Y_ABORT_UNLESS(pbuffer != EHostRole::Primary);

    if (pbuffer == EHostRole::HandOff) {
        return enabled ? TVChunkConfig::EHostHumanReadableState::HandOff
                       : TVChunkConfig::EHostHumanReadableState::Disabled;
    }
    return TVChunkConfig::EHostHumanReadableState::Demoted;
}

>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
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
    return result;
}

// static
TVChunkConfig TVChunkConfig::Make(
    ui32 vChunkIndex,
    THostRoles pbufferHosts,
    THostRoles ddiskHosts,
    THostMask enabledHosts)
{
    TVChunkConfig result;
    result.HostCount = pbufferHosts.HostCount();
    result.VChunkIndex = vChunkIndex;
    result.PBufferHosts = pbufferHosts;
    result.DDiskHosts = ddiskHosts;
    result.EnabledHosts = enabledHosts;
    return result;
}

<<<<<<< HEAD
=======
TVChunkConfig::EHostHumanReadableState TVChunkConfig::GetHostHumanReadableState(
    THostIndex hostIndex,
    bool fresh) const
{
    return CalcHostHumanReadableState(
        DDiskHosts.GetRole(hostIndex),
        PBufferHosts.GetRole(hostIndex),
        EnabledHosts.Get(hostIndex),
        fresh);
}

bool TVChunkConfig::Empty() const
{
    return HostCount == 0;
}

>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
size_t TVChunkConfig::GetHostCount() const
{
    return HostCount;
}

ui32 TVChunkConfig::GetVChunkIndex() const
{
    return VChunkIndex;
}

void TVChunkConfig::SetDBGIndex(ui32 dbgIndex)
{
    DBGIndex = dbgIndex;
}

ui32 TVChunkConfig::GetDBGIndex() const
{
    return DBGIndex;
}

void TVChunkConfig::EnableHost(THostIndex hostIndex)
{
    EnabledHosts.Set(hostIndex);
    const auto ddisks = GetDDisks();
    if (!ddisks.Get(hostIndex) &&
        ddisks.Count() < QuorumDirectBlockGroupHostCount)
    {
        DDiskHosts.SetRole(hostIndex, EHostRole::Primary);
    }
}

void TVChunkConfig::DisableHost(THostIndex hostIndex)
{
    EnabledHosts.Reset(hostIndex);
}

void TVChunkConfig::AppendHost()
{
    Y_ABORT_UNLESS(PBufferHosts.HostCount() == DDiskHosts.HostCount());
    const auto newHostIndex = static_cast<THostIndex>(HostCount);

<<<<<<< HEAD
    PBufferHosts.AppendRole(EHostRole::None);
    DDiskHosts.AppendRole(EHostRole::None);
    EnabledHosts.Reset(newHostIndex);
    Watermarks.push_back(std::nullopt);
=======
    if (ddiskCount < QuorumDirectBlockGroupHostCount) {
        PBufferHosts.AppendRole(EHostRole::Primary);
        DDiskHosts.AppendRole(EHostRole::Primary);
    } else {
        PBufferHosts.AppendRole(EHostRole::HandOff);
        DDiskHosts.AppendRole(EHostRole::None);
    }

    EnabledHosts.Set(newHostIndex);
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    ++HostCount;
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

    return TStringBuilder() << PrintHostIndex(hostIndex) << " demoted, "
                            << PrintHostIndex(to) << " promoted";
}

TString TVChunkConfig::DemoteHost(THostIndex hostIndex)
{
    const auto ddisks = GetDDisks();
    if (ddisks.Count() == 1 && ddisks.Get(hostIndex)) {
        return TStringBuilder()
               << "Can't demote last ddisk " << PrintHostIndex(hostIndex);
    }

    DDiskHosts.SetRole(hostIndex, EHostRole::None);
    PBufferHosts.SetRole(hostIndex, EHostRole::HandOff);
    return {};
}

void TVChunkConfig::PromoteHost(THostIndex hostIndex)
{
    PBufferHosts.SetRole(hostIndex, EHostRole::Primary);
    if (DDiskHosts.GetRole(hostIndex) != EHostRole::Primary) {
        DDiskHosts.SetRole(hostIndex, EHostRole::Primary);
    }
}

<<<<<<< HEAD
<<<<<<< HEAD
=======
TString TVChunkConfig::PromoteHostIfNeeded(bool fresh)
=======
TString TVChunkConfig::PromoteHostIfNeeded()
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
{
    TStringBuilder result;
    auto enabledDDisks = GetEnabledDDisks();
    if (enabledDDisks.Count() >= QuorumDirectBlockGroupHostCount) {
        result << "Enabled DDisks already enough " << DebugPrint();
        return result;
    }
    const THostIndex hostToPromote =
        GetPrimaryCandidate(DDiskHosts, EnabledHosts);
    if (hostToPromote == InvalidHostIndex) {
        result << "Can't find primary candidate " << DebugPrint();
        return result;
    }

    result << "Promote " << PrintHostIndex(hostToPromote) << " "
           << DebugPrint();
    PromoteHost(hostToPromote);
    return result;
}

>>>>>>> e33198cc8e8 ([NBS-7709] Persist VChunk touching (#53346))
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
    THostMask result = Filter(PBufferHosts, EnabledHosts, EHostRole::Primary);
    if (result.Count() >= QuorumDirectBlockGroupHostCount) {
        return result;
    }

    // Add hand-off hosts if primary is not enough for a quorum.
    for (auto host: GetSecondaryPBuffers()) {
        result.Set(host);
        if (result.Count() >= QuorumDirectBlockGroupHostCount) {
            break;
        }
    }

    return result;
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

<<<<<<< HEAD
THostMask TVChunkConfig::GetFullDDisks() const
{
    THostMask result;
    for (THostIndex hostIndex: GetDDisks()) {
        if (Watermarks[hostIndex] == std::nullopt) {
            result.Set(hostIndex);
        }
    }
    return result;
=======
THostMask TVChunkConfig::GetEnabledDDisks() const
{
    return Filter(DDiskHosts, EnabledHosts, EHostRole::Primary);
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
}

THostMask TVChunkConfig::GetDisabledHosts() const
{
    return EnabledHosts.LogicalNot();
}

<<<<<<< HEAD
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

=======
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
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
    result << "[" << DBGIndex << "/" << VChunkIndex << "] PBuffer{"
           << PBufferHosts.DebugPrint() << "} DDisk{" << DDiskHosts.DebugPrint()
           << "} Enabled{";
    for (THostIndex hostIndex = 0; hostIndex < PBufferHosts.HostCount();
         ++hostIndex)
    {
        result << (EnabledHosts.Get(hostIndex) ? "+" : "-");
        if (Watermarks[hostIndex] != std::nullopt) {
            result << "[" << *Watermarks[hostIndex] << "]";
        }
<<<<<<< HEAD
=======
        const auto state = GetHostHumanReadableState(i, false);
        result << Print(state, false);
>>>>>>> f52f47482ee ([NBS-7655] Remove watermarks and ahead state (#53653))
    }
    result << "}";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
