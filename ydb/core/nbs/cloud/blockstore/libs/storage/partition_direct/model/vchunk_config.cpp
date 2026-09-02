#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

namespace {

TVChunkConfig::EHostHumanReadableState CalcHostHumanReadableState(
    EHostRole ddisk,
    EHostRole pbuffer,
    bool enabled,
    std::optional<ui64> watermark)
{
    Y_ABORT_UNLESS(ddisk != EHostRole::HandOff);

    if (ddisk == EHostRole::Primary && enabled) {
        Y_ABORT_UNLESS(pbuffer == EHostRole::Primary);
        return watermark.has_value()
                   ? TVChunkConfig::EHostHumanReadableState::Fresh
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

TVChunkConfig::EHostHumanReadableState TVChunkConfig::GetHostHumanReadableState(
    THostIndex hostIndex) const
{
    return CalcHostHumanReadableState(
        DDiskHosts.GetRole(hostIndex),
        PBufferHosts.GetRole(hostIndex),
        EnabledHosts.Get(hostIndex),
        Watermarks[hostIndex]);
}

bool TVChunkConfig::Empty() const
{
    return HostCount == 0;
}

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
        Watermarks[hostIndex] = 0;
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
    const size_t ddiskCount = GetDDisks().Count();

    if (ddiskCount < QuorumDirectBlockGroupHostCount) {
        PBufferHosts.AppendRole(EHostRole::Primary);
        DDiskHosts.AppendRole(EHostRole::Primary);
        Watermarks.push_back(0);
    } else {
        PBufferHosts.AppendRole(EHostRole::HandOff);
        DDiskHosts.AppendRole(EHostRole::None);
        Watermarks.push_back(std::nullopt);
    }

    EnabledHosts.Set(newHostIndex);
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

TString TVChunkConfig::PromoteHostIfNeeded()
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

THostMask TVChunkConfig::GetEnabledDDisks() const
{
    return Filter(DDiskHosts, EnabledHosts, EHostRole::Primary);
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
    return EnabledHosts.LogicalNot().LogicalAnd(THostMask::MakeAll(HostCount));
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
    std::optional<ui64> watermarkBlockCount)
{
    Watermarks[hostIndex] = watermarkBlockCount;
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

bool TVChunkConfig::operator==(const TVChunkConfig& other) const = default;

TString TVChunkConfig::DebugPrint() const
{
    TStringBuilder result;

    result << "[" << PrintDbgId(DBGIndex);
    result << "/" << PrintVChunkId(VChunkIndex) << "]";

    result << "{";
    for (size_t i = 0; i < HostCount; ++i) {
        if (i) {
            result << ",";
        }
        const auto state = GetHostHumanReadableState(i);
        result << Print(state, false);
    }
    result << "}";

    return result;
}

TString Print(TVChunkConfig::EHostHumanReadableState state, bool brief)
{
    if (!brief) {
        return ToString(state);
    }

    switch (state) {
        case TVChunkConfig::EHostHumanReadableState::Primary:
            return "P";
        case TVChunkConfig::EHostHumanReadableState::Fresh:
            return "F";
        case TVChunkConfig::EHostHumanReadableState::HandOff:
            return "H";
        case TVChunkConfig::EHostHumanReadableState::Rotten:
            return "R";
        case TVChunkConfig::EHostHumanReadableState::Disabled:
            return "-";
        case TVChunkConfig::EHostHumanReadableState::Demoted:
            return "_";
    }
    return "?";
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
