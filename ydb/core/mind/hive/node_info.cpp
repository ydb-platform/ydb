#include "hive_impl.h"
#include "hive_log.h"

namespace NKikimr {
namespace NHive {

const ui64 TNodeInfo::MAX_TABLET_COUNT_DEFAULT_VALUE = NKikimrLocal::TTabletAvailability().GetMaxCount();

TNodeInfo::TNodeInfo(TNodeId nodeId, THive& hive)
    : VolatileState(EVolatileState::Unknown)
    , Hive(hive)
    , Id(nodeId)
    , Down(false)
    , Freeze(false)
    , Drain(false)
    , ResourceValues()
    , ResourceTotalValues()
    , ResourceMaximumValues(GetResourceInitialMaximumValues())
    , StartTime(TInstant::MicroSeconds(0))
    , Location()
    , LocationAcquired(false)
{}

void TNodeInfo::ChangeVolatileState(EVolatileState state) {
    BLOG_W("Node(" << Id << ", " << ResourceValues << ") VolatileState: " << EVolatileStateName(VolatileState) << " -> " << EVolatileStateName(state));

    if (state == EVolatileState::Connected) {
        switch (VolatileState) {
        case EVolatileState::Unknown:
        case EVolatileState::Disconnected:
        case EVolatileState::Connecting:
            RegisterInDomains();
            break;

        default:
            break;
        };
    }

    if (state == EVolatileState::Disconnected) {
        switch (VolatileState) {
        case EVolatileState::Connected:
        case EVolatileState::Disconnecting:
            DeregisterInDomains();
            break;

        default:
            break;
        };
    }

    VolatileState = state;
}

bool TNodeInfo::OnTabletChangeVolatileState(TTabletInfo* tablet, TTabletInfo::EVolatileState newState) {
    if (Freeze) {
        tablet->PreferredNodeId = Id;
        FrozenTablets.push_back(tablet->GetFullTabletId());
    }
    TTabletInfo::EVolatileState oldState = tablet->GetVolatileState();
    if (IsResourceDrainingState(oldState)) {
        if (Tablets[oldState].erase(tablet) != 0) {
            UpdateResourceValues(tablet, tablet->GetResourceValues(), NKikimrTabletBase::TMetrics());
        } else {
            if (oldState != newState) {
                BLOG_W("Node(" << Id << ") could not delete tablet " << tablet->ToString() << " from state " << TTabletInfo::EVolatileStateName(oldState));
            }
        }
    }
    if (IsAliveState(oldState)) {
        TabletsRunningByType[tablet->GetTabletType()].erase(tablet);
        TabletsOfObject[tablet->GetObjectId()].erase(tablet);
        Hive.UpdateCounterTabletsAlive(-1);
        Hive.UpdateDomainTabletsAlive(tablet->GetLeader().ObjectDomain, -1, GetServicedDomain());
        if (tablet->HasCounter() && tablet->IsLeader()) {
            Hive.UpdateObjectCount(tablet->AsLeader(), *this, -1);
        }
    }
    if (IsResourceDrainingState(newState)) {
        if (Tablets[newState].insert(tablet).second) {
            UpdateResourceValues(tablet, NKikimrTabletBase::TMetrics(), tablet->GetResourceValues());
        } else {
            BLOG_W("Node(" << Id << ") could not insert tablet " << tablet->ToString() << " to state " << TTabletInfo::EVolatileStateName(newState));
        }
    }
    if (IsAliveState(newState)) {
        TabletsRunningByType[tablet->GetTabletType()].emplace(tablet);
        TabletsOfObject[tablet->GetObjectId()].emplace(tablet);
        Hive.UpdateCounterTabletsAlive(+1);
        Hive.UpdateDomainTabletsAlive(tablet->GetLeader().ObjectDomain, +1, GetServicedDomain());
        if (tablet->HasCounter() && tablet->IsLeader()) {
            Hive.UpdateObjectCount(tablet->AsLeader(), *this, +1);
        }
    }
    return true;
}

void TNodeInfo::UpdateResourceValues(const TTabletInfo* tablet, const NKikimrTabletBase::TMetrics& before, const NKikimrTabletBase::TMetrics& after) {
    TResourceRawValues delta = ResourceRawValuesFromMetrics(after) - ResourceRawValuesFromMetrics(before);
    auto oldResourceValues = ResourceValues;
    auto oldNormalizedValues = NormalizeRawValues(ResourceValues, ResourceMaximumValues);
    ResourceValues += delta;
    auto normalizedValues = NormalizeRawValues(ResourceValues, ResourceMaximumValues);
    BLOG_TRACE("Node(" << Id << ", " << oldResourceValues << "->" << ResourceValues << ")");
    Hive.UpdateTotalResourceValues(this, tablet, before, after, ResourceValues - oldResourceValues, normalizedValues - oldNormalizedValues);
}

bool TNodeInfo::MatchesFilter(const TNodeFilter& filter, TTabletDebugState* debugState) const {
    const auto& effectiveAllowedDomains = filter.GetEffectiveAllowedDomains();
    bool result = false;

    for (const auto& candidate : effectiveAllowedDomains) {
        if (Hive.DomainHasNodes(candidate)) {
            result = std::find(ServicedDomains.begin(),
                               ServicedDomains.end(),
                               candidate) != ServicedDomains.end();
            if (result) {
                break;
            }
        }
    }

    if (!result) {
        if (debugState) {
            debugState->NodesWithoutDomain++;
        }
        return false;
    }

    const auto& allowedNodes = filter.AllowedNodes;

    if (!allowedNodes.empty()
            && std::find(allowedNodes.begin(), allowedNodes.end(), Id) == allowedNodes.end()) {
        if (debugState) {
            debugState->NodesNotAllowed++;
        }
        return false;
    }

    const TVector<TDataCenterId>& allowedDataCenters = filter.AllowedDataCenters;

    if (!allowedDataCenters.empty()
            && std::find(
                allowedDataCenters.begin(),
                allowedDataCenters.end(),
                GetDataCenter()) == allowedDataCenters.end()) {
        if (debugState) {
            debugState->NodesInDatacentersNotAllowed++;
        }
        return false;
    }

    ui64 maxCount = GetMaxCountForTabletType(filter.TabletType);
    if (maxCount == 0) {
        return false;
    }

    return true;
}

bool TNodeInfo::IsAllowedToRunTablet(TTabletDebugState* debugState) const {
    if (Down) {
        if (debugState) {
            debugState->NodesDown++;
        }
        return false;
    }

    if (!LocationAcquired) {
        if (debugState) {
            debugState->NodesWithoutLocation++;
        }
        return false;
    }
    return true;
}

bool TNodeInfo::IsAllowedToRunTablet(const TTabletInfo& tablet, TTabletDebugState* debugState) const {
    if (!IsAllowedToRunTablet(debugState)) {
        return false;
    }

    if (!MatchesFilter(tablet.GetNodeFilter(), debugState)) {
        return false;
    }

    if (tablet.IsFollower() && tablet.AsFollower().FollowerGroup.LocalNodeOnly) {
        const TLeaderTabletInfo& leader = tablet.GetLeader();
        if (!leader.IsRunning()) {
            if (debugState) {
                debugState->LeaderNotRunning = true;
            }
            return false;
        }
        if (leader.NodeId != Id) {
            if (debugState) {
                debugState->NodesWithLeaderNotLocal++;
            }
            return false;
        }
    }

    return true;
}

i32 TNodeInfo::GetPriorityForTablet(const TTabletInfo& tablet) const {
    i32 priority = 0;

    auto it = TabletAvailability.find(tablet.GetTabletType());
    if (it != TabletAvailability.end()) {
        priority = it->second.FromLocal.GetPriority();
    }

    if (tablet.FailedNodeId == Id) {
        --priority;
    }

    return priority;
}

bool TNodeInfo::IsAbleToRunTablet(const TTabletInfo& tablet, TTabletDebugState* debugState) const {
    if (tablet.IsAliveOnLocal(Local)) {
        return !(IsOverloaded() && tablet.HasAllowedMetric(EResourceToBalance::ComputeResources));
    }
    if (tablet.IsLeader()) {
        const TLeaderTabletInfo& leader = tablet.AsLeader();
        if (leader.IsFollowerPromotableOnNode(Id)) {
            return true;
        }
    }
//            const TLeaderTabletInfo& leader = tablet.GetLeader();
//            if (!leader.Followers.empty()) {
//                if (leader.IsSomeoneAliveOnNode(Id)) {
//                    return false;
//                }
//            }
    if (tablet.IsFollower()) {
        const TFollowerTabletInfo& follower = tablet.AsFollower();
        const TFollowerGroup& followerGroup = follower.FollowerGroup;
        const TLeaderTabletInfo& leader = follower.LeaderTablet;
        if (followerGroup.RequireDifferentNodes) {
            if (leader.IsSomeoneAliveOnNode(Id)) {
                if (debugState) {
                    debugState->NodesWithSomeoneFromOurFamily++;
                }
                return false;
            }
        }
    }

    {
        auto tabletType = tablet.GetTabletType();
        ui64 maxCount = GetMaxCountForTabletType(tabletType);
        if (maxCount != MAX_TABLET_COUNT_DEFAULT_VALUE) {
            ui64 currentCount = GetTabletsRunningByType(tabletType);
            if (currentCount >= maxCount) {
                if (debugState) {
                    debugState->NodesWithoutResources++;
                }
                return false;
            }
        }
    }

    if (tablet.IsAlive() && IsOverloaded() && tablet.HasAllowedMetric(EResourceToBalance::ComputeResources)) {
        // we don't move already running tablet to another overloaded node
        if (debugState) {
            debugState->NodesWithoutResources++;
        }
        return false;
    }

    TResourceRawValues maximumResources = GetResourceMaximumValues() * Hive.GetResourceOvercommitment();
    TResourceRawValues allocatedResources = GetResourceCurrentValues() + tablet.GetResourceCurrentValues();
    auto cmp = piecewise_compare(allocatedResources, maximumResources);
    // only check memory because it's the only resource we can actually run out of
    if (std::get<NMetrics::EResource::Memory>(cmp) != std::partial_ordering::less) {
        if (debugState) {
            debugState->NodesWithoutResources++;
        }
        return false;
    }

    return true;
}

ui64 TNodeInfo::GetMaxTabletsScheduled() const {
    return Hive.GetMaxTabletsScheduled();
}

ui64 TNodeInfo::GetMaxCountForTabletType(TTabletTypes::EType tabletType) const {
    ui64 maxCount = 0;
    const TTabletAvailabilityInfo* availabilityInfo = nullptr;
    if (!TabletAvailability.empty()) {
        auto itTabletAvailability = TabletAvailability.find(tabletType);
        if (itTabletAvailability == TabletAvailability.end()) {
            return 0;
        } else {
            availabilityInfo = &itTabletAvailability->second;
            maxCount = availabilityInfo->EffectiveMaxCount;
        }
    }
    if (availabilityInfo && !availabilityInfo->IsSet) {
        const std::unordered_map<TTabletTypes::EType, NKikimrConfig::THiveTabletLimit>& tabletLimit = Hive.GetTabletLimit();
        auto itTabletLimit = tabletLimit.find(tabletType);
        if (itTabletLimit != tabletLimit.end()) {
            maxCount = itTabletLimit->second.GetMaxCount();
        }
    }
    return maxCount;
}

bool TNodeInfo::IsOverloaded() const {
    auto maxValues = GetResourceMaximumValues() * Hive.GetResourceOvercommitment();
    auto normValues = NormalizeRawValues(GetResourceCurrentValues(), maxValues);
    return GetNodeUsage(normValues) >= Hive.GetMaxNodeUsageToKick();
}

bool TNodeInfo::BecomeConnected() {
    if (VolatileState == EVolatileState::Connected) {
        return true;
    }
    if (VolatileState == EVolatileState::Connecting) {
        Y_ABORT_UNLESS((bool)Local);
        ChangeVolatileState(EVolatileState::Connected);
        StartTime = DEPRECATED_NOW;
        return true;
    } else {
        return false;
    }
}

void TNodeInfo::RegisterInDomains() {
    Hive.DomainsView.RegisterNode(*this);
}

void TNodeInfo::DeregisterInDomains() {
    Hive.DomainsView.DeregisterNode(*this);
    LastSeenServicedDomains = std::move(ServicedDomains); // clear ServicedDomains
}

void TNodeInfo::Ping() {
    Y_ABORT_UNLESS((bool)Local);
    BLOG_D("Node(" << Id << ") Ping(" << Local << ")");
    Hive.QueuePing(Local);
}

void TNodeInfo::SendReconnect(const TActorId& local) {
    BLOG_D("Node(" << Id << ") Reconnect(" << local << ")");
    Hive.SendReconnect(local);
}

void TNodeInfo::SetDown(bool down) {
    Down = down;
    if (Down) {
        Hive.ObjectDistributions.RemoveNode(*this);
    } else {
        Hive.ObjectDistributions.AddNode(*this);
        Hive.ProcessWaitQueue();
    }
}

void TNodeInfo::SetFreeze(bool freeze) {
    Freeze = freeze;
    if (Freeze) {
        for (const auto& [state, tablets] : Tablets) {
            FrozenTablets.reserve(FrozenTablets.size() + tablets.size());
            for (auto* tablet : tablets) {
                FrozenTablets.push_back(tablet->GetFullTabletId());
                tablet->PreferredNodeId = Id;
            }
        }
    } else {
        for (auto tabletId : FrozenTablets) {
            auto tablet = Hive.FindTablet(tabletId);
            if (tablet) {
                tablet->PreferredNodeId = 0;
            }
        }
        FrozenTablets.clear();
        Hive.ProcessWaitQueue();
    }
}

void TNodeInfo::UpdateResourceMaximum(const NKikimrTabletBase::TMetrics& metrics) {
    auto oldNormalizedValues = NormalizeRawValues(ResourceValues, ResourceMaximumValues);
    if (metrics.HasCPU()) {
        std::get<NMetrics::EResource::CPU>(ResourceMaximumValues) = metrics.GetCPU();
    }
    if (metrics.HasMemory()) {
        std::get<NMetrics::EResource::Memory>(ResourceMaximumValues) = metrics.GetMemory();
    }
    if (metrics.HasNetwork()) {
        std::get<NMetrics::EResource::Network>(ResourceMaximumValues) = metrics.GetNetwork();
    }
    auto normalizedValues = NormalizeRawValues(ResourceValues, ResourceMaximumValues);
    Hive.UpdateTotalResourceValues(nullptr, nullptr, NKikimrTabletBase::TMetrics(), NKikimrTabletBase::TMetrics(), {}, normalizedValues - oldNormalizedValues);
}

double TNodeInfo::GetNodeUsageForTablet(const TTabletInfo& tablet) const {
    // what it would like when tablet will run on this node?
    TResourceRawValues nodeValues = GetResourceCurrentValues();
    TResourceRawValues tabletValues = tablet.GetResourceCurrentValues();
    tablet.FilterRawValues(nodeValues);
    tablet.FilterRawValues(tabletValues);
    auto current = tablet.IsAliveOnLocal(Local) ? nodeValues : nodeValues + tabletValues;
    auto maximum = GetResourceMaximumValues();
    // basically, this is: return max(a / b);
    double usage = TTabletInfo::GetUsage(current, maximum);
    if (Hive.GetSpreadNeighbours() && usage < 1) {
        auto neighbours = GetTabletNeighboursCount(tablet);
        if (neighbours > 0) {
            auto remain = 1 - usage;
            auto cost = remain / (neighbours + 1);
            usage += cost * neighbours; // n / (n + 1)
        }
    }
    return usage;
}

double TNodeInfo::GetNodeUsage(const TResourceNormalizedValues& normValues, EResourceToBalance resource) const {
    double usage = TTabletInfo::ExtractResourceUsage(normValues, resource);
    if (resource == EResourceToBalance::ComputeResources && AveragedNodeTotalUsage.IsValueStable()) {
        usage = std::max(usage, AveragedNodeTotalUsage.GetValue());
    }
    return usage;
}

double TNodeInfo::GetNodeUsage(EResourceToBalance resource) const {
    auto normValues = NormalizeRawValues(GetResourceCurrentValues(), GetResourceMaximumValues());
    return GetNodeUsage(normValues, resource);
}

ui64 TNodeInfo::GetTabletsRunningByType(TTabletTypes::EType tabletType) const {
    auto itRunningByType = TabletsRunningByType.find(tabletType);
    if (itRunningByType != TabletsRunningByType.end()) {
        return itRunningByType->second.size();
    }
    return 0;
}

TResourceRawValues TNodeInfo::GetResourceInitialMaximumValues() {
    return Hive.GetResourceInitialMaximumValues();
}

TResourceRawValues TNodeInfo::GetStDevResourceValues() {
    TVector<TResourceRawValues> values;
    const std::unordered_set<TTabletInfo*>& runningTablets = Tablets[TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_RUNNING];
    values.reserve(runningTablets.size());
    for (const TTabletInfo* tablet : runningTablets) {
        values.push_back(tablet->GetResourceCurrentValues());
    }
    return GetStDev(values);
}

bool TNodeInfo::CanBeDeleted() const {
    TInstant lastAlive(TInstant::MilliSeconds(Statistics.GetLastAliveTimestamp()));
    if (lastAlive) {
        return (IsDisconnected() || IsUnknown())
                && !Local
                && GetTabletsTotal() == 0
                && LockedTablets.empty()
                && !Freeze
                && (lastAlive + Hive.GetNodeDeletePeriod() < TInstant::Now());
    } else {
        return (IsDisconnected() || IsUnknown()) && !Local && GetTabletsTotal() == 0 && LockedTablets.empty() && !Freeze;
    }
}

void TNodeInfo::UpdateResourceTotalUsage(const NKikimrHive::TEvTabletMetrics& metrics) {
    if (metrics.HasTotalResourceUsage()) {
        AveragedResourceTotalValues.Push(ResourceRawValuesFromMetrics(metrics.GetTotalResourceUsage()));
        ResourceTotalValues = AveragedResourceTotalValues.GetValue();
    }
    if (metrics.HasTotalNodeUsage()) {
        AveragedNodeTotalUsage.Push(metrics.GetTotalNodeUsage());
        NodeTotalUsage = AveragedNodeTotalUsage.GetValue();
    }
    if (metrics.HasTotalNodeCpuUsage()) {
        AveragedNodeTotalCpuUsage.Push(metrics.GetTotalNodeCpuUsage());
    }
}

TResourceRawValues TNodeInfo::GetResourceCurrentValues() const {
    if (AveragedResourceTotalValues.IsValueStable()) {
        return piecewise_max(ResourceValues, ResourceTotalValues);
    } else {
        return ResourceValues;
    }
}

void TNodeInfo::ActualizeNodeStatistics(TInstant now) {
    TInstant barierTime = now - Hive.GetNodeRestartWatchPeriod();
    Hive.ActualizeRestartStatistics(*Statistics.MutableRestartTimestamp(), barierTime.MilliSeconds());
}

ui64 TNodeInfo::GetRestartsPerPeriod(TInstant barrier) const {
    return Hive.GetRestartsPerPeriod(Statistics.GetRestartTimestamp(), barrier.MilliSeconds());
}

TString TNodeInfo::GetLogPrefix() const {
    return Hive.GetLogPrefix();
}

} // NHive
} // NKikimr
