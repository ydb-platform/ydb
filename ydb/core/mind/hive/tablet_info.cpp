#include "hive_impl.h"
#include "hive_log.h"
#include "tablet_info.h"
#include "node_info.h"
#include "leader_tablet_info.h"
#include "follower_tablet_info.h"

namespace NKikimr {
namespace NHive {

TTabletInfo::TTabletInfo(ETabletRole role, THive& hive)
    : VolatileState(EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN)
    , TabletRole(role)
    , VolatileStateChangeTime()
    , LastBalancerDecisionTime()
    , Hive(hive)
    , PreferredNodeId(0)
    , NodeId(0)
    , Node(nullptr)
    , LastNodeId(0)
    , ResourceValues()
    , ResourceMetricsAggregates(Hive.GetDefaultResourceMetricsAggregates())
    , Weight(0)
    , BalancerPolicy(EBalancerPolicy::POLICY_BALANCE)
{}

const TLeaderTabletInfo& TTabletInfo::GetLeader() const {
    if (IsLeader()) {
        return AsLeader();
    } else {
        return AsFollower().LeaderTablet;
    }
}

TLeaderTabletInfo& TTabletInfo::GetLeader() {
    if (IsLeader()) {
        return AsLeader();
    } else {
        return AsFollower().LeaderTablet;
    }
}

TLeaderTabletInfo& TTabletInfo::AsLeader() {
    Y_ABORT_UNLESS(TabletRole == ETabletRole::Leader);
    return static_cast<TLeaderTabletInfo&>(*this);
}

const TLeaderTabletInfo& TTabletInfo::AsLeader() const {
    Y_ABORT_UNLESS(TabletRole == ETabletRole::Leader);
    return static_cast<const TLeaderTabletInfo&>(*this);
}

TFollowerTabletInfo& TTabletInfo::AsFollower() {
    Y_ABORT_UNLESS(TabletRole == ETabletRole::Follower);
    return static_cast<TFollowerTabletInfo&>(*this);
}

const TFollowerTabletInfo& TTabletInfo::AsFollower() const {
    Y_ABORT_UNLESS(TabletRole == ETabletRole::Follower);
    return static_cast<const TFollowerTabletInfo&>(*this);
}

std::pair<TTabletId, TFollowerId> TTabletInfo::GetFullTabletId() const {
    if (IsLeader()) {
        return { GetLeader().Id, 0 };
    } else {
        return { GetLeader().Id, AsFollower().Id };
    }
}

TFullObjectId TTabletInfo::GetObjectId() const {
    return GetLeader().ObjectId;
}

TTabletTypes::EType TTabletInfo::GetTabletType() const {
    return GetLeader().Type;
}

TString TTabletInfo::ToString() const {
    const TLeaderTabletInfo& leader = GetLeader();
    TStringBuilder str;
    str << TTabletTypes::TypeToStr(leader.Type) << '.' << leader.Id << '.' << ETabletRoleName(TabletRole);
    if (IsFollower()) {
        const TFollowerTabletInfo& follower(AsFollower());
        str << '.' << follower.Id;
    } else {
        str << '.' << leader.KnownGeneration;
    }
    return str;
}

TString TTabletInfo::StateString() const {
    TStringBuilder state;
    state << EVolatileStateName(VolatileState);
    if (Node != nullptr) {
        state << " on node " << Node->Id;
    }
    return state;
}

TString TTabletInfo::FamilyString() const {
    TStringBuilder family;
    const TLeaderTabletInfo& leader = GetLeader();
    family << '{';
    family << leader.ToString() << ' ' << leader.StateString();
    for (const TFollowerTabletInfo& follower : leader.Followers) {
        family << ", " << follower.ToString() << ' ' << follower.StateString();
    }
    family << '}';
    return family;
}

void TTabletInfo::ChangeVolatileState(EVolatileState state) {
    if (VolatileState == state) {
        if (Node != nullptr) {
            BLOG_D("Tablet(" << ToString() << ") VolatileState: " << EVolatileStateName(VolatileState) << " (Node " << Node->Id << ")");
        } else {
            BLOG_D("Tablet(" << ToString() << ") VolatileState: " << EVolatileStateName(VolatileState));
        }
    } else {
        if (Node != nullptr) {
            BLOG_D("Tablet(" << ToString() << ") VolatileState: " << EVolatileStateName(VolatileState) << " -> " << EVolatileStateName(state) << " (Node " << Node->Id << ")");
        } else {
            BLOG_D("Tablet(" << ToString() << ") VolatileState: " << EVolatileStateName(VolatileState) << " -> " << EVolatileStateName(state));
        }
    }
    if (Node != nullptr) {
        Node->OnTabletChangeVolatileState(this, state);
    }
    VolatileState = state;
    VolatileStateChangeTime = TActivationContext::Now();
}

bool TTabletInfo::IsReadyToStart(TInstant now) const {
    if (IsFollower()) {
        if (!GetLeader().IsRunning()) {
            return false;
        }
    }
    return NodeId == 0 && VolatileState == EVolatileState::TABLET_VOLATILE_STATE_BOOTING && now >= PostponedStart;
}

bool TTabletInfo::IsStarting() const {
    return NodeId == 0 && VolatileState == EVolatileState::TABLET_VOLATILE_STATE_STARTING;
}

bool TTabletInfo::IsStartingOnNode(TNodeId nodeId) const {
    return IsStarting() && Node != nullptr && Node->Id == nodeId;
}

bool TTabletInfo::IsRunning() const {
    return Node != nullptr && VolatileState == EVolatileState::TABLET_VOLATILE_STATE_RUNNING;
}

bool TTabletInfo::IsBooting() const {
    return VolatileState == EVolatileState::TABLET_VOLATILE_STATE_BOOTING;
}

bool TTabletInfo::IsAlive() const {
    return Node != nullptr &&
            (VolatileState == EVolatileState::TABLET_VOLATILE_STATE_STARTING
             || VolatileState == EVolatileState::TABLET_VOLATILE_STATE_RUNNING);
}

bool TTabletInfo::CanBeAlive() const {
    return Node != nullptr &&
            (VolatileState == EVolatileState::TABLET_VOLATILE_STATE_STARTING
             || VolatileState == EVolatileState::TABLET_VOLATILE_STATE_RUNNING
             || VolatileState == EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN); // KIKIMR-12558
}

bool TTabletInfo::IsAliveOnLocal(const TActorId& local) const {
    return Node != nullptr
            && Node->Local == local
            && (VolatileState == EVolatileState::TABLET_VOLATILE_STATE_STARTING
                || VolatileState == EVolatileState::TABLET_VOLATILE_STATE_RUNNING
                || VolatileState == EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN); // KIKIMR-3712
}

bool TTabletInfo::IsStopped() const {
    return VolatileState == EVolatileState::TABLET_VOLATILE_STATE_STOPPED;
}

bool TTabletInfo::IsGoodForBalancer(TInstant now) const {
    return (BalancerPolicy == EBalancerPolicy::POLICY_BALANCE)
            && !Hive.IsInBalancerIgnoreList(GetTabletType())
            && (now - LastBalancerDecisionTime > Hive.GetTabletKickCooldownPeriod());
}

bool TTabletInfo::InitiateBoot() {
    if (IsStopped()) {
        ChangeVolatileState(EVolatileState::TABLET_VOLATILE_STATE_BOOTING);
        Hive.AddToBootQueue(this);
        Hive.ProcessBootQueue();
        return true;
    } else {
        return false;
    }
}

TActorId TTabletInfo::GetLocal() const {
    TActorId local;
    TNodeInfo* node = GetNode();
    if (node != nullptr) {
        local = node->Local;
    }
    return local;
}

TNodeInfo* TTabletInfo::GetNode() const {
    TNodeInfo* node = Node;
    if (node == nullptr && NodeId != 0) {
        node = Hive.FindNode(NodeId);
    }
    return node;
}

bool TTabletInfo::InitiateStop(TSideEffects& sideEffects) {
    TNodeInfo* node = GetNode();
    TActorId local;
    if (node != nullptr) {
        local = node->Local;
    }
    if (BecomeStopped()) {
        if (Hive.GetEnableFastTabletMove() && node != nullptr && !node->Freeze && PreferredNodeId != 0) {
            // we only do it when we have PreferredNodeId, which means that we are moving from one node to another
            LastNodeId = node->Id;
        } else {
            SendStopTablet(local, sideEffects);
            LastNodeId = 0;
        }
        if (IsLeader()) {
            for (TFollowerTabletInfo& follower : AsLeader().Followers) {
                if (follower.FollowerGroup.LocalNodeOnly) {
                    follower.InitiateStop(sideEffects);
                }
            }
        }
        return true;
    } else {
        return false;
    }
}

bool TTabletInfo::BecomeStarting(TNodeId nodeId) {
    if (VolatileState != EVolatileState::TABLET_VOLATILE_STATE_STARTING) {
        Node = Hive.FindNode(nodeId);
        Y_ABORT_UNLESS(Node != nullptr);
        ChangeVolatileState(EVolatileState::TABLET_VOLATILE_STATE_STARTING);
        return true;
    }
    return false;
}

bool TTabletInfo::BecomeRunning(TNodeId nodeId) {
    if (VolatileState != EVolatileState::TABLET_VOLATILE_STATE_RUNNING || NodeId != nodeId || (Node != nullptr && Node->Id != nodeId)) {
        NodeId = nodeId;
        PreferredNodeId = 0;
        Y_ABORT_UNLESS(NodeId != 0);
        if (Node == nullptr) {
            Node = Hive.FindNode(NodeId);
            Y_ABORT_UNLESS(Node != nullptr);
        } else if (Node->Id != NodeId) {
            ChangeVolatileState(EVolatileState::TABLET_VOLATILE_STATE_STOPPED);
            Node = Hive.FindNode(NodeId);
            Y_ABORT_UNLESS(Node != nullptr);
        }
        ChangeVolatileState(EVolatileState::TABLET_VOLATILE_STATE_RUNNING);
        return true;
    }
    return false;
}

bool TTabletInfo::BecomeStopped() {
    if (VolatileState != EVolatileState::TABLET_VOLATILE_STATE_STOPPED) {
        if (Node == nullptr && NodeId != 0) {
            Node = Hive.FindNode(NodeId);
            Y_ABORT_UNLESS(Node != nullptr);
        }
        ChangeVolatileState(EVolatileState::TABLET_VOLATILE_STATE_STOPPED);
        BootState.clear();
        if (Node != nullptr && Node->Freeze) {
            PreferredNodeId = Node->Id;
        }
        NodeId = 0;
        Node = nullptr;
        return true;
    } else {
        return false;
    }
}

void TTabletInfo::BecomeUnknown(TNodeInfo* node) {
    Y_ABORT_UNLESS(VolatileState == EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN);
    Y_ABORT_UNLESS(Node == nullptr || node == Node);
    Node = node;
    if (Node->Freeze) {
        PreferredNodeId = Node->Id;
    }
    ChangeVolatileState(EVolatileState::TABLET_VOLATILE_STATE_UNKNOWN);
}

bool TTabletInfo::Kick() {
    if (IsAlive()) {
        Hive.KickTablet(*this);
        return true;
    } else {
        return false;
    }
}

const TVector<i64>& TTabletInfo::GetTabletAllowedMetricIds() const {
    return Hive.GetTabletTypeAllowedMetricIds(GetLeader().Type);
}

bool TTabletInfo::HasAllowedMetric(const TVector<i64>& allowedMetricIds, EResourceToBalance resource) {
    switch (resource) {
        case EResourceToBalance::ComputeResources: { 
            auto isComputeMetric = [](i64 metricId) {
                return metricId == NKikimrTabletBase::TMetrics::kCPUFieldNumber ||
                       metricId == NKikimrTabletBase::TMetrics::kMemoryFieldNumber ||
                       metricId == NKikimrTabletBase::TMetrics::kNetworkFieldNumber;
            };
            return AnyOf(allowedMetricIds.begin(), allowedMetricIds.end(), isComputeMetric);
        }
        case EResourceToBalance::Counter:
            return true;
        case EResourceToBalance::CPU:
            return Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kCPUFieldNumber) != allowedMetricIds.end();
        case EResourceToBalance::Memory:
            return Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kMemoryFieldNumber) != allowedMetricIds.end();
        case EResourceToBalance::Network:
            return Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kNetworkFieldNumber) != allowedMetricIds.end();
    }
}

bool TTabletInfo::HasAllowedMetric(EResourceToBalance resource) const {
    return HasAllowedMetric(GetTabletAllowedMetricIds(), resource);
}

void TTabletInfo::UpdateResourceUsage(const NKikimrTabletBase::TMetrics& metrics) {
    TInstant now = TActivationContext::Now();
    const TVector<i64>& allowedMetricIds(GetTabletAllowedMetricIds());
    auto before = ResourceValues;
    auto maximum = GetResourceMaximumValues();
    if (HasAllowedMetric(allowedMetricIds, EResourceToBalance::CPU)) {
        if (metrics.HasCPU()) {
            if (metrics.GetCPU() > static_cast<ui64>(std::get<NMetrics::EResource::CPU>(maximum))) {
                BLOG_W("Ignoring too high CPU metric (" << metrics.GetCPU() << ") for tablet " << ToString());
            } else {
                ResourceMetricsAggregates.MaximumCPU.SetValue(metrics.GetCPU(), now);
            }
        } else {
            ResourceMetricsAggregates.MaximumCPU.AdvanceTime(now);
        }
        ResourceValues.SetCPU(ResourceMetricsAggregates.MaximumCPU.GetValue());
    }
    if (HasAllowedMetric(allowedMetricIds, EResourceToBalance::Memory)) {
        if (metrics.HasMemory()) {
            if (metrics.GetMemory() > static_cast<ui64>(std::get<NMetrics::EResource::Memory>(maximum))) {
                BLOG_W("Ignoring too high Memory metric (" << metrics.GetMemory() << ") for tablet " << ToString());
            } else {
                ResourceMetricsAggregates.MaximumMemory.SetValue(metrics.GetMemory(), now);
            }
        } else {
            ResourceMetricsAggregates.MaximumMemory.AdvanceTime(now);
        }
        ResourceValues.SetMemory(ResourceMetricsAggregates.MaximumMemory.GetValue());
    }
    if (HasAllowedMetric(allowedMetricIds, EResourceToBalance::Network)) {
        if (metrics.HasNetwork()) {
            if (metrics.GetNetwork() > static_cast<ui64>(std::get<NMetrics::EResource::Network>(maximum))) {
                BLOG_W("Ignoring too high Network metric (" << metrics.GetNetwork() << ") for tablet " << ToString());
            } else {
                ResourceMetricsAggregates.MaximumNetwork.SetValue(metrics.GetNetwork(), now);
            }
        } else {
            ResourceMetricsAggregates.MaximumNetwork.AdvanceTime(now);
        }
        ResourceValues.SetNetwork(ResourceMetricsAggregates.MaximumNetwork.GetValue());
    }
    if (metrics.HasStorage()) {
        ResourceValues.SetStorage(metrics.GetStorage());
    }
    if (metrics.HasReadThroughput()) {
        ResourceValues.SetReadThroughput(metrics.GetReadThroughput());
    }
    if (metrics.HasWriteThroughput()) {
        ResourceValues.SetWriteThroughput(metrics.GetWriteThroughput());
    }
    if (metrics.GroupReadThroughputSize() > 0) {
        ResourceValues.ClearGroupReadThroughput();
        for (const auto& v : metrics.GetGroupReadThroughput()) {
            ResourceValues.AddGroupReadThroughput()->CopyFrom(v);
        }
    }
    if (metrics.GroupWriteThroughputSize() > 0) {
        ResourceValues.ClearGroupWriteThroughput();
        for (const auto& v : metrics.GetGroupWriteThroughput()) {
            ResourceValues.AddGroupWriteThroughput()->CopyFrom(v);
        }
    }
    i64 counterBefore = ResourceValues.GetCounter();
    ActualizeCounter();
    i64 counterAfter = ResourceValues.GetCounter();
    const auto& after = ResourceValues;
    if (Node != nullptr) {
        if (IsResourceDrainingState(VolatileState)) {
            Node->UpdateResourceValues(this, before, after);
        }
        if (IsAliveState(VolatileState)) {
            i64 deltaCounter = counterAfter - counterBefore;
            if (deltaCounter != 0 && IsLeader()) {
                Hive.UpdateObjectCount(AsLeader(), *Node, deltaCounter);
            }
        }
    }
}

TResourceRawValues TTabletInfo::GetResourceCurrentValues() const {
    return ResourceRawValuesFromMetrics(ResourceValues);
}

TResourceRawValues TTabletInfo::GetResourceMaximumValues() const {
    if (Node != nullptr) {
        return Node->GetResourceMaximumValues();
    } else {
        return Hive.GetResourceInitialMaximumValues();
    }
}

i64 TTabletInfo::GetCounterValue() const {
    const auto& allowedMetricIds = GetTabletAllowedMetricIds();
    if (Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kCPUFieldNumber) != allowedMetricIds.end()
        && (ResourceMetricsAggregates.MaximumCPU.GetAllTimeMaximum() > 0
        || ResourceValues.GetCPU() > 0)) {
        return 0;
    }
    if (Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kMemoryFieldNumber) != allowedMetricIds.end()
        && (ResourceMetricsAggregates.MaximumMemory.GetAllTimeMaximum() > 0
        || ResourceValues.GetMemory() > 0)) {
        return 0;
    }
    if (Find(allowedMetricIds, NKikimrTabletBase::TMetrics::kNetworkFieldNumber) != allowedMetricIds.end()
        && (ResourceMetricsAggregates.MaximumNetwork.GetAllTimeMaximum() > 0
        || ResourceValues.GetNetwork() > 0)) {
        return 0;
    }
    return 1;
}

void TTabletInfo::FilterRawValues(TResourceRawValues& values) const {
    const NKikimrTabletBase::TMetrics& metrics(ResourceValues);
    const TVector<i64>& allowedMetricIds = GetTabletAllowedMetricIds();
    if (metrics.GetCounter() == 0) {
        std::get<NMetrics::EResource::Counter>(values) = 0;
    }
    if (!HasAllowedMetric(allowedMetricIds, EResourceToBalance::CPU) || !THive::IsValidMetricsCPU(metrics)) {
        std::get<NMetrics::EResource::CPU>(values) = 0;
    }
    if (!HasAllowedMetric(allowedMetricIds, EResourceToBalance::Memory) || !THive::IsValidMetricsMemory(metrics)) {
        std::get<NMetrics::EResource::Memory>(values) = 0;
    }
    if (!HasAllowedMetric(allowedMetricIds, EResourceToBalance::Network) || !THive::IsValidMetricsNetwork(metrics)) {
        std::get<NMetrics::EResource::Network>(values) = 0;
    }
}

void TTabletInfo::FilterRawValues(TResourceNormalizedValues& values) const {
    const NKikimrTabletBase::TMetrics& metrics(ResourceValues);
    const TVector<i64>& allowedMetricIds = GetTabletAllowedMetricIds();
    if (metrics.GetCounter() == 0) {
        std::get<NMetrics::EResource::Counter>(values) = 0;
    }
    if (!HasAllowedMetric(allowedMetricIds, EResourceToBalance::CPU) || !THive::IsValidMetricsCPU(metrics)) {
        std::get<NMetrics::EResource::CPU>(values) = 0;
    }
    if (!HasAllowedMetric(allowedMetricIds, EResourceToBalance::Memory) || !THive::IsValidMetricsMemory(metrics)) {
        std::get<NMetrics::EResource::Memory>(values) = 0;
    }
    if (!HasAllowedMetric(allowedMetricIds, EResourceToBalance::Network) || !THive::IsValidMetricsNetwork(metrics)) {
        std::get<NMetrics::EResource::Network>(values) = 0;
    }
}

void TTabletInfo::ActualizeCounter() {
    ResourceValues.SetCounter(GetCounterValue());
}

const TNodeFilter& TTabletInfo::GetNodeFilter() const {
    if (IsLeader()) {
        return AsLeader().NodeFilter;
    } else {
        return AsFollower().FollowerGroup.NodeFilter;
    }
}

bool TTabletInfo::InitiateStart(TNodeInfo* node) {
    if (BecomeStarting(node->Id)) {
        PostponedStart = {};
        TFullTabletId tabletId = GetFullTabletId();
        Hive.ExecuteStartTablet(tabletId, node->Local, tabletId.first, false);
        return true;
    }
    return false;
}

void TTabletInfo::SendStopTablet(TSideEffects& sideEffects) {
    TActorId local = GetLocal();
    if (local) {
        SendStopTablet(local, sideEffects);
    }
}

void TTabletInfo::SendStopTablet(const TActorId& local, TSideEffects& sideEffects) {
    if (local) {
        TFullTabletId tabletId = GetFullTabletId();
        ui32 gen = 0;
        if (IsLeader()) {
            gen = AsLeader().KnownGeneration;
        }
        BLOG_D("Sending TEvStopTablet(" << ToString() << " gen " << gen << ") to node " << local.NodeId());
        sideEffects.Send(local, new TEvLocal::TEvStopTablet(tabletId, gen));
    }
}

TString TTabletInfo::GetLogPrefix() const {
    return Hive.GetLogPrefix();
}

void TTabletInfo::ActualizeTabletStatistics(TInstant now) {
    TInstant barierTime = now - Hive.GetTabletRestartWatchPeriod();
    Hive.ActualizeRestartStatistics(*Statistics.MutableRestartTimestamp(), barierTime.MilliSeconds());
}

ui64 TTabletInfo::GetRestartsPerPeriod(TInstant barrier) const {
    return Hive.GetRestartsPerPeriod(Statistics.GetRestartTimestamp(), barrier.MilliSeconds());
}

bool TTabletInfo::RestartsOften() const {
    // Statistics.RestartTimestamp is a repeated proto field that gets trimmed
    // upon each update of tablet metrics (or restart).
    // If its current size is >= RestartsMaxCount, it means the tablet was restarting
    // often at the time of last update, and thus deserves low booting priority
    return Statistics.RestartTimestampSize() >= Hive.GetTabletRestartsMaxCount();
}

} // NHive
} // NKikimr
