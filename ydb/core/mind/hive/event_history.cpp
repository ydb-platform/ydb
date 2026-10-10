#include "hive_impl.h"
#include "hive_log.h"
#include "event_history.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

TStringBuf EHiveEventReasonName(EHiveEventReason value) {
    switch (value) {
    case EHiveEventReason::NewLocalActor: return "new Local actor";
    case EHiveEventReason::SameLocalActor: return "same Local actor";
    case EHiveEventReason::ServicedDomainsChanged: return "serviced domains changed on registration";
    case EHiveEventReason::TenantChanged: return "tenant changed";
    case EHiveEventReason::BecomeUpOnRestart: return "node restarted with BecomeUpOnRestart";
    case EHiveEventReason::RegisterNode: return "RegisterNode";
    case EHiveEventReason::NameService: return "NameService";
    case EHiveEventReason::StatusOk: return "status OK";
    case EHiveEventReason::BadStatus: return "bad status";
    case EHiveEventReason::InterconnectDisconnected: return "interconnect disconnected";
    case EHiveEventReason::InterconnectDisconnectedUnknownNode: return "interconnect disconnected while node state is Unknown";
    case EHiveEventReason::PingUndelivered: return "ping undelivered";
    case EHiveEventReason::NodeExpired: return "not alive, no tablets, delete period expired";
    case EHiveEventReason::DrainDownPolicy: return "drain down policy";
    case EHiveEventReason::DrainRequested: return "drain requested";
    case EHiveEventReason::DrainSwitchedOff: return "drain switched off";
    case EHiveEventReason::DrainStarted: return "drain started";
    case EHiveEventReason::DrainResumed: return "persisted drain resumed on node connect";
    case EHiveEventReason::DrainFinished: return "drain finished";
    case EHiveEventReason::SetDownRequest: return "TEvSetDown";
    case EHiveEventReason::MonitoringRequest: return "monitoring request";
    case EHiveEventReason::LoadedFromDatabase: return "loaded from database";
    case EHiveEventReason::InitialState: return "initial state";
    case EHiveEventReason::OwnerRequest: return "owner request";
    case EHiveEventReason::BootQueue: return "boot queue";
    case EHiveEventReason::SyncTablets: return "node reported tablet on sync";
    case EHiveEventReason::TabletDead: return "tablet reported dead";
    case EHiveEventReason::StartFailed: return "start on node failed";
    case EHiveEventReason::RestartPenalty: return "too many restarts";
    case EHiveEventReason::NodeDisconnected: return "node disconnected";
    case EHiveEventReason::Move: return "move to another node";
    case EHiveEventReason::RestartRequest: return "restart requested";
    case EHiveEventReason::StopRequest: return "stop requested";
    case EHiveEventReason::LockRequest: return "lock requested";
    case EHiveEventReason::UnlockRequest: return "unlock requested";
    case EHiveEventReason::TabletLocked: return "tablet is locked to external owner";
    case EHiveEventReason::Deleting: return "tablet is being deleted";
    case EHiveEventReason::BootingSuppressed: return "booting suppressed";
    case EHiveEventReason::GroupsChanged: return "storage groups changed";
    case EHiveEventReason::FollowerRemoved: return "follower removed";
    case EHiveEventReason::PileUpdate: return "bridge pile update";
    case EHiveEventReason::TenantStopped: return "tenant stopped";
    case EHiveEventReason::ConfigChanged: return "config changed";
    case EHiveEventReason::Seized: return "tablet seized from another hive";
    case EHiveEventReason::Drain: return "drain";
    case EHiveEventReason::Fill: return "fill";
    case EHiveEventReason::ManualMove: return "manual move";
    case EHiveEventReason::StorageReassign: return "storage reassign";
    case EHiveEventReason::TabletNotAlive: return "tablet is not alive";
    case EHiveEventReason::SourceNodeDown: return "source node is down";
    case EHiveEventReason::SourceNodeCannotRunTablet: return "source node cannot run tablet";
    case EHiveEventReason::SourceNodeOverloaded: return "source node is overloaded";
    case EHiveEventReason::SpreadNeighbours: return "spread neighbours";
    case EHiveEventReason::ExpediencyCheckDisabled: return "move expediency check disabled";
    case EHiveEventReason::ResourceStDevImproved: return "resource stdev improves";
    case EHiveEventReason::LeaderNotRunning: return "leader not running";
    case EHiveEventReason::AllNodesDead: return "all nodes are dead";
    case EHiveEventReason::AllNodesDeadOrDown: return "all nodes are dead or down";
    case EHiveEventReason::NoNodesAllowedToRun: return "no nodes allowed to run";
    case EHiveEventReason::FamilyFilledAllNodes: return "all available nodes are already filled with someone from our family";
    case EHiveEventReason::DomainNotFound: return "can't find domain";
    case EHiveEventReason::NotEnoughDatacenters: return "not enough datacenters";
    case EHiveEventReason::NotEnoughResources: return "not enough resources";
    case EHiveEventReason::NodesLocationUnknown: return "nodes location unknown";
    case EHiveEventReason::TooManyStarting: return "too many tablets starting";
    case EHiveEventReason::PreferredNodeUnavailable: return "preferred node unavailable";
    }
    return "Unknown";
}

TStringBuf EHiveEventTypeName(EHiveEventType value) {
    switch (value) {
    case EHiveEventType::Registered: return "Registered";
    case EHiveEventType::Connected: return "Connected";
    case EHiveEventType::Disconnecting: return "Disconnecting";
    case EHiveEventType::Killed: return "Killed";
    case EHiveEventType::Deleted: return "Deleted";
    case EHiveEventType::TenantChanged: return "TenantChanged";
    case EHiveEventType::Down: return "Down";
    case EHiveEventType::Up: return "Up";
    case EHiveEventType::Frozen: return "Frozen";
    case EHiveEventType::Unfrozen: return "Unfrozen";
    case EHiveEventType::DrainStarted: return "DrainStarted";
    case EHiveEventType::DrainFinished: return "DrainFinished";
    case EHiveEventType::LocationChanged: return "LocationChanged";
    case EHiveEventType::AvailabilityChanged: return "AvailabilityChanged";
    case EHiveEventType::Created: return "Created";
    case EHiveEventType::Deleting: return "Deleting";
    case EHiveEventType::Starting: return "Starting";
    case EHiveEventType::Running: return "Running";
    case EHiveEventType::Stopped: return "Stopped";
    case EHiveEventType::BootFailed: return "BootFailed";
    case EHiveEventType::StartPostponed: return "StartPostponed";
    case EHiveEventType::NoNodeToBoot: return "NoNodeToBoot";
    case EHiveEventType::Moved: return "Moved";
    case EHiveEventType::GroupsReassigned: return "GroupsReassigned";
    case EHiveEventType::Locked: return "Locked";
    case EHiveEventType::Unlocked: return "Unlocked";
    case EHiveEventType::StopRequested: return "StopRequested";
    case EHiveEventType::Resumed: return "Resumed";
    }
    return "Unknown";
}

void THive::RecordNodeEvent(TNodeInfo& node, EHiveEventType type, EHiveEventReason reason, TString details) {
    THiveEvent event(TActivationContext::Now(), type, reason, std::move(details));
    // State restored from the database on Hive start is not a transition: it is kept in the histories
    // (so the node page shows the node was already down/frozen before this Hive generation) but not logged,
    // TTxLoadEverything already reports the loaded nodes
    const auto priority = (reason == EHiveEventReason::LoadedFromDatabase) ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_INFO;
    YDB_LOG(priority, "Node event",
        {"logPrefix", GetLogPrefix()},
        {"event", EHiveEventTypeName(type)},
        {"nodeId", node.Id},
        {"nodeName", node.Name},
        {"local", node.Local},
        {"nodeState", TNodeInfo::EVolatileStateName(node.GetVolatileState())},
        {"down", node.Down},
        {"freeze", node.Freeze},
        {"drain", node.Drain},
        {"domain", node.GetServicedDomain()},
        {"dataCenter", node.GetDataCenter()},
        {"pile", node.BridgePileId},
        {"tabletsRunning", node.GetTabletsRunning()},
        {"tabletsStarting", node.GetTabletsScheduled()},
        {"tabletsLocked", node.LockedTablets.size()},
        {"startTime", node.StartTime},
        {"restarts", node.GetRestartsPerPeriod()},
        {"reason", EHiveEventReasonName(reason)},
        {"details", event.Details});
    RecentNodeEvents.PushBack(TRecentNodeEvent{.NodeId = node.Id, .Event = event});
    if (!node.EventHistory) {
        const ui64 historySize = GetNodeEventHistorySize();
        if (historySize == 0) {
            return;
        }
        node.EventHistory.ConstructInPlace(historySize);
    }
    node.EventHistory->PushBack(event);
}

void THive::RecordTabletEvent(const TTabletInfo& tablet, EHiveEventType type, EHiveEventReason reason, TString details, bool skipIfRepeated) {
    if (reason == EHiveEventReason::LoadedFromDatabase || reason == EHiveEventReason::InitialState) {
        // Not transitions: state restored on Hive start or a freshly created tablet entering its first state.
        // Recording them would also allocate a history for every tablet at once.
        return;
    }
    if (skipIfRepeated && tablet.EventHistory && tablet.EventHistory->AvailSize() > 0) {
        const THiveEvent& last = (*tablet.EventHistory)[tablet.EventHistory->TotalSize() - 1];
        if (last.Type == type && last.Reason == reason && last.Details == details) {
            return;
        }
    }
    const TInstant now = TActivationContext::Now();
    THiveEvent event(now, type, reason, std::move(details));
    const TLeaderTabletInfo& leader = tablet.GetLeader();
    YDB_LOG_INFO("Tablet event",
        {"logPrefix", GetLogPrefix()},
        {"event", EHiveEventTypeName(type)},
        {"tabletId", tablet.GetFullTabletId()},
        {"tabletType", TTabletTypes::TypeToStr(tablet.GetTabletType())},
        {"volatileState", TTabletInfo::EVolatileStateName(tablet.GetVolatileState())},
        {"state", ETabletStateName(leader.State)},
        {"nodeId", tablet.NodeId},
        {"lastNodeId", tablet.LastNodeId},
        {"objectId", tablet.GetObjectId()},
        {"generation", leader.KnownGeneration},
        {"bootState", tablet.BootState},
        {"restarts", tablet.GetRestartsPerPeriod(now - GetTabletRestartsPeriodForPenalties())},
        {"reason", EHiveEventReasonName(reason)},
        {"details", event.Details});
    RecentTabletEvents.PushBack(TRecentTabletEvent{.TabletId = tablet.GetFullTabletId(), .Event = event});
    if (!tablet.EventHistory) {
        const ui64 historySize = GetTabletEventHistorySize();
        if (historySize == 0) {
            return;
        }
        tablet.EventHistory.ConstructInPlace(historySize);
    }
    tablet.EventHistory->PushBack(event);
}

void ResizeEventHistory(TMaybe<TSimpleRingBuffer<THiveEvent>>& history, ui64 newSize) {
    if (!history) {
        return;
    }
    if (newSize == 0) {
        history.Clear();
        return;
    }
    const TSimpleRingBuffer<THiveEvent>& old = *history;
    TSimpleRingBuffer<THiveEvent> resized(newSize);
    size_t first = old.FirstIndex();
    if (old.TotalSize() - first > newSize) {
        first = old.TotalSize() - newSize;
    }
    for (size_t i = first; i < old.TotalSize(); ++i) {
        resized.PushBack(old[i]);
    }
    history = std::move(resized);
}

void THive::ResizeTabletEventHistory(ui64 newSize) {
    for (auto& [_, leader] : Tablets) {
        ResizeEventHistory(leader.EventHistory, newSize);
        for (const TFollowerTabletInfo& follower : leader.Followers) {
            ResizeEventHistory(follower.EventHistory, newSize);
        }
    }
}

void THive::ResizeNodeEventHistory(ui64 newSize) {
    for (auto& [_, node] : Nodes) {
        ResizeEventHistory(node.EventHistory, newSize);
    }
}

void THive::RecordTabletBootFailure(const TTabletInfo& tablet, EHiveEventReason reason, TString details) {
    // FindBestNode is also called by the balancer and drain for running tablets, only a tablet waiting
    // in the boot queue is actually failing to boot; the boot queue retries, so repeats are collapsed
    if (tablet.IsBooting()) {
        RecordTabletEvent(tablet, EHiveEventType::NoNodeToBoot, reason, std::move(details), /* skipIfRepeated */ true);
    }
}

} // NHive
} // NKikimr
