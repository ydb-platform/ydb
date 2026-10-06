#include "hive_impl.h"
#include "hive_log.h"
#include "event_history.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

TStringBuf ENodeEventReasonName(ENodeEventReason value) {
    switch (value) {
    case ENodeEventReason::NewLocalActor: return "new Local actor";
    case ENodeEventReason::SameLocalActor: return "same Local actor";
    case ENodeEventReason::ServicedDomainsChanged: return "serviced domains changed on registration";
    case ENodeEventReason::TenantChanged: return "tenant changed";
    case ENodeEventReason::BecomeUpOnRestart: return "node restarted with BecomeUpOnRestart";
    case ENodeEventReason::RegisterNode: return "RegisterNode";
    case ENodeEventReason::NameService: return "NameService";
    case ENodeEventReason::StatusOk: return "status OK";
    case ENodeEventReason::BadStatus: return "bad status";
    case ENodeEventReason::InterconnectDisconnected: return "interconnect disconnected";
    case ENodeEventReason::InterconnectDisconnectedUnknownNode: return "interconnect disconnected while node state is Unknown";
    case ENodeEventReason::PingUndelivered: return "ping undelivered";
    case ENodeEventReason::NodeExpired: return "not alive, no tablets, delete period expired";
    case ENodeEventReason::DrainDownPolicy: return "drain down policy";
    case ENodeEventReason::DrainRequested: return "drain requested";
    case ENodeEventReason::DrainSwitchedOff: return "drain switched off";
    case ENodeEventReason::DrainStarted: return "drain started";
    case ENodeEventReason::DrainFinished: return "drain finished";
    case ENodeEventReason::SetDownRequest: return "TEvSetDown";
    case ENodeEventReason::MonitoringRequest: return "monitoring request";
    case ENodeEventReason::LoadedFromDatabase: return "loaded from database";
    }
    return "Unknown";
}

TStringBuf ENodeEventName(ENodeEvent value) {
    switch (value) {
    case ENodeEvent::Registered: return "Registered";
    case ENodeEvent::Connected: return "Connected";
    case ENodeEvent::Disconnecting: return "Disconnecting";
    case ENodeEvent::Killed: return "Killed";
    case ENodeEvent::Deleted: return "Deleted";
    case ENodeEvent::TenantChanged: return "TenantChanged";
    case ENodeEvent::Down: return "Down";
    case ENodeEvent::Up: return "Up";
    case ENodeEvent::Frozen: return "Frozen";
    case ENodeEvent::Unfrozen: return "Unfrozen";
    case ENodeEvent::DrainStarted: return "DrainStarted";
    case ENodeEvent::DrainFinished: return "DrainFinished";
    case ENodeEvent::LocationChanged: return "LocationChanged";
    case ENodeEvent::AvailabilityChanged: return "AvailabilityChanged";
    }
    return "Unknown";
}

void THive::RecordNodeEvent(TNodeInfo& node, ENodeEvent type, ENodeEventReason reason, TString details) {
    TNodeEvent event(TActivationContext::Now(), type, reason, std::move(details));
    // State restored from the database on Hive start is not a transition: it is kept in the histories
    // (so the node page shows the node was already down/frozen before this Hive generation) but not logged,
    // TTxLoadEverything already reports the loaded nodes
    const auto priority = (reason == ENodeEventReason::LoadedFromDatabase) ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_INFO;
    YDB_LOG(priority, "Node event",
        {"logPrefix", GetLogPrefix()},
        {"event", ENodeEventName(type)},
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
        {"reason", ENodeEventReasonName(reason)},
        {"details", event.Details});
    RecentNodeEvents.Push(TRecentNodeEvent{.NodeId = node.Id, .Event = event});
    node.EventHistory.Push(std::move(event));
}

} // NHive
} // NKikimr
