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
    case EHiveEventReason::DrainFinished: return "drain finished";
    case EHiveEventReason::SetDownRequest: return "TEvSetDown";
    case EHiveEventReason::MonitoringRequest: return "monitoring request";
    case EHiveEventReason::LoadedFromDatabase: return "loaded from database";
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
    RecentNodeEvents.Push(TRecentNodeEvent{.NodeId = node.Id, .Event = event});
    node.EventHistory.Push(std::move(event));
}

} // NHive
} // NKikimr
