#include "hive_impl.h"
#include "hive_log.h"
#include "event_history.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::HIVE

namespace NKikimr {
namespace NHive {

const TString TNodeEventReason::NewLocalActor = "new Local actor";
const TString TNodeEventReason::SameLocalActor = "same Local actor";
const TString TNodeEventReason::ServicedDomainsChanged = "serviced domains changed on registration";
const TString TNodeEventReason::TenantChanged = "tenant changed";
const TString TNodeEventReason::BecomeUpOnRestart = "node restarted with BecomeUpOnRestart";
const TString TNodeEventReason::RegisterNode = "RegisterNode";
const TString TNodeEventReason::NameService = "NameService";
const TString TNodeEventReason::StatusOk = "status OK";
const TString TNodeEventReason::BadStatus = "bad status";
const TString TNodeEventReason::InterconnectDisconnected = "interconnect disconnected";
const TString TNodeEventReason::InterconnectDisconnectedUnknownNode = "interconnect disconnected while node state is Unknown";
const TString TNodeEventReason::PingUndelivered = "ping undelivered";
const TString TNodeEventReason::NodeExpired = "not alive, no tablets, delete period expired";
const TString TNodeEventReason::DrainDownPolicy = "drain down policy";
const TString TNodeEventReason::DrainRequested = "drain requested";
const TString TNodeEventReason::DrainSwitchedOff = "drain switched off";
const TString TNodeEventReason::DrainStarted = "drain started";
const TString TNodeEventReason::DrainFinished = "drain finished";
const TString TNodeEventReason::SetDownRequest = "TEvSetDown";
const TString TNodeEventReason::MonitoringRequest = "monitoring request";
const TString TNodeEventReason::LoadedFromDatabase = "loaded from database";

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

void THive::RecordNodeEvent(TNodeInfo& node, ENodeEvent type, const TString& reason, TString extra) {
    TNodeEvent event(TActivationContext::Now(), type, reason, std::move(extra));
    // State restored from the database on Hive start is not a transition: it is kept in the histories
    // (so the node page shows the node was already down/frozen before this Hive generation) but not logged,
    // TTxLoadEverything already reports the loaded nodes
    const auto priority = (reason == TNodeEventReason::LoadedFromDatabase) ? NActors::NLog::PRI_DEBUG : NActors::NLog::PRI_INFO;
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
        {"reason", event.Reason},
        {"extra", event.Extra});
    RecentNodeEvents.Push(TRecentNodeEvent{.NodeId = node.Id, .Event = event});
    node.EventHistory.Push(std::move(event));
}

} // NHive
} // NKikimr
