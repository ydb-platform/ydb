#pragma once

#include "hive.h"

#include <library/cpp/containers/ring_buffer/ring_buffer.h>

namespace NKikimr {
namespace NHive {

// In-memory history of important Hive events for debugging. Every event is written to the Hive log
// at INFO level and kept in memory in bounded ring buffers: per subject (TNodeInfo::EventHistory,
// TTabletInfo::EventHistory) and across the whole Hive (THive::RecentNodeEvents, THive::RecentTabletEvents),
// see THive::RecordNodeEvent and THive::RecordTabletEvent.
//
// THiveEvent and the two enums below are shared by all kinds of events: node and tablet events today,
// Hive settings changes are expected to follow.
enum class EHiveEventType : ui64 {
    // Node events
    Registered,          // Local registered on Hive (first time or after node restart)
    Connected,           // Local reported StatusOk, node can run tablets
    Disconnecting,       // interconnect session lost, waiting for disconnect timeouts
    Killed,              // node considered dead, its tablets are restarted elsewhere
    Deleted,             // node removed from Hive database
    TenantChanged,       // node re-registered with a different set of serviced domains
    Down,
    Up,
    Frozen,
    Unfrozen,
    DrainStarted,
    DrainFinished,
    LocationChanged,
    AvailabilityChanged, // per-tablet-type MaxCount restriction changed

    // Tablet events
    Created,
    Deleting,            // persistent state switched to Deleting, storage is being released
    Starting,            // boot command sent to a node
    Running,             // node reported the tablet is running
    Stopped,             // tablet is not running anywhere, see reason for why
    BootFailed,          // node reported a boot failure or death
    StartPostponed,      // too many restarts, next start is delayed
    NoNodeToBoot,        // boot queue could not find a node, see reason
    Moved,               // moved to another node by the balancer, drain, fill or by hand
    GroupsReassigned,    // storage groups changed
    Locked,              // execution locked to an external owner
    Unlocked,
    StopRequested,       // persistent stop requested by the owner or tenant
    Resumed,

    MAX, // keep last
};

static_assert(static_cast<ui64>(EHiveEventType::MAX) <= (1 << 8), "EHiveEventType must fit into THiveEvent::Type");

TStringBuf EHiveEventTypeName(EHiveEventType value);

// Why it happened: the constant part of the description, rendered as text only when logged or shown in the UI
enum class EHiveEventReason : ui64 {
    NewLocalActor,
    SameLocalActor,
    ServicedDomainsChanged,
    TenantChanged,
    BecomeUpOnRestart,
    RegisterNode,
    NameService,
    StatusOk,
    BadStatus,
    InterconnectDisconnected,
    InterconnectDisconnectedUnknownNode,
    PingUndelivered,
    NodeExpired,
    DrainDownPolicy,
    DrainRequested,
    DrainSwitchedOff,
    DrainStarted,
    DrainResumed,
    DrainFinished,
    SetDownRequest,
    MonitoringRequest,
    LoadedFromDatabase,

    // Tablet reasons
    InitialState,             // never recorded: a freshly created tablet entering its first state
    OwnerRequest,
    BootQueue,
    SyncTablets,              // node reported the tablet when (re)connecting
    TabletDead,
    StartFailed,
    RestartPenalty,
    NodeDisconnected,
    Move,
    RestartRequest,
    StopRequest,
    LockRequest,
    UnlockRequest,
    TabletLocked,
    Deleting,
    BootingSuppressed,
    GroupsChanged,
    FollowerRemoved,
    PileUpdate,
    TenantStopped,
    ConfigChanged,
    Seized,
    Drain,
    Fill,
    ManualMove,
    StorageReassign,
    // why the balancer decided to move a tablet, see THive::CheckTabletMoveExpediency
    TabletNotAlive,
    SourceNodeDown,
    SourceNodeCannotRunTablet,
    SourceNodeOverloaded,
    SpreadNeighbours,
    ExpediencyCheckDisabled,
    ResourceStDevImproved,
    // boot queue failures, mirror THive::BootState* strings
    LeaderNotRunning,
    AllNodesDead,
    AllNodesDeadOrDown,
    NoNodesAllowedToRun,
    FamilyFilledAllNodes,
    DomainNotFound,
    NotEnoughDatacenters,
    NotEnoughResources,
    NodesLocationUnknown,
    TooManyStarting,
    PreferredNodeUnavailable,

    MAX, // keep last
};

static_assert(static_cast<ui64>(EHiveEventReason::MAX) <= (1 << 8), "EHiveEventReason must fit into THiveEvent::Reason");

TStringBuf EHiveEventReasonName(EHiveEventReason value);

// Kept small on purpose: a cluster may have thousands of nodes and tablets with a full history each.
// One word holds the millisecond timestamp (48 bits), the type and the reason (a byte each);
// the enums have a 64-bit underlying type so that MSVC packs all the bitfields into one word;
// Details is a refcounted pointer (8 bytes, no allocation when empty) shared by the copies
// in the global and per-node histories.
struct THiveEvent {
    ui64 TimestampMs : 48; // milliseconds, enough for ~8900 years
    EHiveEventType Type : 8;
    EHiveEventReason Reason : 8;
    TString Details; // variable part of the description

    THiveEvent() = default;
    THiveEvent(TInstant timestamp, EHiveEventType type, EHiveEventReason reason, TString details)
        : TimestampMs(timestamp.MilliSeconds())
        , Type(type)
        , Reason(reason)
        , Details(std::move(details))
    {}

    TInstant GetTimestamp() const {
        return TInstant::MilliSeconds(TimestampMs);
    }
};

static_assert(sizeof(THiveEvent) <= 16, "THiveEvent is expected to stay compact");

class TEventHistory {
public:
    bool IsEmpty() const {
        return !Buffer.Defined();
    }

    size_t GetCapacity() const {
        return Capacity;
    }

    size_t GetSize() const {
        return Buffer ? Buffer->AvailSize() : 0;
    }

    const THiveEvent* GetNewest() const {
        return GetSize() > 0 ? &(*Buffer)[Buffer->TotalSize() - 1] : nullptr;
    }

    // Appends an event, creating the buffer on the first event and resizing it first if the configured
    // size changed; size 0 drops the buffer and records nothing.
    void Push(ui64 configuredSize, const THiveEvent& event);

    // Rebuilds the buffer with the new capacity keeping the newest events that fit; 0 drops the buffer.
    // An empty history (no events recorded yet) is left as is.
    void Resize(ui64 newSize);

    template <typename TCallback>
    void ForEachNewestFirst(TCallback&& callback) const {
        if (Buffer) {
            for (size_t i = Buffer->TotalSize(); i > Buffer->FirstIndex(); --i) {
                callback((*Buffer)[i - 1]);
            }
        }
    }

private:
    TMaybe<TSimpleRingBuffer<THiveEvent>> Buffer;
    size_t Capacity = 0; // of Buffer, 0 when there is no buffer; TSimpleRingBuffer does not expose it
};

struct TRecentNodeEvent {
    TNodeId NodeId = 0;
    THiveEvent Event;
};

struct TRecentTabletEvent {
    TFullTabletId TabletId;
    THiveEvent Event;
};

} // NHive
} // NKikimr
