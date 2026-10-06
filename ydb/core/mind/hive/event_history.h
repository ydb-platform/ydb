#pragma once

#include "hive.h"

#include <library/cpp/containers/ring_buffer/ring_buffer.h>

namespace NKikimr {
namespace NHive {

// In-memory history of important Hive events for debugging. Every event is written to the Hive log
// at INFO level and kept in memory in bounded ring buffers (TStaticRingBuffer): per subject
// (e.g. TNodeInfo::EventHistory) and across the whole Hive (e.g. THive::RecentNodeEvents), see THive::RecordNodeEvent.
//
// THiveEvent and the two enums below are shared by all kinds of events: node events today,
// tablet events and Hive settings changes are expected to follow.
enum class EHiveEventType : ui8 {
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
};

TStringBuf EHiveEventTypeName(EHiveEventType value);

// Why it happened: the constant part of the description, rendered as text only when logged or shown in the UI
enum class EHiveEventReason : ui8 {
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
    DrainFinished,
    SetDownRequest,
    MonitoringRequest,
    LoadedFromDatabase,
};

TStringBuf EHiveEventReasonName(EHiveEventReason value);

// Kept small on purpose: a cluster may have thousands of nodes and tablets with a full history each.
// One word holds the millisecond timestamp (48 bits), the type and the reason (a byte each);
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

struct TRecentNodeEvent {
    TNodeId NodeId = 0;
    THiveEvent Event;
};

} // NHive
} // NKikimr
