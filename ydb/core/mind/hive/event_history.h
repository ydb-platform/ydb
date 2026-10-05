#pragma once

#include "hive.h"

#include <vector>

namespace NKikimr {
namespace NHive {

// In-memory history of important Hive events for debugging. Currently covers nodes, tablet events are
// expected to follow the same pattern.
//
// Lifecycle events of a node as seen by Hive. Every event is written to the Hive log at INFO level
// and kept in memory: the last TNodeInfo::EVENT_HISTORY_SIZE events per node (TNodeInfo::EventHistory)
// and the last THive::RECENT_NODE_EVENTS_SIZE events across all nodes (THive::RecentNodeEvents).
enum class ENodeEvent : ui8 {
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

TStringBuf ENodeEventName(ENodeEvent value);

// Constant reasons for node events. TString is a refcounted pointer, so passing one of these shared
// instances stores a reference in the event instead of allocating a new string per event.
struct TNodeEventReason {
    static const TString NewLocalActor;
    static const TString SameLocalActor;
    static const TString ServicedDomainsChanged;
    static const TString TenantChanged;
    static const TString BecomeUpOnRestart;
    static const TString RegisterNode;
    static const TString NameService;
    static const TString StatusOk;
    static const TString BadStatus;
    static const TString InterconnectDisconnected;
    static const TString InterconnectDisconnectedUnknownNode;
    static const TString PingUndelivered;
    static const TString NodeExpired;
    static const TString DrainDownPolicy;
    static const TString DrainRequested;
    static const TString DrainSwitchedOff;
    static const TString DrainStarted;
    static const TString DrainFinished;
    static const TString SetDownRequest;
    static const TString MonitoringRequest;
    static const TString LoadedFromDatabase;
};

// Kept small on purpose: a cluster may have thousands of nodes with a full history each.
// TString is a refcounted pointer (8 bytes, no allocation when empty), so the reason shares the buffer
// of the TNodeEventReason constant and copies of an event in the global and per-node histories share
// one Extra buffer.
struct TNodeEvent {
    TInstant Timestamp;
    TString Reason; // constant part of the description, one of TNodeEventReason
    TString Extra;  // variable part of the description
    ENodeEvent Type = ENodeEvent::Registered;

    TNodeEvent() = default;
    TNodeEvent(TInstant timestamp, ENodeEvent type, const TString& reason, TString extra)
        : Timestamp(timestamp)
        , Reason(reason)
        , Extra(std::move(extra))
        , Type(type)
    {}
};

static_assert(sizeof(TNodeEvent) <= 32, "TNodeEvent is expected to stay compact");

// Ring buffer with a fixed capacity that allocates lazily: an idle node costs nothing.
template <typename T, size_t Capacity>
class TLazyRingBuffer {
    std::vector<T> Items;
    size_t Begin = 0; // index of the oldest item once the buffer is full

public:
    static constexpr size_t CAPACITY = Capacity;

    size_t Size() const {
        return Items.size();
    }

    bool Empty() const {
        return Items.empty();
    }

    void Push(T&& item) {
        if (Items.size() < Capacity) {
            if (Items.size() == Items.capacity()) {
                Items.reserve(std::min(std::max<size_t>(4, Items.capacity() * 2), Capacity));
            }
            Items.push_back(std::move(item));
        } else {
            Items[Begin] = std::move(item);
            Begin = (Begin + 1) % Capacity;
        }
    }

    // index 0 is the newest item
    const T& FromNewest(size_t index) const {
        Y_ASSERT(index < Items.size());
        return Items[(Begin + Items.size() - 1 - index) % Items.size()];
    }

    template <typename TCallback>
    void ForEachNewestFirst(TCallback&& callback) const {
        for (size_t i = 0; i < Items.size(); ++i) {
            callback(FromNewest(i));
        }
    }
};

struct TRecentNodeEvent {
    TNodeId NodeId = 0;
    TNodeEvent Event;
};

} // NHive
} // NKikimr
