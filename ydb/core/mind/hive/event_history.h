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

// Constant reasons for node events, rendered as text only when logged or shown in the UI
enum class ENodeEventReason : ui8 {
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

TStringBuf ENodeEventReasonName(ENodeEventReason value);

// Kept small on purpose: a cluster may have thousands of nodes with a full history each.
// One word holds the millisecond timestamp (48 bits), the type and the reason (a byte each);
// Details is a refcounted pointer (8 bytes, no allocation when empty) shared by the copies
// in the global and per-node histories.
struct TNodeEvent {
    ui64 Packed = 0;
    TString Details; // variable part of the description

    static constexpr ui64 TYPE_SHIFT = 0;
    static constexpr ui64 TYPE_MASK = 0xFF;
    static constexpr ui64 REASON_SHIFT = 8;
    static constexpr ui64 REASON_MASK = 0xFF;
    static constexpr ui64 TIMESTAMP_SHIFT = 16;
    static constexpr ui64 TIMESTAMP_MASK = (1ull << 48) - 1; // milliseconds, enough for ~8900 years

    TNodeEvent() = default;
    TNodeEvent(TInstant timestamp, ENodeEvent type, ENodeEventReason reason, TString details)
        : Packed(((static_cast<ui64>(type) & TYPE_MASK) << TYPE_SHIFT)
            | ((static_cast<ui64>(reason) & REASON_MASK) << REASON_SHIFT)
            | ((timestamp.MilliSeconds() & TIMESTAMP_MASK) << TIMESTAMP_SHIFT))
        , Details(std::move(details))
    {}

    ENodeEvent GetType() const {
        return static_cast<ENodeEvent>((Packed >> TYPE_SHIFT) & TYPE_MASK);
    }

    ENodeEventReason GetReason() const {
        return static_cast<ENodeEventReason>((Packed >> REASON_SHIFT) & REASON_MASK);
    }

    TInstant GetTimestamp() const {
        return TInstant::MilliSeconds((Packed >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK);
    }
};

static_assert(sizeof(TNodeEvent) <= 16, "TNodeEvent is expected to stay compact");

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
