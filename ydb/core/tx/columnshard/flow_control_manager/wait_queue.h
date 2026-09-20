#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

#include <list>
#include <optional>

namespace NKikimr::NColumnShard::NFlowControl {

struct TWaiter {
    ui64 WaiterId = 0;
    NActors::TActorId Helper;
    TVector<ui64> TabletIds;
    TVector<ui32> TargetNodes;   // distinct known nodes at enqueue (for the per-node waiter counts)
    TInstant WaitDeadline;
    TInstant EnqueuedAt;
    ui64 BatchSize = 0;   // deserialized batch bytes; charged against the bytes-rate bucket
    bool DrainScheduled = false;
    bool TokenReserved = false;
};

// FIFO of admission waiters, plus the per-node waiter counts that implement the no-jump rule: a
// request must not overtake someone already queued for the same node.
//
// The order is a std::list rather than a deque of ids so that Erase() is O(1). Waiters leave the
// queue out of order all the time (cancel, client deadline, drain), and a linear scan per removal
// made the whole queue quadratic at the sizes it is actually configured for.
class TWaitQueue {
    std::list<TWaiter> Order;
    THashMap<ui64, std::list<TWaiter>::iterator> ById;
    THashMap<ui32, ui64> WaiterCountByNode;
    ui64 NextWaiterId = 1;

public:
    bool Empty() const {
        return Order.empty();
    }

    size_t Size() const {
        return Order.size();
    }

    const std::list<TWaiter>& GetOrder() const {
        return Order;
    }

    std::list<TWaiter>& MutableOrder() {
        return Order;
    }

    // Assigns the waiter id and takes ownership. Returns the id.
    ui64 Enqueue(TWaiter&& waiter);

    bool Contains(ui64 waiterId) const {
        return ById.contains(waiterId);
    }

    TWaiter* Find(ui64 waiterId);

    // Returns the removed waiter so the caller can refund a token it had reserved; nullopt if the
    // id is unknown (already drained, cancelled or timed out).
    std::optional<TWaiter> Erase(ui64 waiterId);

    // No-jump check: is anybody already queued for one of these nodes?
    bool HasWaitersOnAnyNode(const TVector<ui32>& nodes) const;
};

}   // namespace NKikimr::NColumnShard::NFlowControl
