#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>

#include <list>
#include <optional>

namespace NKikimr::NColumnShard::NFlowControl {

// Delayed-reject entry: only the minimum needed to send OVERLOADED after a delay. The Arrow batch
// is dropped at enqueue time to save memory, and the reject reason is attached by the helper
// actor, which owns the client's TIssues.
struct TDelayedReject {
    ui64 RejectId = 0;
    NActors::TActorId ReplyTo;
    TInstant RejectAt;
};

// Requests that could not even be queued for admission and will be failed with OVERLOADED once
// most of their operation timeout has elapsed, rather than immediately — an early reject just
// invites the client to retry into the same overload.
//
// Like TWaitQueue this keeps FIFO order in a std::list so that firing an entry out of order is
// O(1) instead of a scan of the whole queue.
class TDelayedRejectQueue {
    std::list<TDelayedReject> Order;
    THashMap<ui64, std::list<TDelayedReject>::iterator> ById;
    ui64 NextRejectId = 1;

public:
    size_t Size() const {
        return Order.size();
    }

    ui64 Enqueue(NActors::TActorId replyTo, TInstant rejectAt);

    // Returns the removed entry, or nullopt if it already fired or was cancelled.
    std::optional<TDelayedReject> Erase(ui64 rejectId);
};

}   // namespace NKikimr::NColumnShard::NFlowControl
