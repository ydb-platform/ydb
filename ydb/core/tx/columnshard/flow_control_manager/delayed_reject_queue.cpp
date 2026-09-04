#include "delayed_reject_queue.h"

namespace NKikimr::NColumnShard::NFlowControl {

ui64 TDelayedRejectQueue::Enqueue(NActors::TActorId replyTo, TInstant rejectAt) {
    const ui64 rejectId = NextRejectId++;
    TDelayedReject reject;
    reject.RejectId = rejectId;
    reject.ReplyTo = replyTo;
    reject.RejectAt = rejectAt;
    Order.push_back(std::move(reject));
    ById.emplace(rejectId, std::prev(Order.end()));
    return rejectId;
}

std::optional<TDelayedReject> TDelayedRejectQueue::Erase(ui64 rejectId) {
    auto it = ById.find(rejectId);
    if (it == ById.end()) {
        return std::nullopt;
    }
    TDelayedReject reject = std::move(*it->second);
    Order.erase(it->second);
    ById.erase(it);
    return reject;
}

}   // namespace NKikimr::NColumnShard::NFlowControl
