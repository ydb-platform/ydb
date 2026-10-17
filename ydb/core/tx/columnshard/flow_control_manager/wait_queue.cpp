#include "wait_queue.h"

namespace NKikimr::NColumnShard::NFlowControl {

ui64 TWaitQueue::Enqueue(TWaiter&& waiter) {
    const ui64 waiterId = NextWaiterId++;
    waiter.WaiterId = waiterId;
    for (const ui32 nodeId : waiter.TargetNodes) {
        ++WaiterCountByNode[nodeId];
    }
    Order.push_back(std::move(waiter));
    ById.emplace(waiterId, std::prev(Order.end()));
    return waiterId;
}

TWaiter* TWaitQueue::Find(ui64 waiterId) {
    auto it = ById.find(waiterId);
    return it == ById.end() ? nullptr : &*it->second;
}

std::optional<TWaiter> TWaitQueue::Erase(ui64 waiterId) {
    auto it = ById.find(waiterId);
    if (it == ById.end()) {
        return std::nullopt;
    }
    TWaiter waiter = std::move(*it->second);
    Order.erase(it->second);
    ById.erase(it);
    for (const ui32 nodeId : waiter.TargetNodes) {
        auto countIt = WaiterCountByNode.find(nodeId);
        if (countIt == WaiterCountByNode.end()) {
            continue;
        }
        if (countIt->second <= 1) {
            WaiterCountByNode.erase(countIt);
        } else {
            --countIt->second;
        }
    }
    return waiter;
}

bool TWaitQueue::HasWaitersOnAnyNode(const TVector<ui32>& nodes) const {
    for (const ui32 nodeId : nodes) {
        if (const auto* count = WaiterCountByNode.FindPtr(nodeId)) {
            if (*count > 0) {
                return true;
            }
        }
    }
    return false;
}

}   // namespace NKikimr::NColumnShard::NFlowControl
