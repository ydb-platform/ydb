#include "boot_queue.h"
#include "leader_tablet_info.h"

namespace NKikimr {
namespace NHive {

namespace {

double GetDefaultBootPriority(const TTabletInfo &tablet) {
    double priority = 0;
    switch (tablet.GetTabletType()) {
    case TTabletTypes::Hive:
        priority = 4;
        break;
    case TTabletTypes::SchemeShard:
        priority = 3;
        break;
    case TTabletTypes::Mediator:
    case TTabletTypes::Coordinator:
    case TTabletTypes::BlobDepot:
        priority = 2;
        break;
    case TTabletTypes::ColumnShard:
        priority = 0;
        break;
    default:
        if (tablet.IsLeader()) {
            priority = 1;
        }
        break;
    }

    return priority;
}

} // namespace

TBootQueue::TBootQueueRecord::TBootQueueRecord(const TTabletInfo& tablet, double priority, TNodeId suggestedNodeId)
    : TabletId(tablet.GetLeader().Id)
    , Priority(priority)
    , FollowerId(tablet.IsLeader() ? 0 : tablet.AsFollower().Id)
    , SuggestedNodeId(suggestedNodeId)
{
}

void TBootQueue::AddToBootQueue(TBootQueueRecord record) {
    BootQueue.push(record);
}

void TBootQueue::AddToBootQueue(const TTabletInfo &tablet, TNodeId node) {
    double priority = GetBootPriority(tablet);
    BootQueue.emplace(tablet, priority, node);
}

void TBootQueue::UpdateTabletBootQueuePriorities(const NKikimrConfig::THiveConfig& hiveConfig) {
    TTabletTypeToBootPriority updated;
    for (const auto& entry : hiveConfig.GetTabletTypeToBootPriority()) {
        updated.emplace(entry.GetTabletType(), entry.GetPriority());
    }
    TabletTypeToBootPriority = std::move(updated);
}

TBootQueue::TBootQueueRecord TBootQueue::PopFromBootQueue() {
    TQueue& currentQueue = GetCurrentQueue();
    TBootQueueRecord record = currentQueue.top();
    currentQueue.pop();
    if (ProcessWaitQueue) {
        NextFromWaitQueue = !NextFromWaitQueue;
    }
    return record;
}

void TBootQueue::AddToWaitQueue(TBootQueueRecord record) {
    WaitQueue.push(record);
}

void TBootQueue::IncludeWaitQueue() {
    ProcessWaitQueue = true;
}

void TBootQueue::ExcludeWaitQueue() {
    ProcessWaitQueue = false;
}

bool TBootQueue::Empty() const {
    if (ProcessWaitQueue) {
        return BootQueue.empty() && WaitQueue.empty();
    } else {
        return BootQueue.empty();
    }
}

size_t TBootQueue::Size() const {
    if (ProcessWaitQueue) {
        return BootQueue.size() + WaitQueue.size();
    } else {
        return BootQueue.size();
    }
}

TBootQueue::TQueue& TBootQueue::GetCurrentQueue() {
    if (BootQueue.empty()) {
        return WaitQueue;
    }
    if (WaitQueue.empty()) {
        return BootQueue;
    }
    if (ProcessWaitQueue && NextFromWaitQueue) {
        return WaitQueue;
    }
    return BootQueue;
}

double TBootQueue::GetBootPriority(const TTabletInfo& tablet) const {
    double priority = GetDefaultBootPriority(tablet);
    auto tabletType = tablet.GetTabletType();
    const auto* it = TabletTypeToBootPriority.FindPtr(tabletType);
    if (it) {
        priority = *it;
    }
    priority += tablet.Weight;
    if (tablet.RestartsOften()) {
        priority -= 5;
    }
    return priority;
}

}
}
