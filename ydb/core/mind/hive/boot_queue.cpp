#include "boot_queue.h"
#include "leader_tablet_info.h"

namespace NKikimr {
namespace NHive {

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
    // Fill default priorities
    TTabletTypeToBootPriority updated{
        {TTabletTypes::Hive, 4},
        {TTabletTypes::SchemeShard, 3},
        {TTabletTypes::Mediator, 2},
        {TTabletTypes::Coordinator, 2},
        {TTabletTypes::BlobDepot, 2},
        {TTabletTypes::ColumnShard, 0},
    };
    // Update priorities from config
    for (const auto& entry : hiveConfig.GetTabletTypeToBootPriority()) {
        updated.insert_or_assign(entry.GetTabletType(), entry.GetPriority());
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
    double priority = 0;

    if (tablet.IsLeader()) {
        priority = 1;
        auto tabletType = tablet.GetTabletType();
        const auto* it = TabletTypeToBootPriority.FindPtr(tabletType);
        if (it) {
            priority = *it;
        }
    }

    priority += tablet.Weight;
    if (tablet.RestartsOften()) {
        priority -= 5;
    }
    return priority;
}

}
}
