#include "boot_queue.h"
#include "leader_tablet_info.h"

namespace NKikimr {
namespace NHive {

TBootQueue::TBootQueueRecord::TBootQueueRecord(const TTabletInfo& tablet, TNodeId suggestedNodeId)
    : TabletId(tablet.GetLeader().Id)
    , Priority(GetBootPriority(tablet))
    , FollowerId(tablet.IsLeader() ? 0 : tablet.AsFollower().Id)
    , SuggestedNodeId(suggestedNodeId)
{
}

void TBootQueue::AddToBootQueue(TBootQueueRecord record) {
    BootQueue.push(record);
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

}
}
