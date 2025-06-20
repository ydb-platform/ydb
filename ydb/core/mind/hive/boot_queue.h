#pragma once

#include "hive.h"
#include "tablet_info.h"

namespace NKikimr {
namespace NHive {

struct TBootQueue {
    struct TBootQueueRecord {
        TTabletId TabletId;
        double Priority;
        TFollowerId FollowerId;
        TNodeId SuggestedNodeId;

        bool operator <(const TBootQueueRecord& o) const {
            return Priority < o.Priority;
        }

        TBootQueueRecord(const TTabletInfo& tablet, double priority, TNodeId suggestedNodeId);
    };

    static_assert(sizeof(TBootQueueRecord) <= 24);

    using TQueue = TPriorityQueue<TBootQueueRecord>;
    using TTabletTypeToBootPriority = TMap<TTabletTypes::EType, double>;

    TQueue BootQueue;
    TQueue WaitQueue; // tablets from BootQueue waiting for new nodes
private:
    bool ProcessWaitQueue = false;
    bool NextFromWaitQueue = false;

    TTabletTypeToBootPriority TabletTypeToBootPriority;

public:
    void AddToBootQueue(TBootQueueRecord record);
    void AddToBootQueue(const TTabletInfo &tablet, TNodeId node);
    void UpdateTabletBootQueuePriorities(const NKikimrConfig::THiveConfig& hiveConfig);
    TBootQueueRecord PopFromBootQueue();
    void AddToWaitQueue(TBootQueueRecord record);
    void IncludeWaitQueue();
    void ExcludeWaitQueue();
    bool Empty() const;
    size_t Size() const;

private:
    TQueue& GetCurrentQueue();
    double GetBootPriority(const TTabletInfo& tablet) const;
};

}
}
