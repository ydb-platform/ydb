#pragma once

#include "hive.h"
#include "tablet_info.h"

namespace NKikimr {
namespace NHive {

struct TBootQueue {
    struct TBootQueueRecord {
        TFullTabletId TabletId;
        double Priority;

        static double GetBootPriority(const TTabletInfo& tablet) {
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
            default:
                if (tablet.IsLeader()) {
                    priority = 1;
                }
                break;
            }
            priority += tablet.Weight;
            if (tablet.RestartsOften()) {
               priority -= 5;
            }
            return priority;
        }

        bool operator <(const TBootQueueRecord& o) const {
            return Priority < o.Priority;
        }

        TBootQueueRecord(const TTabletInfo& tablet)
            : TabletId(tablet.GetFullTabletId())
            , Priority(GetBootPriority(tablet))
        {
        }
    };

    std::priority_queue<TBootQueueRecord, std::vector<TBootQueueRecord>> BootQueue;
    std::deque<TBootQueueRecord> WaitQueue; // tablets from BootQueue waiting for new nodes

    void AddToBootQueue(TBootQueueRecord record);
    TBootQueueRecord PopFromBootQueue();
    void AddToWaitQueue(TBootQueueRecord record);
    void MoveFromWaitQueueToBootQueue();
};

}
}
