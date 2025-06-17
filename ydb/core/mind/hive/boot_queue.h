#pragma once

#include "hive.h"
#include "tablet_info.h"

namespace NKikimr {
namespace NHive {

struct TBootQueue {
    struct TBootQueueRecord {
        enum EPriority {
            BasePriority = 0,
            ColumnShardPriority = 0,
            LeaderTabletPriority = 5,
            BlockStorePartitionPriority = 10,
            BlockStoreVolumePriority = 11,
            BlockStoreDiskRegistryPriority = 12,
            MediatorPriority = 20,
            BlobDepotPriority = 20,
            CoordinatorPriority = 20,
            SchemeShardPriority = 25,
            HivePriority = 30,
            MaxPriority = 40,
            MinPriority = -MaxPriority,
        };

        TTabletId TabletId;
        double Priority;
        TFollowerId FollowerId;
        TNodeId SuggestedNodeId;

        static double GetBootPriority(const TTabletInfo& tablet) {
            double priority = tablet.RestartsOften() ? MinPriority : BasePriority;
            priority += tablet.Weight;

            switch (tablet.GetTabletType()) {
            case TTabletTypes::Hive:
                priority += HivePriority;
                break;
            case TTabletTypes::SchemeShard:
                priority += SchemeShardPriority;
                break;
            case TTabletTypes::Mediator:
                priority += MediatorPriority;
                break;
            case TTabletTypes::Coordinator:
                priority += CoordinatorPriority;
                break;
            case TTabletTypes::BlobDepot:
                priority += BlobDepotPriority;
                break;
            case TTabletTypes::ColumnShard:
                priority += ColumnShardPriority;
                break;
            case TTabletTypes::BlockStoreDiskRegistry:
                priority += BlockStoreDiskRegistryPriority;
                break;
            case TTabletTypes::BlockStoreVolume:
                priority += BlockStoreVolumePriority;
                break;
            case TTabletTypes::BlockStorePartition:
            case TTabletTypes::BlockStorePartition2:
                priority += BlockStorePartitionPriority;
                break;
            default:
                if (tablet.IsLeader()) {
                    priority += LeaderTabletPriority;
                }
                break;
            }

            return priority;
        }

        bool operator <(const TBootQueueRecord& o) const {
            return Priority < o.Priority;
        }

        TBootQueueRecord(const TTabletInfo& tablet, TNodeId suggestedNodeId = 0);
    };

    static_assert(sizeof(TBootQueueRecord) <= 24);

    using TQueue = TPriorityQueue<TBootQueueRecord>;

    TQueue BootQueue;
    TQueue WaitQueue; // tablets from BootQueue waiting for new nodes

private:
    bool ProcessWaitQueue = false;
    bool NextFromWaitQueue = false;

public:
    void AddToBootQueue(TBootQueueRecord record);
    TBootQueueRecord PopFromBootQueue();
    void AddToWaitQueue(TBootQueueRecord record);
    void IncludeWaitQueue();
    void ExcludeWaitQueue();
    bool Empty() const;
    size_t Size() const;

    template<typename... Args>
    void EmplaceToBootQueue(Args&&... args) {
        BootQueue.emplace(args...);
    }

private:
    TQueue& GetCurrentQueue();
};

}
}
