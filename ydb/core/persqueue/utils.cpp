#include "utils.h"

#include <deque>
#include <util/string/builder.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config) {
    if (!config.HasMeteringMode()) {
        // Only for federative and dedicated installations
        return 0;
    }
    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == config.GetMeteringMode()) {
        return 0;
    }
    if (config.GetPartitionConfig().HasStorageLimitBytes()) {
        return config.GetPartitionConfig().GetStorageLimitBytes();
    }
    return config.GetPartitionConfig().GetLifetimeSeconds() * config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
}

ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config) {
    if (!config.HasMeteringMode()) {
        // Only for federative and dedicated installations
        return 0;
    }
    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == config.GetMeteringMode()) {
        return 0;
    }
    return config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
}

bool SplitMergeEnabled(const NKikimrPQ::TPQTabletConfig& config) {
    return config.GetPartitionStrategy().GetMinPartitionCount() < config.GetPartitionStrategy().GetMaxPartitionCount(); // TODO
}

static constexpr ui64 PUT_UNIT_SIZE = 40960u; // 40Kb

ui64 PutUnitsSize(const ui64 size) {
    ui64 putUnitsCount = size / PUT_UNIT_SIZE;
    if (size % PUT_UNIT_SIZE != 0)
        ++putUnitsCount;    
    return putUnitsCount;        
}

const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config, const ui32 partitionId) {
    for(const auto& p : config.GetPartitions()) {
        if (partitionId == p.GetPartitionId()) {
            return &p;
        }
    }
    return nullptr;
}

void TPartitionGraph::Rebuild(const NKikimrPQ::TPQTabletConfig& config) {
    Partitions.clear();

    if (0 == config.AllPartitionsSize()) {
        return;
    }

    for (const auto& p : config.GetAllPartitions()) {
        Partitions.emplace(p.GetPartitionId(), p);
    }

    std::deque<Node*> queue;
    for(const auto& p : config.GetAllPartitions()) {
        auto& node = Partitions[p.GetPartitionId()];

        node.Children.reserve(p.ChildPartitionIdsSize());
        for (auto id : p.GetChildPartitionIds()) {
            node.Children.push_back(&Partitions[id]);
        }

        node.Parents.reserve(p.ParentPartitionIdsSize());
        for (auto id : p.GetParentPartitionIds()) {
            node.Parents.push_back(&Partitions[id]);
        }

        if (p.GetParentPartitionIds().empty()) {
            queue.push_back(&node);
        }
    }

    while(!queue.empty()) {
        auto* n = queue.front();
        queue.pop_front();

        bool allCompleted = true;
        for(auto* c : n->Parents) {
            if (c->HierarhicalParents.empty() && !c->Parents.empty()) {
                allCompleted = false;
                break;
            }
        }

        if (allCompleted) {
            for(auto* c : n->Parents) {
                n->HierarhicalParents.insert(c->HierarhicalParents.begin(), c->HierarhicalParents.end());
                n->HierarhicalParents.insert(c);
            }
            queue.insert(queue.end(), n->Children.begin(), n->Children.end());
        }
    }
}

std::optional<const TPartitionGraph::Node*> TPartitionGraph::GetPartition(ui32 id) const {
    auto it = Partitions.find(id);
    if (it == Partitions.end()) {
        return std::nullopt;
    }
    return std::optional(&it->second);
}

TPartitionGraph::Node::Node(const NKikimrPQ::TPQTabletConfig::TPartition& config) {
    Id = config.GetPartitionId();
    TabletId = config.GetTabletId();
}

} // NKikimr::NPQ
