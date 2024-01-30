#include "utils.h"

#include <deque>
#include <util/string/builder.h>

//#include <ydb/core/base/appdata_fwd.h>
//#include <ydb/core/base/feature_flags.h>
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
    return 0 < config.GetPartitionStrategy().GetMaxPartitionCount();
}

static constexpr ui64 PUT_UNIT_SIZE = 40960u; // 40Kb

ui64 PutUnitsSize(const ui64 size) {
    ui64 putUnitsCount = size / PUT_UNIT_SIZE;
    if (size % PUT_UNIT_SIZE != 0)
        ++putUnitsCount;    
    return putUnitsCount;        
}

const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config, const TPartitionId& partitionId) {
    for(const auto& p : config.GetPartitions()) {
        if (partitionId.OriginalPartitionId == p.GetPartitionId()) {
            return &p;
        }
    }
    return nullptr;
}

TPartitionGraph::TPartitionGraph() {
}

TPartitionGraph::TPartitionGraph(std::unordered_map<TPartitionId, Node>&& partitions) {
    Partitions = std::move(partitions);
}

const TPartitionGraph::Node* TPartitionGraph::GetPartition(const TPartitionId& id) const {
    auto it = Partitions.find(id);
    if (it == Partitions.end()) {
        return nullptr;
    }
    return &it->second;
}

std::set<TPartitionId> TPartitionGraph::GetActiveChildren(const TPartitionId& id) const {
    const auto* p = GetPartition(id);
    if (!p) {
        return {};
    }

    std::deque<const Node*> queue;
    queue.push_back(p);

    std::set<TPartitionId> result;
    while(!queue.empty()) {
        const auto* n = queue.front();
        queue.pop_front();

        if (n->Children.empty()) {
            result.emplace(n->Id);
        } else {
            queue.insert(queue.end(), n->Children.begin(), n->Children.end());
        }
    }

    return result;
}

template<typename TPartition>
std::unordered_map<TPartitionId, TPartitionGraph::Node> BuildGraph(const ::google::protobuf::RepeatedPtrField<TPartition>& partitions) {
    std::unordered_map<TPartitionId, TPartitionGraph::Node> result;

    if (0 == partitions.size()) {
        return result;
    }

    for (const auto& p : partitions) {
        TPartitionId partitionId(p.GetPartitionId());
        result.emplace(partitionId, TPartitionGraph::Node(partitionId, p.GetTabletId()));
    }

    std::deque<TPartitionGraph::Node*> queue;
    for(const auto& p : partitions) {
        TPartitionId partitionId(p.GetPartitionId());
        auto& node = result[partitionId];

        node.Children.reserve(p.ChildPartitionIdsSize());
        for (auto id : p.GetChildPartitionIds()) {
            TPartitionId partitionId(id);
            node.Children.push_back(&result[partitionId]);
        }

        node.Parents.reserve(p.ParentPartitionIdsSize());
        for (auto id : p.GetParentPartitionIds()) {
            TPartitionId partitionId(id);
            node.Parents.push_back(&result[partitionId]);
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

    return result;
}


TPartitionGraph::Node::Node(const TPartitionId& id, ui64 tabletId)
    : Id(id)
    , TabletId(tabletId) {
}

TPartitionGraph MakePartitionGraph(const NKikimrPQ::TPQTabletConfig& config) {
    return TPartitionGraph(BuildGraph<NKikimrPQ::TPQTabletConfig::TPartition>(config.GetAllPartitions()));
}

TPartitionGraph MakePartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config) {
    return TPartitionGraph(BuildGraph<NKikimrSchemeOp::TPersQueueGroupDescription::TPartition>(config.GetPartitions()));
}

} // NKikimr::NPQ
