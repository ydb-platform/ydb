#pragma once

#include <deque>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config);
ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config);

bool SplitMergeEnabled(const NKikimrPQ::TPQTabletConfig& config);

ui64 PutUnitsSize(const ui64 size);

TString SourceIdHash(const TString& sourceId);

const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config, const ui32 partitionId);

// The graph of split-merge operations. 
class TPartitionGraph {
public:
    struct Node {

        Node() = default;
        Node(Node&&) = default;
        Node(ui32 id, ui64 tabletId);

        ui32 Id;
        ui64 TabletId;

        // Direct parents of this node
        std::vector<Node*> Parents;
        // Direct children of this node
        std::vector<Node*> Children;
        // All parents include parents of parents and so on
        std::set<Node*> HierarhicalParents;
    };

    TPartitionGraph();
    TPartitionGraph(std::unordered_map<ui32, Node>&& partitions);

    const Node* GetPartition(ui32 id) const;
    std::set<ui32> GetActiveChildren(ui32 id) const;
    const std::vector<const Node*>& GetRoots() const;

private:
    std::unordered_map<ui32, Node> Partitions;
    std::vector<const Node*> Roots;
};

TPartitionGraph MakePartitionGraph(const NKikimrPQ::TPQTabletConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrPQ::TUpdateBalancerConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

template<typename TPartition>
inline int GetPartitionId(TPartition p) {
    return p.GetPartitionId();
}

template<>
inline int GetPartitionId(NKikimrPQ::TUpdateBalancerConfig::TPartition p) {
    return p.GetPartition();
}

template<typename TPartition, typename TCollection = ::google::protobuf::RepeatedPtrField<TPartition>>
std::unordered_map<ui32, TPartitionGraph::Node> BuildGraph(const TCollection& partitions) {
    std::unordered_map<ui32, TPartitionGraph::Node> result;

    if (0 == partitions.size()) {
        return result;
    }

    for (const auto& p : partitions) {
        result.emplace(GetPartitionId(p), TPartitionGraph::Node(GetPartitionId(p), p.GetTabletId()));
    }

    std::deque<TPartitionGraph::Node*> queue;
    for(const auto& p : partitions) {
        auto& node = result[GetPartitionId(p)];

        node.Children.reserve(p.ChildPartitionIdsSize());
        for (auto id : p.GetChildPartitionIds()) {
            node.Children.push_back(&result[id]);
        }

        node.Parents.reserve(p.ParentPartitionIdsSize());
        for (auto id : p.GetParentPartitionIds()) {
            node.Parents.push_back(&result[id]);
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


} // NKikimr::NPQ
