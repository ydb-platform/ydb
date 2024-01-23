#pragma once

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
        Node(const NKikimrPQ::TPQTabletConfig::TPartition& config);

        ui32 Id;
        ui64 TabletId;

        // Direct parents of this node
        std::vector<Node*> Parents;
        // Direct children of this node
        std::vector<Node*> Children;
        // All parents include parents of parents and so on
        std::set<Node*> HierarhicalParents;
    };

    void Rebuild(const NKikimrPQ::TPQTabletConfig& config);

    std::optional<const Node*> GetPartition(ui32 id) const;
private:
    std::unordered_map<ui32, Node> Partitions;
};

} // NKikimr::NPQ
