#pragma once

#include "partition_id.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config);
ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config);

bool SplitMergeEnabled(const NKikimrPQ::TPQTabletConfig& config);

ui64 PutUnitsSize(const ui64 size);

TString SourceIdHash(const TString& sourceId);

const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config, const TPartitionId& partitionId);

// The graph of split-merge operations. 
class TPartitionGraph {
public:
    struct Node {

        Node() = default;
        Node(Node&&) = default;
        Node(const TPartitionId& id, ui64 tabletId);

        TPartitionId Id;
        ui64 TabletId;

        // Direct parents of this node
        std::vector<Node*> Parents;
        // Direct children of this node
        std::vector<Node*> Children;
        // All parents include parents of parents and so on
        std::set<Node*> HierarhicalParents;
    };

    TPartitionGraph();
    TPartitionGraph(std::unordered_map<TPartitionId, Node>&& partitions);

    const Node* GetPartition(const TPartitionId& id) const;
    std::set<TPartitionId> GetActiveChildren(const TPartitionId& id) const;

private:
    std::unordered_map<TPartitionId, Node> Partitions;
};

TPartitionGraph MakePartitionGraph(const NKikimrPQ::TPQTabletConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

} // NKikimr::NPQ
