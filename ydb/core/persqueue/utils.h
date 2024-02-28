#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config);
ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config);

bool SplitMergeEnabled(const NKikimrPQ::TPQTabletConfig& config);

ui64 PutUnitsSize(const ui64 size);

TString SourceIdHash(const TString& sourceId);

void Migrate(NKikimrPQ::TPQTabletConfig& config);
bool HasConsumer(const NKikimrPQ::TPQTabletConfig& config, const TString& consumerName);
size_t ConsumerCount(const NKikimrPQ::TPQTabletConfig& config);

bool IsImportantClient(const NKikimrPQ::TPQTabletConfig& config, const TString& consumerName);

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

private:
    std::unordered_map<ui32, Node> Partitions;
};

TPartitionGraph MakePartitionGraph(const NKikimrPQ::TPQTabletConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrPQ::TUpdateBalancerConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

} // NKikimr::NPQ
