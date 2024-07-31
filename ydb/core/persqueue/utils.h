#pragma once

#include <util/string/builder.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config);
ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config);

bool SplitMergeEnabled(const NKikimrPQ::TPQTabletConfig& config);

size_t CountActivePartitions(const ::google::protobuf::RepeatedPtrField< ::NKikimrPQ::TPQTabletConfig_TPartition >& partitions);

ui64 PutUnitsSize(const ui64 size);

TString SourceIdHash(const TString& sourceId);

void Migrate(NKikimrPQ::TPQTabletConfig& config);

// This function required for marking the code which required remove after 25-1
constexpr bool ReadRuleCompatible() { return true; }

bool HasConsumer(const NKikimrPQ::TPQTabletConfig& config, const TString& consumerName);
size_t ConsumerCount(const NKikimrPQ::TPQTabletConfig& config);

const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config, const ui32 partitionId);

// The graph of split-merge operations.
class TPartitionGraph {
public:
    struct Node {

        Node() = default;
        Node(Node&&) = default;
        Node(ui32 id, ui64 tabletId);
        Node(ui32 id, ui64 tabletId, const TString& from, const TString& to);

        ui32 Id;
        ui64 TabletId;
        TString From;
        TString To;

        // Direct parents of this node
        std::vector<Node*> Parents;
        // Direct children of this node
        std::vector<Node*> Children;
        // All parents include parents of parents and so on
        std::set<Node*> HierarhicalParents;

        bool IsRoot() const;
    };

    TPartitionGraph();
    TPartitionGraph(std::unordered_map<ui32, Node>&& partitions);

    const Node* GetPartition(ui32 id) const;
    std::set<ui32> GetActiveChildren(ui32 id) const;

    void Travers(const std::function<bool (ui32 id)>& func) const;
    void Travers(ui32 id, const std::function<bool (ui32 id)>& func, bool includeSelf = false) const;

private:
    std::unordered_map<ui32, Node> Partitions;
};

TPartitionGraph MakePartitionGraph(const NKikimrPQ::TPQTabletConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrPQ::TUpdateBalancerConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

} // NKikimr::NPQ
