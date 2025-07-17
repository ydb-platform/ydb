#pragma once

#include <deque>
#include <util/datetime/base.h>
#include <util/string/builder.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

ui64 TopicPartitionReserveSize(const NKikimrPQ::TPQTabletConfig& config);
ui64 TopicPartitionReserveThroughput(const NKikimrPQ::TPQTabletConfig& config);

bool MirroringEnabled(const NKikimrPQ::TPQTabletConfig& config);
bool SplitMergeEnabled(const NKikimrPQ::TPQTabletConfig& config);

size_t CountActivePartitions(const ::google::protobuf::RepeatedPtrField< ::NKikimrPQ::TPQTabletConfig_TPartition >& partitions);

ui64 PutUnitsSize(const ui64 size);

TString SourceIdHash(const TString& sourceId);

void Migrate(NKikimrPQ::TPQTabletConfig& config);

bool HasConsumer(const NKikimrPQ::TPQTabletConfig& config, const TString& consumerName);
size_t ConsumerCount(const NKikimrPQ::TPQTabletConfig& config);

const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config, const ui32 partitionId);

// The graph of split-merge operations.
class TPartitionGraph {
public:
    using TPtr = std::shared_ptr<TPartitionGraph>;

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
        std::vector<Node*> DirectParents;
        // Direct children of this node
        std::vector<Node*> DirectChildren;
        // All parents include parents of parents and so on
        std::set<Node*> AllParents;
        // All children include children of children and so on
        std::set<Node*> AllChildren;

        bool IsRoot() const;
        bool IsParent(ui32 partitionId) const;
    };

    TPartitionGraph();
    TPartitionGraph(std::unordered_map<ui32, Node>&& partitions);

    const Node* GetPartition(ui32 id) const;
    std::set<ui32> GetActiveChildren(ui32 id) const;

    void Travers(const std::function<bool (ui32 id)>& func) const;
    void Travers(ui32 id, const std::function<bool (ui32 id)>& func, bool includeSelf = false) const;

    bool Empty() const;
    operator bool() const;

    TString DebugString() const;

private:
    std::unordered_map<ui32, Node> Partitions;
};

TPartitionGraph MakePartitionGraph(const NKikimrPQ::TPQTabletConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrPQ::TUpdateBalancerConfig& config);
TPartitionGraph MakePartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

TPartitionGraph::TPtr MakeSharedPartitionGraph(const NKikimrPQ::TPQTabletConfig& config);
TPartitionGraph::TPtr MakeSharedPartitionGraph(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

class TLastCounter {
    static constexpr size_t MaxValueCount = 2;

public:
    void Use(const TString& value, const TInstant& now);
    size_t Count(const TInstant& expirationTime);
    const TString& LastValue() const;

private:
    struct Data {
        TInstant LastUseTime;
        TString Value;
    };
    std::deque<Data> Values;
};


} // NKikimr::NPQ
