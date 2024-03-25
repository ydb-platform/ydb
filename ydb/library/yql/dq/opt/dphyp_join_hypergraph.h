#include <bitset>
#include <vector>

#include <ydb/library/yql/core/yql_cost_function.h>

namespace NYql::NDq {

template <typename TNodeSet>
class TJoinHypergraph {
public:
    struct TEdge {
        TEdge(const TNodeSet& left, const TNodeSet& right)
            : Left(left)
            , Right(right)
        {}

        TNodeSet Left;
        TNodeSet Right;
    };

    struct TNode {
        TNodeSet SimpleNeighborhood;
        TVector<TEdge*> ComplexEdges;
    };

public:
    void AddNode(const TString& relationName) {
        if (NodeIdByRelationName_.contains(relationName)) {
            return;
        }

        size_t nodeId = Nodes_.size(); 
        NodeIdByRelationName_.insert({relationName, nodeId});
        Nodes_.push_back({});
    }

    void AddEdge(TEdge) {
    }

private:
    THashMap<TString, size_t> NodeIdByRelationName_;

    TVector<TNode> Nodes_;
    TVector<TEdge> Edges_;
};

} // namespace NYql::NDq
