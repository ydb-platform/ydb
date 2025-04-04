#pragma once
#include "abstract.h"
#include "graph_execute.h"

#include <library/cpp/json/writer/json_value.h>
#include <util/digest/fnv.h>
#include <util/digest/numeric.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA {
class TCalculationProcessor;
}

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {
class TCompiledGraph;
}

namespace NKikimr::NArrow::NSSA::NGraph::NOptimization {

class TResourceAddress {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY_DEF(TString, SubColumnName);

public:
    TResourceAddress(const ui32 columnId, const TString& subColumnName = "")
        : ColumnId(columnId)
        , SubColumnName(subColumnName) {
    }

    bool operator<(const TResourceAddress& item) const {
        return std::tie(ColumnId, SubColumnName) < std::tie(item.ColumnId, item.SubColumnName);
    }

    bool operator==(const TResourceAddress& item) const {
        return std::tie(ColumnId, SubColumnName) == std::tie(item.ColumnId, item.SubColumnName);
    }

    explicit operator size_t() const {
        if (SubColumnName) {
            return CombineHashes<ui64>(ColumnId, FnvHash<ui64>(SubColumnName.data(), SubColumnName.size()));
        } else {
            return ColumnId;
        }
    }

    TString DebugString() const;
};

enum class EOptimizerMarkers {
    FetchMerged
};

class TGraphNode {
private:
    std::set<EOptimizerMarkers> OptimizerMarkers;
    YDB_READONLY(i64, Identifier, 0);
    YDB_READONLY_DEF(std::shared_ptr<IResourceProcessor>, Processor);
    class TAddress {
    private:
        YDB_READONLY(ui32, ResourceId, 0);
        YDB_READONLY(ui64, NodeId, 0);

    public:
        TAddress(const i64 nodeId, const ui32 resourceId)
            : ResourceId(resourceId)
            , NodeId(nodeId) {
        }

        TAddress AnotherNodeId(const i64 nodeId) const {
            return TAddress(ResourceId, nodeId);
        }

        bool operator<(const TAddress& item) const {
            return std::tie(ResourceId, NodeId) < std::tie(item.ResourceId, item.NodeId);
        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("r", ResourceId);
            result.InsertValue("n", NodeId);
            return result;
        }
    };
    std::map<TAddress, TGraphNode*> InputEdges;
    std::map<TAddress, TGraphNode*> OutputEdges;

public:

    bool AddOptimizerMarker(const EOptimizerMarkers marker) {
        return OptimizerMarkers.emplace(marker).second;
    }

    bool HasOptimizerMarker(const EOptimizerMarkers marker) {
        return OptimizerMarkers.contains(marker);
    }

    void AddEdgeTo(TGraphNode* to, const ui32 resourceId);
    void AddEdgeFrom(TGraphNode* from, const ui32 resourceId);
    void RemoveEdgeTo(const ui32 identifier, const ui32 resourceId);
    void RemoveEdgeFrom(const ui32 identifier, const ui32 resourceId);
    bool HasEdgeFrom(const ui32 nodeId, const ui32 resourceId) const;
    bool HasEdgeTo(const ui32 nodeId, const ui32 resourceId) const;
    bool IsDisconnected() const {
        return InputEdges.empty() && OutputEdges.empty();
    }

    bool Is(const EProcessorType type) const {
        return GetProcessor()->GetProcessorType() == type;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("id", Identifier);
        auto& inputArr = result.InsertValue("input", NJson::JSON_ARRAY);
        for (auto&& i : InputEdges) {
            inputArr.AppendValue(i.first.DebugJson());
        }
        auto& outputArr = result.InsertValue("output", NJson::JSON_ARRAY);
        for (auto&& i : OutputEdges) {
            outputArr.AppendValue(i.first.DebugJson());
        }
        return result;
    }

    const std::map<TAddress, TGraphNode*>& GetInputEdges() const {
        return InputEdges;
    }

    const std::map<TAddress, TGraphNode*>& GetOutputEdges() const {
        return OutputEdges;
    }

    TGraphNode(const ui32 id, const std::shared_ptr<IResourceProcessor>& processor)
        : Identifier(id)
        , Processor(processor) {
        AFL_VERIFY(Processor);
    }

    template <class TProcessor>
    std::shared_ptr<TProcessor> GetProcessorAs() const {
        return std::static_pointer_cast<TProcessor>(Processor);
    }
};

class TGraph {
private:
    ui32 NextResourceId = 0;
    THashSet<ui32> FetchersMerged;
    const IColumnResolver& Resolver;
    std::map<ui64, std::shared_ptr<TGraphNode>> Nodes;
    THashMap<TResourceAddress, TGraphNode*> Producers;
    THashSet<ui32> IndexesConstructed;
    THashSet<ui32> HeaderCheckConstructed;
    ui32 NodeId = 0;
    TGraphNode* GetProducerVerified(const TResourceAddress& resourceId) const {
        auto it = Producers.find(resourceId);
        AFL_VERIFY(it != Producers.end());
        return it->second;
    }
    std::optional<TResourceAddress> GetOriginalAddress(TGraphNode* condNode) const;
    TConclusion<bool> OptimizeForFetchSubColumns(TGraphNode* condNode);
    TConclusion<bool> OptimizeConditionsForHeadersCheck(TGraphNode* condNode);

    TConclusion<bool> OptimizeConditionsForStream(TGraphNode* condNode);
    TConclusion<bool> OptimizeConditionsForIndexes(TGraphNode* condNode);
    TConclusion<bool> OptimizeIndexesToApply(TGraphNode* condNode);
    TConclusion<bool> OptimizeFilterWithCoalesce(TGraphNode* cNode);
    TConclusion<bool> OptimizeFilterWithAnd(TGraphNode* filterNode, TGraphNode* filterArg, const std::shared_ptr<TCalculationProcessor>& calc);
    TConclusion<bool> OptimizeMergeFetching(TGraphNode* baseNode);


    bool HasEdge(const TGraphNode* from, const TGraphNode* to, const ui32 resourceId) const;
    void AddEdge(TGraphNode* from, TGraphNode* to, const ui32 resourceId);
    void RemoveEdge(TGraphNode* from, TGraphNode* to, const ui32 resourceId);
    void RemoveNode(const ui32 idenitifier);
    THashMap<ui32, TGraphNode*> GetBranch(TGraphNode* from, const bool backOnly) const;
    void RemoveBranch(TGraphNode* from, const bool backOnly);
    [[nodiscard]] std::shared_ptr<TGraphNode> AddNode(const std::shared_ptr<IResourceProcessor>& processor) {
        auto result = std::make_shared<TGraphNode>(NodeId++, processor);
        Nodes.emplace(result->GetIdentifier(), result);
        return result;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& nodesArr = result.InsertValue("nodes", NJson::JSON_ARRAY);
        for (auto&& i : Nodes) {
            nodesArr.AppendValue(i.second->DebugJson());
        }
        return result;
    }

    std::shared_ptr<NExecution::TCompiledGraph> Compile();

    TConclusionStatus Collapse();

    void RegisterProducer(const TResourceAddress& resourceId, TGraphNode* node) {
        AFL_VERIFY(Producers.emplace(resourceId, node).second);
    }

    void ResetProducer(const TResourceAddress& resourceId, TGraphNode* node) {
        auto it = Producers.find(resourceId);
        AFL_VERIFY(it != Producers.end());
        it->second = node;
    }

    ui32 BuildNextResourceId() {
        return ++NextResourceId;
    }

    TGraph(std::vector<std::shared_ptr<IResourceProcessor>>&& processors, const IColumnResolver& resolver);

public:
    const std::map<ui64, std::shared_ptr<TGraphNode>>& GetNodes() const {
        return Nodes;
    }

    class TBuilder {
    private:
        std::vector<std::shared_ptr<IResourceProcessor>> Processors;
        const IColumnResolver& Resolver;
        bool Finished = false;

    public:
        TBuilder(const IColumnResolver& resolver)
            : Resolver(resolver) {
        }

        void Add(const std::shared_ptr<IResourceProcessor>& processor) {
            AFL_VERIFY(!Finished);
            Processors.emplace_back(processor);
        }

        TConclusion<std::shared_ptr<NExecution::TCompiledGraph>> Finish() {
            AFL_VERIFY(!Finished);
            Finished = true;
            TGraph graph(std::move(Processors), Resolver);
            graph.Collapse();
            return graph.Compile();
        }
    };
};

}   // namespace NKikimr::NArrow::NSSA::NGraph::NOptimization
