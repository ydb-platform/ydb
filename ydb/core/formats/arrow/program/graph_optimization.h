#pragma once
#include "abstract.h"
#include "graph_dependency.h"
#include "graph_execute.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/sub_column_name.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/digest/fnv.h>
#include <util/digest/numeric.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

#include <ranges>

namespace NKikimr::NArrow::NSSA {
class TCalculationOperation;
}

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {
class TCompiledGraph;
}

namespace NKikimr::NArrow::NSSA::NGraph::NOptimization {

class TResourceAddress {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    NAccessor::NSubColumns::TCanonicalSubColumnName SubColumnName;

public:
    const NAccessor::NSubColumns::TCanonicalSubColumnName& GetSubColumnName() const {
        return SubColumnName;
    }

    TResourceAddress(const ui32 columnId, const NAccessor::NSubColumns::TCanonicalSubColumnName& subColumnName = {})
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
            return CombineHashes<ui64>(ColumnId, SubColumnName.GetHash());
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
    YDB_READONLY_DEF(std::shared_ptr<IProgramOperation>, Processor);
    class TAddress {
    private:
        TDependency Dependency;
        YDB_READONLY(ui64, NodeId, 0);

    public:
        TAddress(const i64 nodeId, const TDependency& dependency)
            : Dependency(dependency)
            , NodeId(nodeId) {
        }

        const TDependency& GetDependency() const {
            return Dependency;
        }

        bool operator<(const TAddress& item) const {
            return std::tie(Dependency, NodeId) < std::tie(item.Dependency, item.NodeId);
        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("kind", Dependency.IsData() ? "data" : "execution");
            if (Dependency.IsData()) {
                result.InsertValue("r", Dependency.GetResourceId());
            }
            result.InsertValue("n", NodeId);
            return result;
        }
    };
    std::map<TAddress, TGraphNode*> InputDependencies;
    std::map<TAddress, TGraphNode*> OutputDependencies;

public:

    bool AddOptimizerMarker(const EOptimizerMarkers marker) {
        return OptimizerMarkers.emplace(marker).second;
    }

    bool HasOptimizerMarker(const EOptimizerMarkers marker) {
        return OptimizerMarkers.contains(marker);
    }

    // Update both ends of a dependency together.
    void AddDependencyTo(TGraphNode& to, const TDependency dependency);
    void RemoveDependencyTo(TGraphNode& to, const TDependency dependency);
    bool IsDisconnected() const {
        return InputDependencies.empty() && OutputDependencies.empty();
    }

    bool Is(const EProcessorType type) const {
        return GetProcessor()->GetProcessorType() == type;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("id", Identifier);
        auto& inputArr = result.InsertValue("input", NJson::JSON_ARRAY);
        for (auto&& i : InputDependencies) {
            inputArr.AppendValue(i.first.DebugJson());
        }
        auto& outputArr = result.InsertValue("output", NJson::JSON_ARRAY);
        for (auto&& i : OutputDependencies) {
            outputArr.AppendValue(i.first.DebugJson());
        }
        return result;
    }

    const std::map<TAddress, TGraphNode*>& GetInputDependencies() const {
        return InputDependencies;
    }

    const std::map<TAddress, TGraphNode*>& GetOutputDependencies() const {
        return OutputDependencies;
    }

    auto GetInputDataDependencies() const {
        return InputDependencies | std::views::filter([](const auto& item) { return item.first.GetDependency().IsData(); });
    }

    auto GetOutputDataDependencies() const {
        return OutputDependencies | std::views::filter([](const auto& item) { return item.first.GetDependency().IsData(); });
    }

    size_t GetOutputDataDependenciesCount() const {
        return std::ranges::distance(GetOutputDataDependencies());
    }

    TGraphNode(const ui32 id, const std::shared_ptr<IProgramOperation>& processor)
        : Identifier(id)
        , Processor(processor) {
        YDB_LOG_VERIFY(Processor);
    }

    template <class TProcessor>
    std::shared_ptr<TProcessor> GetProcessorAs() const {
        return std::static_pointer_cast<TProcessor>(Processor);
    }
};

class TGraph {
private:
    TResourceId NextResourceId = 0;
    THashSet<ui32> FetchersMerged;
    bool ReserveIndexMemory = false;
    const IColumnResolver& Resolver;
    std::map<ui64, std::shared_ptr<TGraphNode>> Nodes;
    THashMap<TResourceAddress, TGraphNode*> Producers;
    THashSet<ui32> IndexesConstructed;
    THashSet<ui32> HeaderCheckConstructed;
    ui32 NodeId = 0;
    TGraphNode* GetProducerVerified(const TResourceAddress& resourceId) const {
        auto it = Producers.find(resourceId);
        YDB_LOG_VERIFY(it != Producers.end());
        return it->second;
    }
    std::optional<TResourceAddress> GetOriginalAddress(TGraphNode* condNode) const;
    TConclusion<bool> OptimizeForFetchSubColumns(TGraphNode* condNode);
    TConclusion<bool> OptimizeConditionsForHeadersCheck(TGraphNode* condNode);

    TConclusion<bool> FlattenNestedLogicalOperations(TGraphNode* condNode);
    TConclusion<bool> OptimizeConditionsForIndexes(TGraphNode* condNode);
    TConclusion<bool> OptimizeConditionsForPatternHints(TGraphNode* condNode);
    TConclusion<bool> OptimizeIndexesToApply(TGraphNode* condNode);
    TConclusion<bool> OptimizeFilterWithCoalesce(TGraphNode* cNode);
    TConclusion<bool> OptimizeFilterWithAnd(TGraphNode* filterNode, TGraphNode* filterArg, const std::shared_ptr<TCalculationOperation>& calc);
    TConclusion<bool> OptimizeMergeFetching(TGraphNode* baseNode);
    TConclusion<bool> OptimizeForFetchDictionaryOnly(TGraphNode* node, const THashSet<ui32>& requiredDataColumnIds);
    /** The DISTINCT marker key is `JsonValue(dataColumnId, const path)` computed over the dedicated single sub-column
     *  fetch of `dataColumnId` (built by OptimizeForFetchSubColumns), and no other fetch of `dataColumnId` exists.
     *  Returns that FetchOriginalData node, nullptr otherwise. Walks edges only: `Producers` is stale for sub-columns. */
    TGraphNode* FindSingleSubColumnJsonKeyFetch(TGraphNode* markerNode, const ui32 dataColumnId) const;
    /** Marks the FetchOriginalData node that feeds the producer of `columnId` as dictionary-only. */
    bool SetDictionaryOnlyOnFetch(const ui32 columnId);


    void AddDependency(TGraphNode& from, TGraphNode& to, const TDependency dependency);
    void RemoveDependency(TGraphNode& from, TGraphNode& to, const TDependency dependency);
    void RemoveNode(const ui32 idenitifier);
    THashMap<ui32, TGraphNode*> GetBranch(TGraphNode* from, const bool backOnly) const;
    void RemoveBranch(TGraphNode* from, const bool backOnly);
    /** Column ids that are fetched as data in the entire graph (all FetchOriginalData nodes). */
    THashSet<ui32> CollectRequiredDataColumnIdsFromGraph() const;
    [[nodiscard]] std::shared_ptr<TGraphNode> AddNode(const std::shared_ptr<IProgramOperation>& processor) {
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
        YDB_LOG_VERIFY(Producers.emplace(resourceId, node).second);
    }

    void ResetProducer(const TResourceAddress& resourceId, TGraphNode* node) {
        auto it = Producers.find(resourceId);
        YDB_LOG_VERIFY(it != Producers.end());
        it->second = node;
    }

    TResourceId BuildNextResourceId() {
        return ++NextResourceId;
    }

    TGraph(std::vector<std::shared_ptr<IProgramOperation>>&& processors, const IColumnResolver& resolver, const bool reserveIndexMemory);

public:
    const std::map<ui64, std::shared_ptr<TGraphNode>>& GetNodes() const {
        return Nodes;
    }

    class TBuilder {
    private:
        std::vector<std::shared_ptr<IProgramOperation>> Processors;
        const IColumnResolver& Resolver;
        bool Finished = false;
        bool ReserveIndexMemory = false;

    public:
        TBuilder(const IColumnResolver& resolver)
            : Resolver(resolver) {
        }

        void Add(const std::shared_ptr<IProgramOperation>& processor) {
            YDB_LOG_VERIFY(!Finished);
            Processors.emplace_back(processor);
        }

        void EnableIndexMemoryReserve() {
            YDB_LOG_VERIFY(!Finished);
            ReserveIndexMemory = true;
        }

        TConclusion<std::shared_ptr<NExecution::TCompiledGraph>> Finish() {
            YDB_LOG_VERIFY(!Finished);
            Finished = true;
            TGraph graph(std::move(Processors), Resolver, ReserveIndexMemory);
            auto status = graph.Collapse();
            if (status.IsFail()) {
                return status;
            }
            return graph.Compile();
        }
    };
};

}   // namespace NKikimr::NArrow::NSSA::NGraph::NOptimization
