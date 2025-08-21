#pragma once
#include "abstract.h"

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/string/join.h>

namespace NKikimr::NArrow::NSSA {
class TCalculationProcessor;
class IDataSource;
}   // namespace NKikimr::NArrow::NSSA

namespace NKikimr::NArrow::NSSA::NGraph::NOptimization {
class TGraph;
}

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {

class TCompiledGraph {
public:
    class TNode: public TExecutionNodeContext {
    private:
        friend class TCompiledGraph;

        YDB_READONLY(ui32, Identifier, 0);
        YDB_READONLY_DEF(std::shared_ptr<IResourceProcessor>, Processor);
        YDB_READONLY_DEF(std::vector<TNode*>, InputEdges);
        YDB_READONLY_DEF(std::vector<TNode*>, OutputEdges);
        std::optional<ui64> Weight;
        std::optional<ui32> SequentialIdx;

        void CalcWeight(const IColumnResolver& resolver, const ui64 sumChildren);

        void ConnectTo(TNode* to) {
            for (auto&& i : to->InputEdges) {
                if (i->GetIdentifier() == GetIdentifier()) {
                    return;
                }
            }
            for (auto&& i : OutputEdges) {
                AFL_VERIFY(i->GetIdentifier() != to->GetIdentifier());
            }
            to->InputEdges.emplace_back(this);
            OutputEdges.emplace_back(to);
        }

        void SortInputs() {
            const auto pred = [](const TNode* l, const TNode* r) {
                return l->GetWeight() < r->GetWeight();
            };
            std::sort(InputEdges.begin(), InputEdges.end(), pred);
        }

    public:
        TString GetSignalCategoryName() const {
            return Processor->GetSignalCategoryName();
        }

        void SetSequentialIdx(const ui32 idx) {
            AFL_VERIFY(!SequentialIdx);
            SequentialIdx = idx;
        }

        i32 GetSequentialIdx(const std::optional<i32> defValue = std::nullopt) const {
            if (SequentialIdx) {
                return *SequentialIdx;
            } else if (!!defValue) {
                return *defValue;
            } else {
                AFL_VERIFY(false);
                return 0;
            }
        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("id", Identifier);
            result.InsertValue("p", Processor->DebugJson());
            result.InsertValue("w", GetWeight());
            return result;
        }

        ui64 GetWeight() const {
            AFL_VERIFY(!!Weight);
            return *Weight;
        }

        bool HasWeight() const {
            return !!Weight;
        }

        TNode(const ui32 identifier, const std::shared_ptr<IResourceProcessor>& processor)
            : Identifier(identifier)
            , Processor(processor) {
        }
    };
    class TIterator;

    class IVisitor {
    public:
        enum class EVisitStatus {
            NeedExecute,
            Skipped,
            Finished
        };

    private:
        THashSet<ui32> Visited;
        THashSet<ui32> Executed;
        THashSet<ui32> Current;

        virtual TConclusion<EVisitStatus> DoOnExit(const TCompiledGraph::TNode& node) = 0;
        virtual TConclusionStatus DoOnEnter(const TCompiledGraph::TNode& node) = 0;
        virtual TConclusionStatus DoOnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) = 0;

    public:
        virtual ~IVisitor() = default;

        const THashSet<ui32>& GetExecutedIds() const {
            return Executed;
        }
        [[nodiscard]] TConclusion<EVisitStatus> OnExit(const TCompiledGraph::TNode& node);
        [[nodiscard]] TConclusionStatus OnEnter(const TCompiledGraph::TNode& node) {
            AFL_VERIFY(node.GetProcessor());
            AFL_VERIFY(Visited.emplace(node.GetIdentifier()).second);
            AFL_VERIFY(Current.emplace(node.GetIdentifier()).second);
            return DoOnEnter(node);
        }
        [[nodiscard]] TConclusionStatus OnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) {
            AFL_VERIFY(node.GetProcessor());
            AFL_VERIFY(Current.contains(node.GetIdentifier()));
            return DoOnComeback(node, readyInputs);
        }
    };

private:
    THashMap<ui64, std::shared_ptr<TNode>> Nodes;
    THashSet<ui32> SourceColumns;
    THashSet<ui32> FilterColumns;
    YDB_READONLY_DEF(std::shared_ptr<TNode>, ResultRoot);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TNode>>, FilterRoot);
    bool IsFilterRoot(const ui32 identifier) const;

public:
    std::shared_ptr<IResourcesAggregator> GetResultsAggregationProcessor() const {
        if (ResultRoot->GetProcessor()->GetProcessorType() != EProcessorType::Projection) {
            return nullptr;
        }
        std::vector<std::shared_ptr<IResourcesAggregator>> aggregators;
        for (auto&& i : ResultRoot->GetInputEdges()) {
            if (i->GetProcessor()->IsAggregation()) {
                aggregators.emplace_back(i->GetProcessor()->BuildResultsAggregator());
                if (!aggregators.back()) {
                    return nullptr;
                }
            } else {
                //                return nullptr;
            }
        }
        if (aggregators.size() > 1) {
            return std::make_shared<TCompositeResourcesAggregator>(std::move(aggregators));
        } else if (aggregators.size() == 1) {
            return aggregators.front();
        } else {
            return nullptr;
        }
    }

    const THashMap<ui64, std::shared_ptr<TNode>>& GetNodes() const {
        return Nodes;
    }

    bool IsGenerated(const ui32 resourceId) const {
        return !SourceColumns.contains(resourceId);
    }

    const THashSet<ui32>& GetSourceColumns() const {
        return SourceColumns;
    }

    const THashSet<ui32>& GetFilterColumns() const {
        return FilterColumns;
    }

    class TIterator {
    public:
        enum class ENodeStatus {
            InProgress,
            Finished
        };

    private:
        std::vector<std::shared_ptr<TCompiledGraph::TNode>> GraphNodes;
        std::shared_ptr<TCompiledGraph::TNode> CurrentGraphNode;
        ui32 CurrentGraphNodeIdx = 0;
        std::vector<TCompiledGraph::TNode*> NodesStack;
        THashMap<ui32, ENodeStatus> Statuses;
        TCompiledGraph::TNode* CurrentNode = nullptr;
        std::shared_ptr<IVisitor> Visitor;
        [[nodiscard]] TConclusionStatus ProvideCurrentToExecute();

        [[nodiscard]] TConclusion<bool> GlobalInitialize();

    public:
        TIterator(const std::shared_ptr<IVisitor>& visitor)
            : Visitor(visitor) {
        }

        ui32 GetCurrentNodeId() const {
            return GetCurrentNode().GetIdentifier();
        }

        const std::shared_ptr<TCompiledGraph::TNode>& GetCurrentGraphNode() const {
            AFL_VERIFY(CurrentGraphNode);
            return CurrentGraphNode;
        }

        const TCompiledGraph::TNode& GetCurrentNode() const {
            AFL_VERIFY(IsValid());
            return *CurrentNode;
        }

        TCompiledGraph::TNode& MutableCurrentNode() {
            AFL_VERIFY(IsValid());
            return *CurrentNode;
        }

        [[nodiscard]] TConclusion<bool> Reset(std::vector<std::shared_ptr<TCompiledGraph::TNode>>&& graphNodes);

        bool IsValid() const {
            return !!CurrentNode;
        }

        const std::shared_ptr<IResourceProcessor>& GetProcessorVerified() const {
            AFL_VERIFY(!!CurrentNode);
            return CurrentNode->GetProcessor();
        }

        TConclusion<bool> Next();
    };

    std::shared_ptr<TIterator> BuildIterator(const std::shared_ptr<IVisitor>& visitor) const;

    TCompiledGraph(const NOptimization::TGraph& original, const IColumnResolver& resolver);

    TConclusion<std::unique_ptr<TAccessorsCollection>> Apply(
        const std::shared_ptr<IDataSource>& source, std::unique_ptr<TAccessorsCollection>&& resources) const;

    NJson::TJsonValue DebugJson() const;

    bool HasAggregations() const {
        for (auto&& i : Nodes) {
            if (i.second->GetProcessor()->IsAggregation()) {
                return true;
            }
        }
        return false;
    }

    TString DebugDOT(const THashSet<ui32>& special = Default<THashSet<ui32>>()) const;
    TString DebugStats() const;

    TString DebugString() const {
        return DebugJson().GetStringRobust();
    }
};

}   // namespace NKikimr::NArrow::NSSA::NGraph::NExecution
