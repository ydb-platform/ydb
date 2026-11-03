#include "execution.h"
#include "graph_execute.h"
#include "aggr_keys.h"
#include "assign_const.h"
#include "assign_internal.h"
#include "stats.h"
#include "stream_logic.h"

#include <util/string/cast.h>
#include <util/string/join.h>
#include "graph_optimization.h"
#include "visitor.h"

#include <yql/essentials/minikql/mkql_terminator.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SSA_GRAPH_EXECUTION

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {

class TResourceUsageInfo {
private:
    YDB_READONLY_DEF(std::optional<ui32>, LastUsage);
    YDB_READONLY_DEF(std::optional<ui32>, Construct);

public:
    void InUsage(const ui32 stepIdx) {
        AFL_VERIFY(!!Construct);
        if (!!LastUsage) {
            AFL_VERIFY(*LastUsage < stepIdx);
        }
        LastUsage = stepIdx;
    }

    void Constructed(const ui32 stepIdx) {
        if (!Construct) {
            Construct = stepIdx;
        } else {
            AFL_VERIFY(Construct < stepIdx);
        }
    }
};

class TStepResourcesUsage {
private:
    YDB_READONLY_DEF(THashSet<ui32>, LastUsageResources);

public:
    void OnLastUsage(const ui32 resourceId) {
        AFL_VERIFY(LastUsageResources.emplace(resourceId).second);
    }
};

TCompiledGraph::TCompiledGraph(const NOptimization::TGraph& original, const IColumnResolver& resolver) {
    for (auto&& i : original.GetNodes()) {
        Nodes.emplace(i.first, std::make_shared<TNode>(i.first, i.second->GetProcessor()));
        for (const auto& input : i.second->GetProcessor()->GetInput()) {
            const auto name = resolver.GetColumnName(input.GetColumnId(), false);
            if (name) {
                StatsColumnNames.emplace(input.GetColumnId(), name);
            }
        }
    }
    for (auto&& n : original.GetNodes()) {
        auto itTo = Nodes.find(n.first);
        AFL_VERIFY(itTo != Nodes.end());
        for (auto&& from : n.second->GetInputEdges()) {
            auto itFrom = Nodes.find(from.first.GetNodeId());
            AFL_VERIFY(itFrom != Nodes.end());
            itFrom->second->ConnectTo(itTo->second.get());
        }
    }
    std::vector<TNode*> currentNodes;
    for (auto&& n : Nodes) {
        if (n.second->GetInputEdges().empty()) {
            currentNodes.emplace_back(n.second.get());
            n.second->CalcWeight(resolver, 0);
        }
    }
    ui32 nodesCount = currentNodes.size();
    while (currentNodes.size()) {
        std::vector<TNode*> nextNodes;
        THashMap<ui32, ui64> weightByNode;
        THashSet<ui32> nextNodeIds;
        for (auto&& i : currentNodes) {
            for (auto&& neighbor : i->GetOutputEdges()) {
                if (!nextNodeIds.emplace(neighbor->GetIdentifier()).second) {
                    continue;
                }
                AFL_VERIFY(!neighbor->HasWeight());
                bool hasWeight = true;
                ui64 sumWeight = 0;
                for (auto&& test : neighbor->GetInputEdges()) {
                    if (!test->HasWeight()) {
                        hasWeight = false;
                        break;
                    }
                    sumWeight += test->GetWeight();
                }
                if (hasWeight) {
                    weightByNode[neighbor->GetIdentifier()] = sumWeight;
                    nextNodes.emplace_back(neighbor);
                }
            }
        }
        for (auto&& i : nextNodes) {
            ++nodesCount;
            auto it = weightByNode.find(i->GetIdentifier());
            AFL_VERIFY(it != weightByNode.end());
            i->CalcWeight(resolver, it->second);
        }
        currentNodes = std::move(nextNodes);
    }
    AFL_VERIFY(nodesCount == Nodes.size())("init", nodesCount)("nodes", Nodes.size());

    {
        std::vector<ui32> toRemoveConst;
        for (auto&& i : Nodes) {
            i.second->SortInputs();
            if (!i.second->GetOutputEdges().size()) {
                if (i.second->GetProcessor()->GetProcessorType() == EProcessorType::Filter ||
                    i.second->GetProcessor()->GetProcessorType() == EProcessorType::DistinctMarker) {
                    AFL_VERIFY(!IsFilterRoot(i.second->GetIdentifier()));
                    FilterRoot.emplace_back(i.second);
                } else if (i.second->GetProcessor()->GetProcessorType() == EProcessorType::Projection) {
                    AFL_VERIFY(!ResultRoot)("debug", DebugDOT());
                    ResultRoot = i.second;
                } else {
                    toRemoveConst.emplace_back(i.first);
                }
            }
        }
        for (auto&& i : toRemoveConst) {
            Nodes.erase(i);
        }
    }
    THashMap<ui32, TResourceUsageInfo> usage;
    std::vector<TNode*> sortedNodes;
    {
        ui32 currentIndex = 0;
        auto it = BuildIterator(nullptr);
        for (; it->IsValid(); it->Next()) {
            it->MutableCurrentNode().SetSequentialIdx(currentIndex);
            for (auto&& i : it->GetProcessorVerified()->GetInput()) {
                if (!i.GetColumnId()) {
                    continue;
                }
                if (resolver.HasColumn(i.GetColumnId())) {
                    if (IsFilterRoot(it->GetCurrentGraphNode()->GetIdentifier())) {
                        FilterColumns.emplace(i.GetColumnId());
                    }
                    SourceColumns.emplace(i.GetColumnId());
                }
                usage[i.GetColumnId()].InUsage(currentIndex);
            }
            for (auto&& i : it->GetProcessorVerified()->GetOutput()) {
                if (!i.GetColumnId()) {
                    continue;
                }
                usage[i.GetColumnId()].Constructed(currentIndex);
            }
            sortedNodes.emplace_back(&it->MutableCurrentNode());
            ++currentIndex;
        }
    }
    AFL_VERIFY(sortedNodes.size() == Nodes.size())("debug", DebugDOT());
    THashMap<ui32, TStepResourcesUsage> stepInfo;
    for (auto&& u : usage) {
        if (u.second.GetLastUsage()) {
            stepInfo[*u.second.GetLastUsage()].OnLastUsage(u.first);
        }
    }
    for (auto&& i : stepInfo) {
        AFL_VERIFY(i.first < sortedNodes.size());
        auto* node = sortedNodes[i.first];
        if (i.first + 1 != sortedNodes.size()) {
            node->SetRemoveResourceIds(i.second.GetLastUsageResources());
        }
    }
    YDB_LOG_TRACE("",
        {"graphConstructed", DebugDOT()});
    BuildStatsMetaSteps();
}

void TCompiledGraph::BuildStatsMetaSteps() {
    constexpr ui8 filter = 1;
    constexpr ui8 aggregation = 2;
    constexpr ui8 index = 4;
    constexpr ui8 result = 8;
    const auto boundary = [&](const TNode& node) -> ui8 {
        const auto& processor = node.GetProcessor();
        if (processor->IsAggregation()) {
            return aggregation;
        }
        switch (processor->GetProcessorType()) {
            case EProcessorType::Filter:
                return filter;
            case EProcessorType::CheckIndexData:
                return index;
            case EProcessorType::Projection:
                return result;
            default:
                return 0;
        }
    };
    THashMap<ui32, ui32> remaining;
    THashMap<ui32, ui8> consumers;
    std::vector<TNode*> ready;
    for (const auto& [id, node] : Nodes) {
        ui32 outputs = 0;
        for (const auto* output : node->GetOutputEdges()) {
            outputs += Nodes.contains(output->GetIdentifier());
        }
        remaining.emplace(id, outputs);
        consumers.emplace(id, boundary(*node));
        if (outputs == 0) {
            ready.push_back(node.get());
        }
    }
    while (!ready.empty()) {
        const auto* node = ready.back();
        ready.pop_back();
        const ui32 id = node->GetIdentifier();
        const ui8 mask = consumers.at(id);
        TString meta;
        if (node->GetProcessor()->GetProcessorType() == EProcessorType::Projection) {
            meta = "Projection";
        } else if (mask & filter) {
            meta = "Filter";
        } else if (mask & index) {
            meta = "IndexFilter";
        } else if (mask == aggregation) {
            meta = "Aggregation";
        } else {
            meta = "FetchOtherColumns";
        }
        StatsMetaSteps.emplace(id, std::move(meta));
        for (auto* input : node->GetInputEdges()) {
            const ui32 inputId = input->GetIdentifier();
            if (!remaining.contains(inputId)) {
                continue;
            }
            if (!boundary(*input)) {
                consumers.at(inputId) |= mask;
            }
            if (--remaining.at(inputId) == 0) {
                ready.push_back(input);
            }
        }
    }
    AFL_VERIFY(StatsMetaSteps.size() == Nodes.size());
}

const TString& TCompiledGraph::GetStatsMetaStep(ui32 identifier) const {
    const auto it = StatsMetaSteps.find(identifier);
    AFL_VERIFY(it != StatsMetaSteps.end())("node_id", identifier);
    return it->second;
}

TConclusion<std::unique_ptr<TAccessorsCollection>> TCompiledGraph::Apply(
    IDataSource& source, std::unique_ptr<TAccessorsCollection>&& resources) const {
    const auto collector = NScanStats::CurrentCollector();
    const bool plain = collector && collector->GetReaderKind() == "PLAIN";
    THashMap<ui32, TString> expressions;
    if (plain) {
        for (const ui32 id : resources->GetStatsColumnIds()) {
            const auto name = StatsColumnNames.find(id);
            expressions.emplace(id, name == StatsColumnNames.end()
                ? TString(resources->GetFieldVerified(id)->name()) : name->second);
        }
    }
    const auto resolve = [&](ui32 id) -> TString {
        const auto it = expressions.find(id);
        if (it == expressions.end()) {
            return TStringBuilder() << "resource_" << id;
        }
        return it->second;
    };
    TProcessorContext context(source, std::move(resources), std::nullopt, false);
    NMiniKQL::TThrowingBindTerminator bind;
    std::shared_ptr<TExecutionVisitor> visitor = std::make_shared<TExecutionVisitor>(std::move(context));
    for (auto it = BuildIterator(visitor); it->IsValid();) {
        {
            std::shared_ptr<NScanStats::TOperation> operation;
            if (collector && visitor->GetExecutionNode()) {
                const auto& processor = it->GetProcessorVerified();
                const auto kind = processor->GetStatsKind();
                const TString& meta = GetStatsMetaStep(it->GetCurrentNodeId());
                const auto type = processor->GetProcessorType();
                std::vector<TString> columns;
                for (const auto& input : processor->GetInput()) {
                    columns.emplace_back(resolve(input.GetColumnId()));
                }
                TString function = processor->GetSignalCategoryName();
                if (const auto* calculation = dynamic_cast<const TCalculationProcessor*>(processor.get())) {
                    function = calculation->GetStatsFunctionDescription();
                } else if (const auto* stream = dynamic_cast<const TStreamLogicProcessor*>(processor.get())) {
                    function = ::ToString(stream->GetOperation());
                }
                TString description = BuildStatsExpression(function, columns, it->GetCurrentNodeId());
                if (const auto* constant = dynamic_cast<const TConstProcessor*>(processor.get())) {
                    const auto value = constant->GetScalarConstant()->ToString();
                    description = BoundStatsDescription(TStringBuf(value.data(), value.size()), it->GetCurrentNodeId());
                }
                if (type == EProcessorType::Calculation || type == EProcessorType::Const ||
                    type == EProcessorType::StreamLogic || type == EProcessorType::CheckIndexData ||
                    type == EProcessorType::CheckHeaderData) {
                    for (const auto& output : processor->GetOutput()) {
                        expressions[output.GetColumnId()] = description;
                    }
                }
                const bool supplied = plain && (meta == "IndexFilter" || kind == NScanStats::TStep::FetchColumns ||
                    kind == NScanStats::TStep::MemoryReserve || kind == NScanStats::TStep::Assemble);
                if (!supplied) {
                    operation = collector->Start(kind, processor->GetSignalCategoryName(), description, NScanStats::Current());
                }
                if (operation) {
                    operation->Update([&](NScanStats::TStep& record) {
                        record.SetMetaStep(meta);
                        record.SetCanonicalPath("");
                        if (kind == NScanStats::TStep::Filter) {
                            record.MutableDetails()->MutableFilter()->SetPredicate(description);
                        }
                        if (const auto* keyed = dynamic_cast<const NAggregation::TWithKeysAggregationProcessor*>(processor.get())) {
                            auto* details = record.MutableDetails()->MutableAggregation();
                            for (const auto& key : keyed->GetAggregationKeys()) {
                                details->MutableKeys()->AddNames(resolve(key.GetColumnId()));
                            }
                            std::vector<TString> functions;
                            for (const auto& aggregate : keyed->GetAggregations()) {
                                std::vector<TString> inputs;
                                for (const auto& input : aggregate.GetInputs()) {
                                    inputs.emplace_back(resolve(input.GetColumnId()));
                                }
                                const TString expression = TStringBuilder() << aggregate.GetAggregationId() << "(" << JoinSeq(", ", inputs) << ")";
                                details->MutableFunctions()->AddNames(expression);
                                functions.emplace_back(expression);
                                expressions[aggregate.GetOutput().GetColumnId()] = expression;
                            }
                            record.SetOperation(TStringBuilder() << "GROUP BY " << JoinSeq(", ", details->GetKeys().GetNames()) << ": " << JoinSeq(", ", functions));
                        }
                        if (kind == NScanStats::TStep::Projection) {
                            auto* details = record.MutableDetails()->MutableProjection();
                            for (const ui32 id : visitor->MutableContext().GetResources().GetStatsColumnIds()) {
                                details->MutableInputColumns()->AddNames(resolve(id));
                            }
                            for (const auto& column : columns) {
                                details->MutableOutputColumns()->AddNames(column);
                            }
                        }
                    });
                }
            }
            auto conclusion = [&] {
                NScanStats::TScope scope(operation, collector);
                return visitor->Execute();
            }();
            if (operation) {
                operation->Finish(conclusion.IsFail() ? NScanStats::TStep::failed : NScanStats::TStep::completed);
            }
            if (conclusion.IsFail()) {
                return conclusion;
            } else {
                AFL_VERIFY(!conclusion->IsPending());
            }
        }
        if (visitor->MutableContext().GetResources().HasDataAndResultIsEmpty()) {
            visitor->MutableContext().MutableResources().Clear();
            return visitor->MutableContext().ExtractResources();
        }
        {
            auto conclusion = it->Next();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
    }
    return visitor->MutableContext().ExtractResources();
}

NJson::TJsonValue TCompiledGraph::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonNodes = result.InsertValue("nodes", NJson::JSON_MAP);
    for (auto&& i : Nodes) {
        jsonNodes.InsertValue(::ToString(i.first), i.second->DebugJson());
    }
    auto& jsonEdges = result.InsertValue("edges", NJson::JSON_ARRAY);
    for (auto&& i : Nodes) {
        auto& jsonProcessor = jsonEdges.AppendValue(NJson::JSON_MAP);
        jsonProcessor.InsertValue("owner_id", i.first);
        auto& jsonInputs = jsonProcessor.InsertValue("inputs", NJson::JSON_ARRAY);
        for (auto&& input : i.second->GetInputEdges()) {
            auto& jsonEdge = jsonInputs.AppendValue(NJson::JSON_MAP);
            jsonEdge.InsertValue("from", input->GetIdentifier());
        }
    }
    return result;
}

TString TCompiledGraph::DebugDOT(const THashSet<ui32>& special) const {
    TStringBuilder result;
    result << "digraph program {";
    std::map<ui32, ui32> remapSeqToId;
    for (auto&& i : Nodes) {
        if (i.second->GetSequentialIdx(-1) != -1) {
            remapSeqToId.emplace(i.second->GetSequentialIdx(), i.first);
        }
        NJson::TJsonValue quote;
        quote.AppendValue(i.second->GetProcessor()->DebugJson().GetStringRobust());
        const TString data = quote.GetStringRobust();
        TStringBuilder label;
        label << "N" << i.second->GetSequentialIdx(-1) << "(" << i.second->GetWeight() << ")" << ":" << data.substr(2, data.size() - 4) << "\\n";
        if (i.second->GetRemoveResourceIds().size()) {
            label << "REMOVE:" << JoinSeq(",", i.second->GetRemoveResourceIds());
        }
        result << "N" << i.second->GetIdentifier() << "[shape=box, label=\"" << label << "\"";
        if (special.size()) {
            if (special.contains(i.second->GetIdentifier())) {
                result << ",style=filled,color=\"#99FF99\"";
            } else {
                result << ",style=filled,color=\"#777777\"";
            }
        } else {
            if (i.second->GetOutputEdges().empty()) {
                result << ",style=filled,color=\"#FFAAAA\"";
            } else if (i.second->GetProcessor()->GetProcessorType() == EProcessorType::AssembleOriginalData ||
                       i.second->GetProcessor()->GetProcessorType() == EProcessorType::FetchOriginalData) {
                result << ",style=filled,color=\"#FFFF88\"";
            }
        }
        result << "];" << Endl;
        ui32 idx = 1;
        for (auto&& input : i.second->GetInputEdges()) {
            result << "N" << input->GetIdentifier() << " -> " << "N" << i.second->GetIdentifier() << "[label=\"" + ::ToString(idx) + "\"];"
                   << Endl;
            ++idx;
        }
    }
    if (remapSeqToId.size()) {
        TStringBuilder seq;
        for (auto&& i : remapSeqToId) {
            if (seq.size()) {
                seq << "->";
            }
            seq << "N" << i.second;
        }
        seq << "[color=red];";
        result << seq << Endl;
    }
    result << "}";
    return result;
}

std::shared_ptr<TCompiledGraph::TIterator> TCompiledGraph::BuildIterator(const std::shared_ptr<IVisitor>& visitor) const {
    auto result = std::make_shared<TIterator>(visitor);
    auto rootNodes = FilterRoot;
    rootNodes.emplace_back(ResultRoot);
    result->Reset(std::move(rootNodes)).DetachResult();
    return result;
}

bool TCompiledGraph::IsFilterRoot(const ui32 identifier) const {
    for (auto&& i : FilterRoot) {
        if (i->GetIdentifier() == identifier) {
            return true;
        }
    }
    return false;
}

TString TCompiledGraph::DebugStats() const {
    std::map<EProcessorType, ui32> countProcType;
    std::map<EProcessorType, ui32> countProcTypeWithSub;
    for (auto&& [_, i] : Nodes) {
        ++countProcType[i->GetProcessor()->GetProcessorType()];
        if (i->GetProcessor()->HasSubColumns()) {
            ++countProcTypeWithSub[i->GetProcessor()->GetProcessorType()];
        }
    }
    TStringBuilder result;
    result << "[TOTAL:";
    for (auto&& i : countProcType) {
        result << i.first << ":" << i.second << ";";
    }
    result << "];";
    result << "SUB:[";
    for (auto&& i : countProcTypeWithSub) {
        result << i.first << ":" << i.second << ";";
    }
    result << "];";
    return result;
}

TConclusionStatus TCompiledGraph::TIterator::ProvideCurrentToExecute() {
    while (true) {
        AFL_VERIFY(CurrentNode);
        if (Visitor) {
            auto conclusion = Visitor->OnEnter(*CurrentNode);
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        NodesStack.emplace_back(CurrentNode);
        auto it = Statuses.find(CurrentNode->GetIdentifier());
        if (it == Statuses.end()) {
            AFL_VERIFY(Statuses.emplace(CurrentNode->GetIdentifier(), ENodeStatus::InProgress).second);
            bool needInput = false;
            for (auto&& input : CurrentNode->GetInputEdges()) {
                auto it = Statuses.find(input->GetIdentifier());
                if (it == Statuses.end()) {
                    CurrentNode = input;
                    needInput = true;
                    break;
                } else {
                    AFL_VERIFY(it->second == ENodeStatus::Finished);
                }
            }
            if (!needInput) {
                break;
            }
        } else {
            AFL_VERIFY(it->second == ENodeStatus::Finished);
            NodesStack.pop_back();
            if (NodesStack.size()) {
                CurrentNode = NodesStack.back();
            } else {
                CurrentNode = nullptr;
            }
            break;
        }
    }
    return TConclusionStatus::Success();
}

TConclusion<bool> TCompiledGraph::TIterator::GlobalInitialize() {
    while (!IsValid()) {
        if (CurrentGraphNodeIdx + 1 < GraphNodes.size()) {
            ++CurrentGraphNodeIdx;
        } else {
            break;
        }
        CurrentGraphNode = GraphNodes[CurrentGraphNodeIdx];
        CurrentNode = CurrentGraphNode.get();
        auto conclusion = ProvideCurrentToExecute();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    }
    return IsValid();
}

TConclusion<bool> TCompiledGraph::TIterator::Reset(std::vector<std::shared_ptr<TCompiledGraph::TNode>>&& graphNodes) {
    GraphNodes = std::move(graphNodes);
    GraphNodes.erase(std::remove_if(GraphNodes.begin(), GraphNodes.end(),
                         [](const std::shared_ptr<TCompiledGraph::TNode>& item) {
                             return !item;
                         }),
        GraphNodes.end());
    if (GraphNodes.size()) {
        CurrentGraphNode = GraphNodes.front();
        CurrentGraphNodeIdx = 0;
        CurrentNode = CurrentGraphNode.get();
        {
            auto conclusion = ProvideCurrentToExecute();
            if (conclusion.IsFail()) {
                return conclusion;
            }
        }
        return GlobalInitialize();
    } else {
        return false;
    }
}

TConclusion<bool> TCompiledGraph::TIterator::Next() {
    AFL_VERIFY(CurrentNode);
    AFL_VERIFY(NodesStack.size());
    bool skipFlag = false;
    if (Visitor) {
        AFL_VERIFY(NodesStack.back());
        auto conclusion = Visitor->OnExit(*NodesStack.back());
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (*conclusion == IVisitor::EVisitStatus::NeedExecute) {
            return true;
        }
        if (*conclusion == IVisitor::EVisitStatus::Skipped) {
            skipFlag = true;
        }
    }
    TNode* nextNode = nullptr;
    if (NodesStack.size() > 1) {
        auto owner = NodesStack[NodesStack.size() - 2];
        AFL_VERIFY(owner);
        bool found = false;
        for (ui32 idx = 0; idx < owner->GetInputEdges().size(); ++idx) {
            if (found) {
                auto itStatus = Statuses.find(owner->GetInputEdges()[idx]->GetIdentifier());
                if (itStatus == Statuses.end()) {
                    nextNode = owner->GetInputEdges()[idx];
                    break;
                } else {
                    AFL_VERIFY(itStatus->second == ENodeStatus::Finished);
                }
            } else if (owner->GetInputEdges()[idx]->GetIdentifier() == CurrentNode->GetIdentifier()) {
                if (Visitor) {
                    std::vector<TColumnChainInfo> columnsInput;
                    for (auto&& out : owner->GetInputEdges()[idx]->GetProcessor()->GetOutput()) {
                        if (owner->GetProcessor()->HasInput(out.GetColumnId())) {
                            columnsInput.emplace_back(out.GetColumnId());
                        }
                    }
                    AFL_VERIFY(columnsInput.size());
                    auto conclusion = Visitor->OnComeback(*owner, columnsInput);
                    if (conclusion.IsFail()) {
                        return conclusion;
                    }
                }
                found = true;
            }
        }
        AFL_VERIFY(found);
    }
    AFL_VERIFY(NodesStack.size());
    NodesStack.pop_back();
    auto itStatus = Statuses.find(CurrentNode->GetIdentifier());
    AFL_VERIFY(itStatus != Statuses.end());
    AFL_VERIFY(itStatus->second == ENodeStatus::InProgress);
    if (skipFlag) {
        Statuses.erase(itStatus);
    } else {
        itStatus->second = ENodeStatus::Finished;
    }
    if (!!nextNode) {
        CurrentNode = nextNode;
        auto conclusion = ProvideCurrentToExecute();
        if (conclusion.IsFail()) {
            return conclusion;
        }
    } else {
        if (NodesStack.size()) {
            CurrentNode = NodesStack.back();
        } else {
            CurrentNode = nullptr;
        }
    }
    return GlobalInitialize();
}

TConclusion<TCompiledGraph::IVisitor::EVisitStatus> TCompiledGraph::IVisitor::OnExit(const TCompiledGraph::TNode& node) {
    AFL_VERIFY(node.GetProcessor());
    auto conclusion = DoOnExit(node);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    if (*conclusion == EVisitStatus::Finished) {
        AFL_VERIFY(Current.erase(node.GetIdentifier()))("id", node.GetIdentifier());
    } else if (*conclusion == EVisitStatus::Skipped) {
        AFL_VERIFY(Current.erase(node.GetIdentifier()))("id", node.GetIdentifier());
        AFL_VERIFY(Visited.erase(node.GetIdentifier()))("id", node.GetIdentifier());
    } else {
        AFL_VERIFY(Executed.emplace(node.GetIdentifier()).second)("id", node.GetIdentifier());
    }
    return conclusion;
}

void TCompiledGraph::TNode::CalcWeight(const IColumnResolver& /*resolver*/, const ui64 sumChildren) {
    Weight = Processor->GetWeight() + sumChildren;
}

}   // namespace NKikimr::NArrow::NSSA::NGraph::NExecution
