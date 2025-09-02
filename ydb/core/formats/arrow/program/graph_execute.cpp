#include "execution.h"
#include "graph_execute.h"
#include "graph_optimization.h"
#include "visitor.h"

#include <yql/essentials/minikql/mkql_terminator.h>

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
            for (auto&& near : i->GetOutputEdges()) {
                if (!nextNodeIds.emplace(near->GetIdentifier()).second) {
                    continue;
                }
                AFL_VERIFY(!near->HasWeight());
                bool hasWeight = true;
                ui64 sumWeight = 0;
                for (auto&& test : near->GetInputEdges()) {
                    if (!test->HasWeight()) {
                        hasWeight = false;
                        break;
                    }
                    sumWeight += test->GetWeight();
                }
                if (hasWeight) {
                    weightByNode[near->GetIdentifier()] = sumWeight;
                    nextNodes.emplace_back(near);
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
                if (i.second->GetProcessor()->GetProcessorType() == EProcessorType::Filter) {
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
    AFL_TRACE(NKikimrServices::SSA_GRAPH_EXECUTION)("graph_constructed", DebugDOT());
//    Cerr << DebugDOT() << Endl;
}

TConclusion<std::unique_ptr<TAccessorsCollection>> TCompiledGraph::Apply(
    const std::shared_ptr<IDataSource>& source, std::unique_ptr<TAccessorsCollection>&& resources) const {
    TProcessorContext context(source, std::move(resources), std::nullopt, false);
    NMiniKQL::TThrowingBindTerminator bind;
    std::shared_ptr<TExecutionVisitor> visitor = std::make_shared<TExecutionVisitor>(std::move(context));
    for (auto it = BuildIterator(visitor); it->IsValid();) {
        {
            auto conclusion = visitor->Execute();
            if (conclusion.IsFail()) {
                return conclusion;
            } else {
                AFL_VERIFY(*conclusion != IResourceProcessor::EExecutionResult::InBackground);
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
