#include "assign_const.h"
#include "assign_internal.h"
#include "filter.h"
#include "graph_optimization.h"
#include "index.h"
#include "original.h"
#include "stream_logic.h"

#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/string/builder.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA::NGraph::NOptimization {

void TGraphNode::AddEdgeTo(TGraphNode* to, const ui32 resourceId) {
    AFL_VERIFY(OutputEdges.emplace(TAddress(to->GetIdentifier(), resourceId), to).second);
}

void TGraphNode::AddEdgeFrom(TGraphNode* from, const ui32 resourceId) {
    AFL_VERIFY(InputEdges.emplace(TAddress(from->GetIdentifier(), resourceId), from).second);
}

void TGraphNode::RemoveEdgeTo(const ui32 identifier, const ui32 resourceId) {
    AFL_VERIFY(OutputEdges.erase(TAddress(identifier, resourceId)));
}

void TGraphNode::RemoveEdgeFrom(const ui32 identifier, const ui32 resourceId) {
    AFL_VERIFY(InputEdges.erase(TAddress(identifier, resourceId)));
}

bool TGraphNode::HasEdgeFrom(const ui32 nodeId, const ui32 resourceId) const {
    return InputEdges.contains(TAddress(nodeId, resourceId));
}

bool TGraphNode::HasEdgeTo(const ui32 nodeId, const ui32 resourceId) const {
    return OutputEdges.contains(TAddress(nodeId, resourceId));
}

bool TGraph::HasEdge(const TGraphNode* from, const TGraphNode* to, const ui32 resourceId) const {
    const bool hasEdgeTo = from->HasEdgeTo(to->GetIdentifier(), resourceId);
    const bool hasEdgeFrom = to->HasEdgeFrom(from->GetIdentifier(), resourceId);
    AFL_VERIFY(hasEdgeTo == hasEdgeFrom)("from", hasEdgeFrom)("to", hasEdgeTo);
    return hasEdgeFrom;
}

void TGraph::AddEdge(TGraphNode* from, TGraphNode* to, const ui32 resourceId) {
    from->AddEdgeTo(to, resourceId);
    to->AddEdgeFrom(from, resourceId);
}

void TGraph::RemoveEdge(TGraphNode* from, TGraphNode* to, const ui32 resourceId) {
    from->RemoveEdgeTo(to->GetIdentifier(), resourceId);
    to->RemoveEdgeFrom(from->GetIdentifier(), resourceId);
}

void TGraph::RemoveNode(const ui32 idenitifier) {
    auto it = Nodes.find(idenitifier);
    AFL_VERIFY(it != Nodes.end());
    for (auto&& i : it->second->GetInputEdges()) {
        i.second->RemoveEdgeTo(it->second->GetIdentifier(), i.first.GetResourceId());
    }
    for (auto&& i : it->second->GetOutputEdges()) {
        i.second->RemoveEdgeFrom(it->second->GetIdentifier(), i.first.GetResourceId());
    }
    Nodes.erase(it);
}

TGraph::TGraph(std::vector<std::shared_ptr<IResourceProcessor>>&& processors, const IColumnResolver& resolver)
    : Resolver(resolver) {
    NextResourceId = 0;
    for (auto&& i : processors) {
        for (auto&& input : i->GetInput()) {
            NextResourceId = std::max<ui32>(NextResourceId, input.GetColumnId());
        }
        for (auto&& input : i->GetOutput()) {
            NextResourceId = std::max<ui32>(NextResourceId, input.GetColumnId());
        }
    }
    ++NextResourceId;
    for (auto&& i : processors) {
        auto node = AddNode(i);
        for (auto&& output : i->GetOutput()) {
            AFL_VERIFY(Producers.emplace(output.GetColumnId(), node.get()).second);
        }
        for (auto&& input : i->GetInput()) {
            if (Producers.find(input.GetColumnId()) != Producers.end()) {
                continue;
            }
            const TString name = Resolver.GetColumnName(input.GetColumnId(), true);

            auto inputFetcher = AddNode(std::make_shared<TOriginalColumnDataProcessor>(
                input.GetColumnId(), input.GetColumnId(), Resolver.GetColumnName(input.GetColumnId()), ""));
            //            AFL_VERIFY(Producers.emplace(input.GetColumnId(), inputFetcher.get()).second);

            auto nodeInputAssembler =
                AddNode(std::make_shared<TOriginalColumnAccessorProcessor>(input.GetColumnId(), input.GetColumnId(), input.GetColumnId(), ""));
            AFL_VERIFY(Producers.emplace(input.GetColumnId(), nodeInputAssembler.get()).second);
            AddEdge(inputFetcher.get(), nodeInputAssembler.get(), input.GetColumnId());
        }
    }
    for (auto&& [_, i] : Nodes) {
        for (auto&& p : i->GetProcessor()->GetInput()) {
            if (i->GetProcessor()->GetProcessorType() == EProcessorType::AssembleOriginalData ||
                i->GetProcessor()->GetProcessorType() == EProcessorType::FetchOriginalData) {
                continue;
            }
            auto node = GetProducerVerified(p.GetColumnId());
            AddEdge(node, i.get(), p.GetColumnId());
        }
    }
}

TConclusion<bool> TGraph::OptimizeConditionsForStream(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::StreamLogic) {
        return false;
    }
    if (condNode->GetOutputEdges().size() != 1) {
        return false;
    }
    auto* nodeOwner = condNode->GetOutputEdges().begin()->second;
    if (nodeOwner->GetProcessor()->GetProcessorType() != EProcessorType::StreamLogic) {
        return false;
    }
    auto streamChildrenCalc = condNode->GetProcessorAs<TStreamLogicProcessor>();
    auto streamOwnerCalc = nodeOwner->GetProcessorAs<TStreamLogicProcessor>();
    if (streamChildrenCalc->GetOperation() != streamOwnerCalc->GetOperation()) {
        return false;
    }
    for (auto&& [connectInfo, inputNode] : condNode->GetInputEdges()) {
        AddEdge(inputNode, nodeOwner, connectInfo.GetResourceId());
        nodeOwner->GetProcessor()->AddInput(connectInfo.GetResourceId());
    }
    nodeOwner->GetProcessor()->RemoveInput(condNode->GetOutputEdges().begin()->first.GetResourceId());
    RemoveNode(condNode->GetIdentifier());
    return true;
}

TConclusion<bool> TGraph::OptimizeForFetchSubColumns(TGraphNode* condNode) {
    if (!condNode->Is(EProcessorType::AssembleOriginalData)) {
        return false;
    }
    if (!!condNode->GetProcessorAs<TOriginalColumnAccessorProcessor>()->GetSubColumnName()) {
        return false;
    }
    auto originalAssemble = condNode->GetProcessorAs<TOriginalColumnAccessorProcessor>();
    std::vector<TGraphNode*> removeTo;
    std::vector<ui32> removeResourceId;
    THashMap<TResourceAddress, TGraphNode*> resourceProducers;
    for (auto&& [_, i] : condNode->GetOutputEdges()) {
        if (!i->Is(EProcessorType::Calculation)) {
            continue;
        }
        auto addr = GetOriginalAddress(i);
        if (!addr) {
            continue;
        }
        if (!addr->GetSubColumnName()) {
            continue;
        }
        AFL_VERIFY(addr->GetColumnId() == originalAssemble->GetColumnId());
        auto it = resourceProducers.find(*addr);
        if (it == resourceProducers.end()) {
            auto inputFetcher = AddNode(std::make_shared<TOriginalColumnDataProcessor>(
                addr->GetColumnId(), addr->GetColumnId(), Resolver.GetColumnName(addr->GetColumnId()), addr->GetSubColumnName()));
            auto nodeInputAssembler = AddNode(std::make_shared<TOriginalColumnAccessorProcessor>(
                addr->GetColumnId(), addr->GetColumnId(), addr->GetColumnId(), addr->GetSubColumnName()));
            AddEdge(inputFetcher.get(), nodeInputAssembler.get(), addr->GetColumnId());
            it = resourceProducers.emplace(*addr, nodeInputAssembler.get()).first;
        }
        AddEdge(it->second, i, addr->GetColumnId());
        removeTo.emplace_back(i);
        removeResourceId.emplace_back(addr->GetColumnId());
    }
    ui32 idx = 0;
    for (auto&& i : removeTo) {
        RemoveEdge(condNode, i, removeResourceId[idx++]);
    }
    if (condNode->GetOutputEdges().empty()) {
        RemoveBranch(condNode);
    }
    return (bool)removeTo.size();
}

TConclusion<bool> TGraph::OptimizeIndexesToApply(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::CheckIndexData) {
        return false;
    }
    if (condNode->GetProcessorAs<TIndexCheckerProcessor>()->GetApplyToFilter()) {
        return false;
    }
    if (condNode->GetProcessor()->GetOutput().size() != 1) {
        return false;
    }
    if (condNode->GetOutputEdges().size() != 1) {
        return false;
    }
    const auto* dest = condNode->GetOutputEdges().begin()->second;
    if (dest->GetProcessor()->GetProcessorType() != EProcessorType::StreamLogic) {
        return false;
    }
    if (dest->GetProcessorAs<TStreamLogicProcessor>()->GetOperation() != NKernels::EOperation::And) {
        return false;
    }
    if (dest->GetOutputEdges().size() != 1) {
        return false;
    }
    const auto* destDest = dest->GetOutputEdges().begin()->second;
    if (destDest->GetProcessor()->GetProcessorType() == EProcessorType::Filter) {
        condNode->GetProcessorAs<TIndexCheckerProcessor>()->SetApplyToFilter();
        return true;
    }
    return false;
}

std::optional<TResourceAddress> TGraph::GetOriginalAddress(TGraphNode* condNode) const {
    if (condNode->GetProcessor()->GetProcessorType() == EProcessorType::AssembleOriginalData) {
        const auto proc = condNode->GetProcessorAs<TOriginalColumnAccessorProcessor>();
        return TResourceAddress(proc->GetColumnId(), proc->GetSubColumnName());
    } else if (condNode->GetProcessor()->GetProcessorType() == EProcessorType::Calculation) {
        const auto proc = condNode->GetProcessorAs<TCalculationProcessor>();
        if (!proc->GetKernelLogic()) {
            return std::nullopt;
        }
        if (proc->GetKernelLogic()->GetClassName() != TGetJsonPath::GetClassNameStatic()) {
            return std::nullopt;
        }
        if (proc->GetInput().size() != 2) {
            return std::nullopt;
        }
        auto nodeData = GetProducerVerified(proc->GetInput()[0].GetColumnId());
        auto nodePath = GetProducerVerified(proc->GetInput()[1].GetColumnId());
        if (nodeData->GetProcessor()->GetOutput().size() != 1) {
            return std::nullopt;
        }
        if (!nodeData->Is(EProcessorType::AssembleOriginalData)) {
            return std::nullopt;
        }
        if (!nodePath->Is(EProcessorType::Const)) {
            return std::nullopt;
        }
        auto constProc = nodePath->GetProcessorAs<TConstProcessor>();
        TString path;
        if (constProc->GetScalarConstant()->type->id() == arrow::utf8()->id() ||
            constProc->GetScalarConstant()->type->id() == arrow::binary()->id()) {
            path = constProc->GetScalarConstant()->ToString();
            if (path.StartsWith("$.")) {
                path = path.substr(2);
            }
            if (path.StartsWith("\"") && path.EndsWith("\"")) {
                if (path.size() < 2) {
                    return std::nullopt;
                }
                path = path.substr(1, path.size() - 2);
            }
            if (!path) {
                return std::nullopt;
            }
        } else {
            return std::nullopt;
        }
        return TResourceAddress(nodeData->GetProcessor()->GetOutput()[0].GetColumnId(), path);
    } else {
        return std::nullopt;
    }
}

TConclusion<bool> TGraph::OptimizeConditionsForIndexes(TGraphNode* condNode) {
    if (condNode->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    if (condNode->GetProcessor()->GetInput().size() != 2) {
        return false;
    }
    auto calc = condNode->GetProcessorAs<TCalculationProcessor>();
    if (!calc->GetYqlOperationId()) {
        return false;
    }
    if (condNode->GetOutputEdges().size() != 1) {
        return false;
    }
    auto dataNode = GetProducerVerified(calc->GetInput().front().GetColumnId());
    auto constNode = GetProducerVerified(calc->GetInput().back().GetColumnId());
    if (constNode->GetProcessor()->GetProcessorType() != EProcessorType::Const) {
        return false;
    }
    std::optional<TResourceAddress> dataAddr = GetOriginalAddress(dataNode);
    if (!dataAddr) {
        return false;
    }
    auto* dest = condNode->GetOutputEdges().begin()->second;
    const ui32 destResourceId = condNode->GetOutputEdges().begin()->first.GetResourceId();
    if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::Equals ||
        (NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::StartsWith ||
        (NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::EndsWith ||
        (NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::StringContains) {
        if (!IndexesConstructed.emplace(condNode->GetIdentifier()).second) {
            return false;
        }
        RemoveEdge(condNode, dest, destResourceId);

        const EIndexCheckOperation indexOperation = [&]() {
            if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::Equals) {
                return EIndexCheckOperation::Equals;
            }
            if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::StartsWith) {
                return EIndexCheckOperation::StartsWith;
            }
            if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::EndsWith) {
                return EIndexCheckOperation::EndsWith;
            }
            if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::StringContains) {
                return EIndexCheckOperation::Contains;
            }
            return EIndexCheckOperation::Contains;
            AFL_VERIFY(false);
        }();

        const ui32 resourceIdxFetch = BuildNextResourceId();
        IDataSource::TFetchIndexContext indexContext(dataAddr->GetColumnId(), dataAddr->GetSubColumnName(), indexOperation);
        auto indexFetchProc = std::make_shared<TOriginalIndexDataProcessor>(resourceIdxFetch, indexContext);
        auto indexFetchNode = AddNode(indexFetchProc);
        RegisterProducer(resourceIdxFetch, indexFetchNode.get());

        const ui32 resourceIdIndexToAnd = BuildNextResourceId();
        auto indexCheckProc = std::make_shared<TIndexCheckerProcessor>(
            resourceIdxFetch, constNode->GetProcessor()->GetOutputColumnIdOnce(), indexContext, resourceIdIndexToAnd);
        auto indexProcNode = AddNode(indexCheckProc);
        RegisterProducer(resourceIdIndexToAnd, indexProcNode.get());
        AddEdge(indexFetchNode.get(), indexProcNode.get(), resourceIdxFetch);
        AddEdge(constNode, indexProcNode.get(), constNode->GetProcessor()->GetOutputColumnIdOnce());

        const ui32 resourceIdEqToAnd = BuildNextResourceId();
        RegisterProducer(resourceIdEqToAnd, condNode);
        calc->SetOutputResourceIdOnce(resourceIdEqToAnd);

        auto andProcessor = std::make_shared<TStreamLogicProcessor>(TColumnChainInfo::BuildVector({ resourceIdEqToAnd, resourceIdIndexToAnd }),
            TColumnChainInfo(destResourceId), NKernels::EOperation::And);
        auto andNode = AddNode(andProcessor);
        AddEdge(andNode.get(), dest, destResourceId);

        AddEdge(indexProcNode.get(), andNode.get(), resourceIdIndexToAnd);
        AddEdge(condNode, andNode.get(), resourceIdEqToAnd);
        ResetProducer(destResourceId, andNode.get());
        return true;
    }
    return false;
}

TConclusion<bool> TGraph::OptimizeFilterWithCoalesce(TGraphNode* cNode) {
    if (cNode->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    const auto calc = cNode->GetProcessorAs<TCalculationProcessor>();
    if (!calc->GetYqlOperationId()) {
        return false;
    }
    if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() != NYql::TKernelRequestBuilder::EBinaryOp::Coalesce) {
        return false;
    }
    if (cNode->GetOutputEdges().size() != 1) {
        return false;
    }
    if (calc->GetInput().size() != 2) {
        return TConclusionStatus::Fail("incorrect coalesce incoming columns (!= 2) : " + ::ToString(calc->GetInput().size()));
    }
    TGraphNode* dataNode = GetProducerVerified(calc->GetInput()[0].GetColumnId());
    TGraphNode* argNode = GetProducerVerified(calc->GetInput()[1].GetColumnId());
    if (argNode->GetProcessor()->GetProcessorType() != EProcessorType::Const) {
        return false;
    }
    auto scalar = argNode->GetProcessorAs<TConstProcessor>()->GetScalarConstant();
    if (!scalar) {
        return TConclusionStatus::Fail("coalesce with null arg is impossible");
    }

    auto* nextNode = cNode->GetOutputEdges().begin()->second;
    if (nextNode->GetProcessor()->GetProcessorType() != EProcessorType::Filter) {
        if (nextNode->GetProcessor()->GetProcessorType() == EProcessorType::Calculation) {
            const auto outputCalc = nextNode->GetProcessorAs<TCalculationProcessor>();
            if (!outputCalc->GetYqlOperationId()) {
                return false;
            }
            if ((NYql::TKernelRequestBuilder::EBinaryOp)*outputCalc->GetYqlOperationId() != NYql::TKernelRequestBuilder::EBinaryOp::And) {
                return false;
            }
        } else if (nextNode->GetProcessor()->GetProcessorType() == EProcessorType::StreamLogic) {
            const auto outputCalc = nextNode->GetProcessorAs<TStreamLogicProcessor>();
            if (outputCalc->GetOperation() != NKernels::EOperation::And) {
                return false;
            }
        }
        if (nextNode->GetOutputEdges().size() != 1) {
            return false;
        }
        if (nextNode->GetOutputEdges().begin()->second->GetProcessor()->GetProcessorType() != EProcessorType::Filter) {
            return false;
        }
    }
    if (scalar) {
        bool doOptimize = false;
        NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TScalar = typename arrow::TypeTraits<T>::ScalarType;
            auto& typedScalar = static_cast<const TScalar&>(*scalar);
            if constexpr (arrow::has_c_type<T>()) {
                doOptimize = (typedScalar.value == 0);
            }
            return true;
        });
        if (!doOptimize) {
            return false;
        }
    }
    nextNode->GetProcessor()->ExchangeInput(cNode->GetProcessor()->GetOutputColumnIdOnce(), dataNode->GetProcessor()->GetOutputColumnIdOnce());
    AddEdge(dataNode, nextNode, dataNode->GetProcessor()->GetOutputColumnIdOnce());
    RemoveNode(cNode->GetIdentifier());
    if (argNode->IsDisconnected()) {
        RemoveNode(argNode->GetIdentifier());
    }
    return true;
}

TConclusionStatus TGraph::Collapse() {
    bool hasChanges = true;
    //    Cerr << DebugJson() << Endl;

    std::vector<TGraphNode*> filters;
    for (auto&& [_, n] : Nodes) {
        if (n->Is(EProcessorType::Filter)) {
            filters.emplace_back(n.get());
        }
    }
    if (filters.size() > 1) {
        const ui32 finalResourceId = BuildNextResourceId();
        auto filterNode = AddNode(std::make_shared<TFilterProcessor>(finalResourceId));
        std::vector<ui32> inputs;
        std::vector<TGraphNode*> inputNodes;
        for (auto&& i : filters) {
            inputs.emplace_back(i->GetProcessor()->GetInputColumnIdOnce());
            inputNodes.emplace_back(GetProducerVerified(i->GetProcessor()->GetInputColumnIdOnce()));
            RemoveNode(i->GetIdentifier());
        }
        auto mergeNode = AddNode(std::make_shared<TStreamLogicProcessor>(
            TColumnChainInfo::BuildVector(inputs), TColumnChainInfo(finalResourceId), NKernels::EOperation::And));
        ui32 idx = 0;
        for (auto&& i : inputNodes) {
            AddEdge(i, mergeNode.get(), inputs[idx]);
            ++idx;
        }
        AddEdge(mergeNode.get(), filterNode.get(), finalResourceId);
        RegisterProducer(finalResourceId, mergeNode.get());
    }

    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            {
                auto conclusion = OptimizeFilterWithCoalesce(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }

            {
                auto conclusion = OptimizeConditionsForIndexes(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }

            {
                auto conclusion = OptimizeConditionsForStream(n.get());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (*conclusion) {
                    hasChanges = true;
                    break;
                }
            }
        }
    }
    hasChanges = true;
    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            auto conclusion = OptimizeForFetchSubColumns(n.get());
            if (conclusion.IsFail()) {
                return conclusion;
            }
            if (*conclusion) {
                hasChanges = true;
                break;
            }
        }
    }
    return TConclusionStatus::Success();
}

class TFilterChain {
private:
    YDB_READONLY_DEF(std::vector<const TGraphNode*>, Nodes);
    ui64 Weight = 0;

public:
    TFilterChain(const std::vector<const TGraphNode*>& nodes)
        : Nodes(nodes) {
        for (auto&& i : nodes) {
            Weight += i->GetProcessor()->GetWeight();
        }
    }

    bool operator<(const TFilterChain& item) const {
        return Weight < item.Weight;
    }
};

std::shared_ptr<NExecution::TCompiledGraph> TGraph::Compile() {
    return std::make_shared<NExecution::TCompiledGraph>(*this, Resolver);
}

void TGraph::RemoveBranch(TGraphNode* from) {
    THashSet<ui32> nodeIdsToRemove;
    THashMap<ui32, TGraphNode*> current;
    current.emplace(from->GetIdentifier(), from);
    nodeIdsToRemove.emplace(from->GetIdentifier());
    while (current.size()) {
        THashMap<ui32, TGraphNode*> next;
        for (auto&& [_, i] : current) {
            for (auto&& [_, e] : i->GetInputEdges()) {
                if (nodeIdsToRemove.emplace(e->GetIdentifier()).second) {
                    next.emplace(e->GetIdentifier(), e);
                }
            }
            for (auto&& [_, e] : i->GetOutputEdges()) {
                if (nodeIdsToRemove.emplace(e->GetIdentifier()).second) {
                    next.emplace(e->GetIdentifier(), e);
                }
            }
        }
        current = next;
    }
    for (auto&& i : nodeIdsToRemove) {
        RemoveNode(i);
    }
}

TString TResourceAddress::DebugString() const {
    if (SubColumnName) {
        return TStringBuilder() << "[" << ColumnId << "," << SubColumnName << "]";
    } else {
        return TStringBuilder() << "[" << ColumnId << "]";
    }
}

}   // namespace NKikimr::NArrow::NSSA::NGraph::NOptimization
