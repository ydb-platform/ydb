#include "assign_const.h"
#include "assign_internal.h"
#include "filter.h"
#include "graph.h"
#include "original.h"

#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA::NOptimization {

TGraph::TGraph(std::vector<std::shared_ptr<IResourceProcessor>>&& processors, const IColumnResolver& resolver) {
    for (auto&& i : processors) {
        auto node = std::make_shared<TGraphNode>(i);
        Nodes.emplace(node->GetIdentifier(), node);
        for (auto&& output : i->GetOutput()) {
            AFL_VERIFY(Producers.emplace(output.GetColumnId(), node.get()).second);
        }
        for (auto&& input : i->GetInput()) {
            if (Producers.find(input.GetColumnId()) != Producers.end()) {
                continue;
            }
            const TString name = resolver.GetColumnName(input.GetColumnId(), false);
            if (!!name) {
                auto nodeInput = std::make_shared<TGraphNode>(
                    std::make_shared<TOriginalColumnProcessor>(input.GetColumnId(), resolver.GetColumnName(input.GetColumnId())));
                Nodes.emplace(nodeInput->GetIdentifier(), nodeInput);
                Producers.emplace(input.GetColumnId(), nodeInput.get());
            }
        }
    }
    for (auto&& [_, i] : Nodes) {
        for (auto&& p : i->GetProcessor()->GetInput()) {
            auto node = GetProducerVerified(p.GetColumnId());
            node->AddDataTo(p.GetColumnId(), i);
            i->AddDataFrom(p.GetColumnId(), node);
        }
    }
}

TConclusion<bool> TGraph::OptimizeFilter(TGraphNode* filterNode) {
    if (filterNode->GetProcessor()->GetProcessorType() != EProcessorType::Filter) {
        return false;
    }
    if (filterNode->GetDataFrom().size() != 1) {
        return TConclusionStatus::Fail("incorrect filter incoming columns (!= 1) : " + ::ToString(filterNode->GetDataFrom().size()));
    }
    auto* first = filterNode->GetDataFrom().begin()->second;
    if (first->GetProcessor()->GetProcessorType() != EProcessorType::Calculation) {
        return false;
    }
    auto calc = first->GetProcessorAs<TCalculationProcessor>();
    if (!calc->GetYqlOperationId()) {
        return false;
    }
    {
        auto conclusion = OptimizeFilterWithAnd(filterNode, first, calc);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (*conclusion) {
            return true;
        }
    }
    {
        auto conclusion = OptimizeFilterWithCoalesce(filterNode, first, calc);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (*conclusion) {
            return true;
        }
    }
    return false;
}

TConclusion<bool> TGraph::OptimizeFilterWithAnd(
    TGraphNode* filterNode, TGraphNode* filterArg, const std::shared_ptr<TCalculationProcessor>& calc) {
    if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() != NYql::TKernelRequestBuilder::EBinaryOp::And) {
        return false;
    }
    if (calc->GetInput().size() < 2) {
        return TConclusionStatus::Fail("incorrect and operation incoming columns (< 2) : " + ::ToString(calc->GetInput().size()));
    }
    for (auto&& c : calc->GetInput()) {
        AddNode(std::make_shared<TFilterProcessor>(TColumnChainInfo(c)));
    }
    DetachNode(filterNode);
    DetachNode(filterArg);
    RemoveNode(filterNode);
    RemoveNode(filterArg);
    Cerr << DebugJson() << Endl;
    return true;
}

TConclusion<bool> TGraph::OptimizeFilterWithCoalesce(
    TGraphNode* filterNode, TGraphNode* filterArg, const std::shared_ptr<TCalculationProcessor>& calc) {
    if ((NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() != NYql::TKernelRequestBuilder::EBinaryOp::Coalesce) {
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
    for (auto&& c : dataNode->GetProcessor()->GetOutput()) {
        AddNode(std::make_shared<TFilterProcessor>(TColumnChainInfo(c.GetColumnId())));
    }
    DetachNode(filterNode);
    DetachNode(filterArg);
    RemoveNode(filterNode);
    RemoveNode(filterArg);
    return true;
}

TConclusionStatus TGraph::Collapse() {
    bool hasChanges = true;
    //    Cerr << DebugJson() << Endl;
    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            {
                auto conclusion = OptimizeFilter(n.get());
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

TConclusion<std::vector<std::shared_ptr<IResourceProcessor>>> TGraph::BuildChain() {
    std::vector<TFilterChain> nodeChains;
    THashSet<i64> readyNodeIds;
    for (auto&& [_, i] : Nodes) {
        if (i->GetProcessor()->GetProcessorType() == EProcessorType::Filter) {
            std::vector<const TGraphNode*> chain = i->GetFetchingChain();
            std::vector<const TGraphNode*> actualChain;
            for (auto&& c : chain) {
                if (readyNodeIds.emplace(c->GetIdentifier()).second) {
                    actualChain.emplace_back(c);
                }
            }
            AFL_VERIFY(actualChain.size());
            nodeChains.emplace_back(std::move(actualChain));
        }
    }
    std::sort(nodeChains.begin(), nodeChains.end());
    ui32 foundCount = 0;
    for (auto&& [_, i] : Nodes) {
        if (i->GetProcessor()->GetProcessorType() != EProcessorType::Filter && i->GetProcessor()->GetOutput().empty()) {
            ++foundCount;
            std::vector<const TGraphNode*> chain = i->GetFetchingChain();
            std::vector<const TGraphNode*> actualChain;
            for (auto&& c : chain) {
                if (readyNodeIds.emplace(c->GetIdentifier()).second) {
                    actualChain.emplace_back(c);
                }
            }
            AFL_VERIFY(actualChain.size());
            nodeChains.emplace_back(std::move(actualChain));
        }
    }
    if (!foundCount) {
        return TConclusionStatus::Fail("not found projection node");
    }
    std::vector<std::shared_ptr<IResourceProcessor>> result;
    for (auto&& c : nodeChains) {
        for (auto&& p : c.GetNodes()) {
            if (p->GetProcessor()->GetProcessorType() != EProcessorType::Original) {
                result.emplace_back(p->GetProcessor());
            }
        }
    }
    return result;
}

void TGraph::AddNode(const std::shared_ptr<IResourceProcessor>& processor) {
    auto node = std::make_shared<TGraphNode>(processor);
    Nodes.emplace(node->GetIdentifier(), node);
    for (auto&& i : processor->GetInput()) {
        auto nodeProducer = GetProducerVerified(i.GetColumnId());
        nodeProducer->AddDataTo(i.GetColumnId(), node);
        node->AddDataFrom(i.GetColumnId(), nodeProducer);
    }
}

void TGraph::RemoveNode(TGraphNode* node) {
    Nodes.erase(node->GetIdentifier());
}

void TGraph::DetachNode(TGraphNode* node) {
    for (auto&& i : node->GetDataFrom()) {
        i.second->RemoveDataTo(i.first.AnotherNodeId(node->GetIdentifier()));
    }
    for (auto&& i : node->GetDataTo()) {
        i.second->RemoveDataFrom(i.first.AnotherNodeId(node->GetIdentifier()));
    }
}

std::vector<const TGraphNode*> TGraphNode::GetFetchingChain() const {
    std::vector<const TGraphNode*> result;
    result.emplace_back(this);
    ui32 frontStart = 0;
    ui32 frontFinish = result.size();
    while (frontFinish > frontStart) {
        for (ui32 i = frontStart; i < frontFinish; ++i) {
            for (auto&& input : result[i]->GetDataFrom()) {
                result.emplace_back(input.second);
            }
        }
        frontStart = frontFinish;
        frontFinish = result.size();
    }
    std::reverse(result.begin(), result.end());
    return result;
}

}   // namespace NKikimr::NArrow::NSSA::NOptimization
