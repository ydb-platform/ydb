#include "assign_internal.h"
#include "filter.h"
#include "graph.h"
#include "original.h"

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

TConclusionStatus TGraph::Collapse() {
    bool hasChanges = true;
//    Cerr << DebugJson() << Endl;
    while (hasChanges) {
        hasChanges = false;
        for (auto&& [_, n] : Nodes) {
            if (n->GetProcessor()->GetProcessorType() == EProcessorType::Filter) {
                if (n->GetDataFrom().size() != 1) {
                    return TConclusionStatus::Fail("incorrect filter incoming columns (!= 1) : " + ::ToString(n->GetDataFrom().size()));
                }
                auto* first = n->GetDataFrom().begin()->second;
                if (first->GetProcessor()->GetProcessorType() == EProcessorType::Calculation) {
                    auto calc = first->GetProcessorAs<TCalculationProcessor>();
                    if (calc->GetYqlOperationId() &&
                        (NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::And) {
                        for (auto&& c : calc->GetInput()) {
                            AddNode(std::make_shared<TFilterProcessor>(TColumnChainInfo(c)));
                        }
                        DetachNode(n.get());
                        DetachNode(first);
                        RemoveNode(n.get());
                        RemoveNode(first);
                        Cerr << DebugJson() << Endl;
                        hasChanges = true;
                        break;
                    }
                }
                if (first->GetProcessor()->GetProcessorType() == EProcessorType::Calculation) {
                    auto calc = first->GetProcessorAs<TCalculationProcessor>();
                    if (calc->GetYqlOperationId() &&
                        (NYql::TKernelRequestBuilder::EBinaryOp)*calc->GetYqlOperationId() == NYql::TKernelRequestBuilder::EBinaryOp::Coalesce) {
                        if (calc->GetInput().size() != 2) {
                            return TConclusionStatus::Fail(
                                "incorrect coalesce incoming columns (!= 2) : " + ::ToString(calc->GetInput().size()));
                        }
                        TGraphNode* dataNode = GetProducerVerified(calc->GetInput().front().GetColumnId());
                        for (auto&& c : dataNode->GetProcessor()->GetOutput()) {
                            AddNode(std::make_shared<TFilterProcessor>(TColumnChainInfo(c.GetColumnId())));
                        }
                        DetachNode(n.get());
                        DetachNode(first);
                        RemoveNode(n.get());
                        RemoveNode(first);
                        hasChanges = true;
                        break;
                    }
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
    bool found = false;
    for (auto&& [_, i] : Nodes) {
        if (i->GetProcessor()->GetProcessorType() == EProcessorType::Projection) {
            if (found) {
                return TConclusionStatus::Fail("detected projections duplication");
            }
            found = true;
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
    if (!found) {
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
