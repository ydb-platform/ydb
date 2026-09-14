#pragma once
#include "execution.h"
#include "graph_execute.h"

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {
class TExecutionVisitor: public TCompiledGraph::IVisitor {
private:
    TProcessorContext Context;
    THashSet<ui32> SkipActivity;
    const TCompiledGraph::TNode* ExecutionNode = nullptr;
    bool Executed = false;

public:
    TProcessorContext& MutableContext() {
        return Context;
    }

    const TCompiledGraph::TNode* GetExecutionNode() const {
        return ExecutionNode;
    }

    TConclusion<IResourceProcessor::EExecutionResult> Execute() {
        if (ExecutionNode) {
            Executed = true;
            return ExecutionNode->GetProcessor()->Execute(Context, *ExecutionNode);
        } else {
            return IResourceProcessor::EExecutionResult::Success;
        }
    }

    TExecutionVisitor(TProcessorContext&& context)
        : Context(std::move(context)) {
    }

    virtual TConclusion<IVisitor::EVisitStatus> DoOnExit(const TCompiledGraph::TNode& node) override;
    virtual TConclusionStatus DoOnEnter(const TCompiledGraph::TNode& /*node*/) override {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoOnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) override;
};

}   // namespace NKikimr::NArrow::NSSA::NGraph::NExecution
