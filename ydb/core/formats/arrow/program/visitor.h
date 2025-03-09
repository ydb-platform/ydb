#pragma once
#include "execution.h"
#include "graph_execute.h"

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {
class TExecutionVisitor: public TCompiledGraph::IVisitor {
private:
    const TProcessorContext Context;
    THashSet<ui32> SkipActivity;
    YDB_READONLY(bool, InBackgroundMarker, false);

public:
    void ResetInBackgroundMarker() {
        AFL_VERIFY(InBackgroundMarker);
        InBackgroundMarker = false;
    }

    TExecutionVisitor(const TProcessorContext& context)
        : Context(context) {
    }

    virtual TConclusionStatus DoOnExit(const TCompiledGraph::TNode& node) override;
    virtual TConclusionStatus DoOnEnter(const TCompiledGraph::TNode& /*node*/) override {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoOnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) override;
};

}   // namespace NKikimr::NArrow::NSSA::NGraph::NExecution
