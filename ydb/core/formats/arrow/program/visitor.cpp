#include "stream_logic.h"
#include "visitor.h"

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {

TConclusionStatus TExecutionVisitor::DoOnExit(const TCompiledGraph::TNode& node) {
    AFL_VERIFY(!InBackgroundMarker);
    if (!SkipActivity.size()) {
        auto conclusion = node.GetProcessor()->Execute(Context, node);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        if (*conclusion == IResourceProcessor::EExecutionResult::InBackground) {
            InBackgroundMarker = true;
        }
    }
    SkipActivity.erase(node.GetIdentifier());
    for (auto&& i : node.GetRemoveResourceIds()) {
        Context.GetResources()->Remove(i, true);
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TExecutionVisitor::DoOnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) {
    AFL_VERIFY(!InBackgroundMarker);
    if (node.GetProcessor()->GetProcessorType() == EProcessorType::StreamLogic) {
        const TStreamLogicProcessor* streamProc = static_cast<const TStreamLogicProcessor*>(node.GetProcessor().get());
        for (auto&& i : readyInputs) {
            auto conclusion = streamProc->OnInputReady(i.GetColumnId(), Context, node);
            if (conclusion.IsFail()) {
                return conclusion;
            }
            if (*conclusion) {
                SkipActivity.emplace(node.GetIdentifier());
            }
        }
    }
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NSSA::NGraph::NExecution
