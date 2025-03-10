#include "stream_logic.h"
#include "visitor.h"

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {

TConclusion<IResourceProcessor::EExecutionResult> TExecutionVisitor::DoOnExit(const TCompiledGraph::TNode& node) {
    if (InBackgroundMarker) {
        InBackgroundMarker = false;
        return IResourceProcessor::EExecutionResult::Success;
    }
    AFL_VERIFY(!InBackgroundMarker);
    IResourceProcessor::EExecutionResult result = IResourceProcessor::EExecutionResult::Skipped;
    if (!SkipActivity.size()) {
        auto conclusion = node.GetProcessor()->Execute(Context, node);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        result = *conclusion;
        if (*conclusion == IResourceProcessor::EExecutionResult::InBackground) {
            InBackgroundMarker = true;
        }
    }
    SkipActivity.erase(node.GetIdentifier());
    for (auto&& i : node.GetRemoveResourceIds()) {
        Context.GetResources()->Remove(i, true);
    }
    return result;
}

TConclusionStatus TExecutionVisitor::DoOnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) {
    AFL_VERIFY(!InBackgroundMarker)("id", node.GetIdentifier())("ready", JoinSeq(",", TColumnChainInfo::ExtractColumnIds(readyInputs)));
    if (SkipActivity.empty() && node.GetProcessor()->GetProcessorType() == EProcessorType::StreamLogic) {
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
