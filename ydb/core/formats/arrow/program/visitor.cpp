#include "stream_logic.h"
#include "visitor.h"

namespace NKikimr::NArrow::NSSA::NGraph::NExecution {

TConclusion<TCompiledGraph::IVisitor::EVisitStatus> TExecutionVisitor::DoOnExit(const TCompiledGraph::TNode& node) {
    if (!SkipActivity.size()) {
        if (ExecutionNode) {
            AFL_VERIFY(Executed);
            AFL_VERIFY(ExecutionNode->GetIdentifier() == node.GetIdentifier());
            ExecutionNode = nullptr;
            for (auto&& i : node.GetRemoveResourceIds()) {
                Context.MutableResources().Remove(i, true);
            }
            return EVisitStatus::Finished;
        } else {
            Executed = false;
            ExecutionNode = &node;
            return EVisitStatus::NeedExecute;
        }
    } else {
        AFL_VERIFY(!ExecutionNode);
        SkipActivity.erase(node.GetIdentifier());
        return EVisitStatus::Skipped;
    }
}

TConclusionStatus TExecutionVisitor::DoOnComeback(const TCompiledGraph::TNode& node, const std::vector<TColumnChainInfo>& readyInputs) {
    AFL_VERIFY(!ExecutionNode)("id", node.GetIdentifier())("ready", JoinSeq(",", TColumnChainInfo::ExtractColumnIds(readyInputs)));
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
