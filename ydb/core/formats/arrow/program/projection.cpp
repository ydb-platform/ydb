#include "collection.h"
#include "execution.h"
#include "projection.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TProjectionProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    context.MutableResources().RemainOnly(TColumnChainInfo::ExtractColumnIds(GetInput()), true);
    if (context.GetLimit()) {
        context.MutableResources().CutFilter(context.GetResources().GetRecordsCountRobustVerified(), *context.GetLimit(), context.GetReverse());
    }
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
