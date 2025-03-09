#include "collection.h"
#include "execution.h"
#include "projection.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TProjectionProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    context.GetResources()->RemainOnly(TColumnChainInfo::ExtractColumnIds(GetInput()), true);
    if (Limit) {
        context.GetResources()->CutFilter(context.GetResources()->GetRecordsCountActualVerified(), *Limit, context.GetReverse());
    }
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
