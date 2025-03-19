#include "collection.h"
#include "execution.h"
#include "projection.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TProjectionProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    context.GetResources()->RemainOnly(TColumnChainInfo::ExtractColumnIds(GetInput()), true);
    if (context.GetLimit()) {
        context.GetResources()->CutFilter(context.GetResources()->GetRecordsCountActualVerified(), *context.GetLimit(), context.GetReverse());
    }
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
