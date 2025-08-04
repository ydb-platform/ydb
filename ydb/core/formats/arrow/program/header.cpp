#include "execution.h"
#include "header.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> THeaderCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto source = context.GetDataSource().lock();
    if (!source) {
        return TConclusionStatus::Fail("source was destroyed before (header check start)");
    }
    auto conclusion = source->CheckHeader(context, HeaderContext);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    if (conclusion->IsTotalDenyFilter()) {
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
            NAccessor::TSparsedArray::BuildFalseArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
    } else if (conclusion->IsTotalAllowFilter() || !ApplyToFilterFlag) {
        context.MutableResources().AddVerified(GetOutputColumnIdOnce(),
            NAccessor::TSparsedArray::BuildTrueArrayUI8(context.GetResources().GetRecordsCountRobustVerified()), false);
    } else {
        context.MutableResources().AddFilter(*conclusion);
    }
    return IResourceProcessor::EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
