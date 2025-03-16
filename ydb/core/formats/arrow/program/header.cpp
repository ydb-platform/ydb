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
        context.GetResources()->AddVerified(GetOutputColumnIdOnce(),
            std::make_shared<NAccessor::TSparsedArray>(
                std::make_shared<arrow::UInt8Scalar>(0), arrow::uint8(), context.GetResources()->GetRecordsCountActualVerified()),
            false);
    } else if (conclusion->IsTotalAllowFilter() || !ApplyToFilterFlag) {
        context.GetResources()->AddVerified(GetOutputColumnIdOnce(),
            std::make_shared<NAccessor::TSparsedArray>(
                std::make_shared<arrow::UInt8Scalar>(1), arrow::uint8(), context.GetResources()->GetRecordsCountActualVerified()),
            false);
    } else {
        context.GetResources()->AddFilter(*conclusion);
    }
    return IResourceProcessor::EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
