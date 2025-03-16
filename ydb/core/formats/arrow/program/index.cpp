#include "execution.h"
#include "index.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TIndexCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto scalarConst = context.GetResources()->GetConstantScalarVerified(GetInput().back().GetColumnId());

    auto source = context.GetDataSource().lock();
    if (!source) {
        return TConclusionStatus::Fail("source was destroyed before (index check start)");
    }
    auto conclusion = source->CheckIndex(context, IndexContext, scalarConst);
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
