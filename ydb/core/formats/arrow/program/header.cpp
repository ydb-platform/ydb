#include "execution.h"
#include "header.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TOriginalHeaderDataProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto conclusion = context.GetDataSource()->StartFetchHeader(context, HeaderContext);
    if (conclusion.IsFail()) {
        return conclusion;
    } else if (*conclusion) {
        return EExecutionResult::InBackground;
    } else {
        return EExecutionResult::Success;
    }
}

NJson::TJsonValue TOriginalHeaderDataProcessor::DoDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("c_id", HeaderContext.GetColumnId());
    result.InsertValue("sub_id", HeaderContext.GetSubColumnName());
    return result;
}

TConclusion<IResourceProcessor::EExecutionResult> THeaderCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto conclusion = context.GetDataSource()->CheckHeader(context, HeaderContext);
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
