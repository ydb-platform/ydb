#include "execution.h"
#include "index.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TOriginalIndexDataProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto conclusion = context.GetDataSource()->StartFetchIndex(context, IndexContext);
    if (conclusion.IsFail()) {
        return conclusion;
    } else if (*conclusion) {
        return EExecutionResult::InBackground;
    } else {
        return EExecutionResult::Success;
    }
}

NJson::TJsonValue TOriginalIndexDataProcessor::DoDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("c_id", IndexContext.GetColumnId());
    if (IndexContext.GetSubColumnName()) {
        result.InsertValue("sub_id", IndexContext.GetSubColumnName());
    }
    result.InsertValue("op", ::ToString(IndexContext.GetOperation()));
    return result;
}

TConclusion<IResourceProcessor::EExecutionResult> TIndexCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto scalarConst = context.GetResources()->GetConstantScalarVerified(GetInput().back().GetColumnId());

    auto conclusion = context.GetDataSource()->CheckIndex(context, IndexContext, scalarConst);
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
