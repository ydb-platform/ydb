#include "execution.h"
#include "index.h"

#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TOriginalIndexDataProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto conclusion = context.GetDataSource()->StartFetchIndex(context, GetOutputColumnIdOnce(), ColumnId, SubColumnName);
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
    result.InsertValue("c_id", ColumnId);
    if (SubColumnName) {
        result.InsertValue("sub_id", SubColumnName);
    }
    return result;
}

TConclusion<IResourceProcessor::EExecutionResult> TIndexCheckerProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& nodeContext) const {
    IDataSource::TCheckIndexContext checkContext(columnId, );

    context.GetDataSource()->CheckIndex(context, GetOutputColumnIdOnce(), );
    context.GetResources()->AddVerified(GetOutputColumnIdOnce(),
        std::make_shared<NAccessor::TSparsedArray>(
            std::make_shared<arrow::BooleanScalar>(true), arrow::boolean(), context.GetResources()->GetRecordsCountActualVerified()),
        false);
    Y_UNUSED(ColumnId);
    return EExecutionResult::Success;
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
