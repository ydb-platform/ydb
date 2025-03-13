#include "execution.h"
#include "original.h"

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TOriginalColumnDataProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    auto acc = context.GetResources()->GetAccessorOptional(ColumnId);
    if (!!acc) {
        if (!SubColumnName) {
            if (acc->HasWholeDataVolume()) {
                return EExecutionResult::Success;
            } else {
                context.GetResources()->Remove(ColumnId);
            }
        } else if (acc->HasSubColumnData(SubColumnName)) {
            return EExecutionResult::Success;
        }
    }
    auto conclusion = context.GetDataSource()->StartFetchData(context, GetOutputColumnIdOnce(), SubColumnName);
    if (conclusion.IsFail()) {
        return conclusion;
    } else if (*conclusion) {
        return EExecutionResult::InBackground;
    } else {
        return EExecutionResult::Success;
    }
}

TConclusion<IResourceProcessor::EExecutionResult> TOriginalColumnAccessorProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    const auto acc = context.GetResources()->GetAccessorOptional(GetOutputColumnIdOnce());
    if (!acc || !acc->HasSubColumnData(SubColumnName)) {
        context.GetDataSource()->AssembleAccessor(context, GetOutputColumnIdOnce(), SubColumnName);
    }
    return EExecutionResult::Success;
}

}   // namespace NKikimr::NArrow::NSSA
