
#include "yql_yt_base_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/yql_yt_partition_settings_helpers.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

TFmrStageOperationManagerBase::TFmrStageOperationManagerBase(TIntrusivePtr<IRandomProvider> randomProvider)
    : RandomProvider_(randomProvider)
{
}

TPrepareStageResult TFmrStageOperationManagerBase::PrepareOperationStage(
    const TPrepareOperationStageContext& context
) {
    YQL_ENSURE(!Finished_, "Operation has already finished all stages");
    try {
        auto partitionResult = PartitionOperationImpl(context);
        if (partitionResult.Error) {
            return TPrepareStageResult{.Error = partitionResult.Error};
        }
        return TPrepareStageResult{.PartitionResult = std::move(partitionResult)};
    } catch (const std::exception& e) {
        return TPrepareStageResult{.Error = TFmrError{
            .Component = EFmrComponent::Coordinator,
            .Reason = ParseFmrReasonFromErrorMessage(e.what()),
            .ErrorMessage = e.what()
        }};
    } catch (...) {
        return TPrepareStageResult{.Error = TFmrError{
            .Component = EFmrComponent::Coordinator,
            .Reason = EFmrErrorReason::Unknown,
            .ErrorMessage = CurrentExceptionMessage()
        }};
    }
}

TGenerateTasksResult TFmrStageOperationManagerBase::GenerateTasksForCurrentStage(
    const TGenerateTasksContext& context
) {
    YQL_ENSURE(!Finished_, "Operation has already finished all stages");

    auto fmrResourceTasksResult = PartitionFmrResourcesIntoTasks(context.FmrResources, context.FmrOperationSpec, context.PartIdsForTables, context.PartIdStats);
    if (fmrResourceTasksResult.Error) {
        return TGenerateTasksResult{.Error = fmrResourceTasksResult.Error};
    }

    auto generateResult = GenerateTasksImpl(context);
    if (generateResult.Error) {
        return generateResult;
    }

    generateResult.FmrResourceTasks = std::move(fmrResourceTasksResult.Tasks);
    return generateResult;
}

TAdvanceStageResult TFmrStageOperationManagerBase::AdvanceToNextStage() {
    Finished_ = true;
    return TAdvanceStageResult{.HasNextStage = false};
}

TString TFmrStageOperationManagerBase::GenerateId() {
    return GetGuidAsString(RandomProvider_->GenGuid());
}

} // namespace NYql::NFmr
