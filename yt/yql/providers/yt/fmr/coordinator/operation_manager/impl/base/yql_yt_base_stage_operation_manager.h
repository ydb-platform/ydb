
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/interface/yql_yt_stage_operation_manager.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NYql::NFmr {

class TFmrStageOperationManagerBase: public IFmrStageOperationManager {
public:
    TFmrStageOperationManagerBase(TIntrusivePtr<IRandomProvider> randomProvider);

    TPrepareStageResult PrepareOperationStage(
        const TPrepareOperationStageContext& context
    ) override;

    TGenerateTasksResult GenerateTasksForCurrentStage(
        const TGenerateTasksContext& context
    ) override;

    TAdvanceStageResult AdvanceToNextStage() override;

protected:
    virtual TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) = 0;
    virtual TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) = 0;
    virtual TString GenerateId();

    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    bool Finished_ = false;
};

} // namespace NYql::NFmr
