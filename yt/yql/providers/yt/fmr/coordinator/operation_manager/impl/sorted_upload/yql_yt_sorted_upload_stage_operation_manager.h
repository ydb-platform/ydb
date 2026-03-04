
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base/yql_yt_base_stage_operation_manager.h>

namespace NYql::NFmr {

class TSortedUploadStageOperationManager: public TFmrStageOperationManagerBase {
public:
    void OnTaskCompleted(const TStatistics& stats) override;
    std::vector<TString> GetOperationResult() override;

protected:
    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) override;
    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) override;

private:
    std::vector<TString> FragmentResultsYson_;
};

IFmrStageOperationManager::TPtr MakeSortedUploadStageOperationManager();

}
