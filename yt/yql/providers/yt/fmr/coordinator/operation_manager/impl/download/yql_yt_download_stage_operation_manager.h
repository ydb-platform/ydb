
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base/yql_yt_base_stage_operation_manager.h>

namespace NYql::NFmr {

class TDownloadStageOperationManager: public TFmrStageOperationManagerBase {
public:
protected:
    TPartitionResult PartitionOperationImpl(const TPrepareOperationStageContext& context) override;
    TGenerateTasksResult GenerateTasksImpl(const TGenerateTasksContext& context) override;
};

IFmrStageOperationManager::TPtr MakeDownloadStageOperationManager();

}
