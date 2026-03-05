
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/interface/yql_yt_stage_operation_manager.h>

namespace NYql::NFmr {

IFmrStageOperationManager::TPtr MakeStageOperationManager(EOperationType operationType);

} // namespace NYql::NFmr
