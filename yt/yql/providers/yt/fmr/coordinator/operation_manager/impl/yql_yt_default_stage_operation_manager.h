
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/interface/yql_yt_stage_operation_manager.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NYql::NFmr {

IFmrStageOperationManager::TPtr MakeStageOperationManager(EOperationType operationType, TIntrusivePtr<IRandomProvider> randomProvider);

} // namespace NYql::NFmr
