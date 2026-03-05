
#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base/yql_yt_base_stage_operation_manager.h>

namespace NYql::NFmr {

IFmrStageOperationManager::TPtr MakeSortedUploadStageOperationManager();

} // namespace NYql::NFmr
