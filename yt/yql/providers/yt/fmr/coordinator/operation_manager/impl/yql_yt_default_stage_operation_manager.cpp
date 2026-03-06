
#include "yql_yt_default_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/upload/yql_yt_upload_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/download/yql_yt_download_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/merge/yql_yt_merge_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_merge/yql_yt_sorted_merge_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/map/yql_yt_map_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_upload/yql_yt_sorted_upload_stage_operation_manager.h>

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

IFmrStageOperationManager::TPtr MakeStageOperationManager(EOperationType operationType) {
    switch (operationType) {
        case EOperationType::Upload:
            return MakeUploadStageOperationManager();
        case EOperationType::Download:
            return MakeDownloadStageOperationManager();
        case EOperationType::Merge:
            return MakeMergeStageOperationManager();
        case EOperationType::SortedMerge:
            return MakeSortedMergeStageOperationManager();
        case EOperationType::Map:
            return MakeMapStageOperationManager();
        case EOperationType::SortedUpload:
            return MakeSortedUploadStageOperationManager();
        default:
            ythrow yexception() << "Unknown operation type for stage operation manager";
    }
}

} // namespace NYql::NFmr
