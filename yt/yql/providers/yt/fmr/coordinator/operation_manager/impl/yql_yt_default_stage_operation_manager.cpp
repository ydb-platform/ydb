
#include "yql_yt_default_stage_operation_manager.h"

#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/upload/yql_yt_upload_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/download/yql_yt_download_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/merge/yql_yt_merge_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_merge/yql_yt_sorted_merge_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/map/yql_yt_map_stage_operation_manager.h>
#include <yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_upload/yql_yt_sorted_upload_stage_operation_manager.h>

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

IFmrStageOperationManager::TPtr MakeStageOperationManager(ETaskType taskType) {
    switch (taskType) {
        case ETaskType::Upload:
            return MakeUploadStageOperationManager();
        case ETaskType::Download:
            return MakeDownloadStageOperationManager();
        case ETaskType::Merge:
            return MakeMergeStageOperationManager();
        case ETaskType::SortedMerge:
            return MakeSortedMergeStageOperationManager();
        case ETaskType::Map:
            return MakeMapStageOperationManager();
        case ETaskType::SortedUpload:
            return MakeSortedUploadStageOperationManager();
        default:
            YQL_ENSURE(false, "Unknown task type for stage operation manager");
    }
}

}
