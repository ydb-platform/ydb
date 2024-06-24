#include "write_controller.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>

namespace NKikimr::NColumnShard {

void IWriteController::OnBlobWriteResult(const TEvBlobStorage::TEvPutResult& result) {
    NOlap::TUnifiedBlobId blobId(result.GroupId.GetRawId(), result.Id);
    auto it = WaitingActions.find(result.StorageId ? result.StorageId : NOlap::IStoragesManager::DefaultStorageId);
    AFL_VERIFY(it != WaitingActions.end());
    it->second->OnBlobWriteResult(blobId, result.Status);
    if (it->second->IsReady()) {
        WaitingActions.erase(it);
    }
    DoOnBlobWriteResult(result);
}

NKikimr::NOlap::TBlobWriteInfo& IWriteController::AddWriteTask(NOlap::TBlobWriteInfo&& task) {
    auto fullAction = WritingActions.Add(task.GetWriteOperator());
    task.SetWriteOperator(fullAction);
    WaitingActions.emplace(fullAction->GetStorageId(), fullAction);
    WriteTasks.emplace_back(std::move(task));
    return WriteTasks.back();
}

}
