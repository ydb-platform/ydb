#include "modification.h"

#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>

namespace NKikimr::NOlap::NDataSharing {

void TTaskForTablet::ApplyForDB(NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TSharedBlobsManager>& manager) const {
    for (auto&& i : TasksByStorage) {
        auto storageManager = manager->GetStorageManagerVerified(i.first);
        i.second.ApplyForDB(txc, storageManager);
    }
}

void TTaskForTablet::ApplyForRuntime(const std::shared_ptr<TSharedBlobsManager>& manager) const {
    for (auto&& i : TasksByStorage) {
        auto storageManager = manager->GetStorageManagerVerified(i.first);
        i.second.ApplyForRuntime(storageManager);
    }
}

void TStorageTabletTask::ApplyForDB(
    NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TStorageSharedBlobsManager>& manager) const {
    for (auto&& i : RemapOwner) {
        manager->CASBorrowedBlobsDB(txc, i.second.GetFrom(), i.second.GetTo(), { i.first });
    }
    manager->WriteBorrowedBlobsDB(txc, InitOwner);
    manager->WriteSharedBlobsDB(txc, AddSharingLinks);
    manager->RemoveSharedBlobsDB(txc, RemoveSharingLinks);
}

void TStorageTabletTask::ApplyForRuntime(const std::shared_ptr<TStorageSharedBlobsManager>& manager) const {
    for (auto&& i : RemapOwner) {
        manager->CASBorrowedBlobs(i.second.GetFrom(), i.second.GetTo(), { i.first });
    }
    manager->AddBorrowedBlobs(InitOwner);
    Y_UNUSED(manager->AddSharedBlobs(AddSharingLinks));
    manager->RemoveSharedBlobs(RemoveSharingLinks);
}

}   // namespace NKikimr::NOlap::NDataSharing
