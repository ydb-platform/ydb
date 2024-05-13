#include "modification.h"
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/data_sharing/modification/transactions/tx_change_blobs_owning.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimr::TConclusion<std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction>> TTaskForTablet::BuildModificationTransaction(NColumnShard::TColumnShard* self, const TTabletId initiator, const TString& sessionId, const ui64 packIdx, const std::shared_ptr<TTaskForTablet>& selfPtr) {
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxApplyLinksModification(self, selfPtr, sessionId, initiator, packIdx));
}

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

void TStorageTabletTask::ApplyForDB(NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TStorageSharedBlobsManager>& manager) const {
    for (auto&& i : RemapOwner) {
        manager->CASBorrowedBlobsDB(txc, i.second.GetFrom(), i.second.GetTo(), {i.first});
    }
    manager->WriteBorrowedBlobsDB(txc, InitOwner);
    manager->WriteSharedBlobsDB(txc, AddSharingLinks);
    manager->RemoveSharedBlobsDB(txc, RemoveSharingLinks);
}

void TStorageTabletTask::ApplyForRuntime(const std::shared_ptr<TStorageSharedBlobsManager>& manager) const {
    for (auto&& i : RemapOwner) {
        manager->CASBorrowedBlobs(i.second.GetFrom(), i.second.GetTo(), {i.first});
    }
    manager->AddBorrowedBlobs(InitOwner);
    Y_UNUSED(manager->AddSharedBlobs(AddSharingLinks));
    manager->RemoveSharedBlobs(RemoveSharingLinks);
}

}