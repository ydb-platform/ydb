#include "transfer.h"
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

THashMap<NKikimr::NOlap::TTabletId, NKikimr::NOlap::NDataSharing::TTaskForTablet> TPathIdData::BuildLinkTabletTasks(
    const std::shared_ptr<IStoragesManager>& storages, const TTabletId selfTabletId, const TTransferContext& context, const TVersionedIndex& index) {
    THashMap<TString, THashSet<TUnifiedBlobId>> blobIds;
    for (auto&& i : Portions) {
        auto schema = i.GetSchema(index);
        i.FillBlobIdsByStorage(blobIds, schema->GetIndexInfo());
    }

    const std::shared_ptr<TSharedBlobsManager> sharedBlobs = storages->GetSharedBlobsManager();

    THashMap<TString, THashMap<TUnifiedBlobId, TBlobSharing>> blobsInfo;

    for (auto&& i : blobIds) {
        auto sharingManager = sharedBlobs->GetStorageManagerVerified(i.first);
        auto storageManager = storages->GetOperatorVerified(i.first);
        auto storeCategories = sharingManager->BuildStoreCategories(i.second);
        auto& blobs = blobsInfo[i.first];
        for (auto it = storeCategories.GetDirect().GetIterator(); it.IsValid(); ++it) {
            auto itSharing = blobs.find(it.GetBlobId());
            if (itSharing == blobs.end()) {
                itSharing = blobs.emplace(it.GetBlobId(), TBlobSharing(i.first, it.GetBlobId())).first;
            }
        }
        for (auto it = storeCategories.GetSharing().GetIterator(); it.IsValid(); ++it) {
            auto itSharing = blobs.find(it.GetBlobId());
            if (itSharing == blobs.end()) {
                itSharing = blobs.emplace(it.GetBlobId(), TBlobSharing(i.first, it.GetBlobId())).first;
            }
            if (storageManager->HasToDelete(it.GetBlobId(), it.GetTabletId())) {
                continue;
            }
            itSharing->second.AddShared(it.GetTabletId());
        }
        for (auto it = storeCategories.GetBorrowed().GetIterator(); it.IsValid(); ++it) {
            auto itSharing = blobs.find(it.GetBlobId());
            if (itSharing == blobs.end()) {
                itSharing = blobs.emplace(it.GetBlobId(), TBlobSharing(i.first, it.GetBlobId())).first;
            }
            itSharing->second.AddBorrowed(it.GetTabletId());
        }
    }

    THashMap<TTabletId, TTaskForTablet> globalTabletTasks;
    for (auto&& [storageId, blobs] : blobsInfo) {
        THashMap<TTabletId, TStorageTabletTask> storageTabletTasks;
        for (auto&& [_, blobInfo] : blobs) {
            THashMap<TTabletId, TStorageTabletTask> blobTabletTasks = context.GetMoving() ? blobInfo.BuildTabletTasksOnMove(context, selfTabletId, storageId) : blobInfo.BuildTabletTasksOnCopy(context, selfTabletId, storageId);
            for (auto&& [tId, tInfo] : blobTabletTasks) {
                auto itTablet = storageTabletTasks.find(tId);
                if (itTablet == storageTabletTasks.end()) {
                    itTablet = storageTabletTasks.emplace(tId, TStorageTabletTask(storageId, tId)).first;
                }
                itTablet->second.Merge(tInfo);
            }
        }
        for (auto&& i : storageTabletTasks) {
            auto it = globalTabletTasks.find(i.first);
            if (it == globalTabletTasks.end()) {
                it = globalTabletTasks.emplace(i.first, TTaskForTablet(i.first)).first;
            }
            it->second.AddStorage(std::move(i.second));
        }
    }
    return globalTabletTasks;
}

}
