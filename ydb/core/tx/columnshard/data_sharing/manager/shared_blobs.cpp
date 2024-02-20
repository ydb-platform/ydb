#include "shared_blobs.h"
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>

namespace NKikimr::NOlap::NDataSharing {

bool TSharedBlobsManager::Load(NTable::TDatabase& database) {
    NIceDb::TNiceDb db(database);
    using namespace NKikimr::NColumnShard;
    {
        auto rowset = db.Table<Schema::SharedBlobIds>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString& storageId = rowset.GetValue<Schema::SharedBlobIds::StorageId>();
            const TString blobIdStr = rowset.GetValue<Schema::SharedBlobIds::BlobId>();
            NOlap::TUnifiedBlobId unifiedBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            Y_ABORT_UNLESS(unifiedBlobId.IsValid(), "%s", error.c_str());

            AFL_VERIFY(GetStorageManagerGuarantee(storageId)->UpsertSharedBlobOnLoad(unifiedBlobId, (TTabletId)rowset.GetValue<Schema::SharedBlobIds::TabletId>()));
            if (!rowset.Next())
                return false;
        }
    }

    {
        auto rowset = db.Table<Schema::BorrowedBlobIds>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString& storageId = rowset.GetValue<Schema::BorrowedBlobIds::StorageId>();
            const TString blobIdStr = rowset.GetValue<Schema::BorrowedBlobIds::BlobId>();
            NOlap::TUnifiedBlobId unifiedBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            Y_ABORT_UNLESS(unifiedBlobId.IsValid(), "%s", error.c_str());

            AFL_VERIFY(GetStorageManagerGuarantee(storageId)->UpsertBorrowedBlobOnLoad(unifiedBlobId, (TTabletId)rowset.GetValue<Schema::BorrowedBlobIds::TabletId>()));
            if (!rowset.Next())
                return false;
        }
    }
    return true;
}

void TStorageSharedBlobsManager::RemoveSharedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletsByBlob& blobIds) {
    NIceDb::TNiceDb db(txc.DB);
    for (auto i = blobIds.GetIterator(); i.IsValid(); ++i) {
        db.Table<NColumnShard::Schema::SharedBlobIds>().Key(StorageId, i.GetBlobId().ToStringNew(), (ui64)i.GetTabletId()).Delete();
    }
}

void TStorageSharedBlobsManager::WriteSharedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletsByBlob& blobIds) {
    NIceDb::TNiceDb db(txc.DB);
    for (auto i = blobIds.GetIterator(); i.IsValid(); ++i) {
        db.Table<NColumnShard::Schema::SharedBlobIds>().Key(StorageId, i.GetBlobId().ToStringNew(), (ui64)i.GetTabletId()).Update();
    }
}

void TStorageSharedBlobsManager::WriteBorrowedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletByBlob& blobIds) {
    NIceDb::TNiceDb db(txc.DB);
    for (auto&& it: blobIds) {
        db.Table<NColumnShard::Schema::BorrowedBlobIds>().Key(StorageId, it.first.ToStringNew()).Update(NIceDb::TUpdate<NColumnShard::Schema::BorrowedBlobIds::TabletId>((ui64)it.second));
    }
}

void TStorageSharedBlobsManager::CASBorrowedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletId tabletIdFrom, const TTabletId tabletIdTo, const THashSet<TUnifiedBlobId>& blobIds) {
    NIceDb::TNiceDb db(txc.DB);
    for (auto&& i : blobIds) {
        auto it = BorrowedBlobIds.find(i);
        AFL_VERIFY(it != BorrowedBlobIds.end())("blob_id", i.ToStringNew());
        AFL_VERIFY(it->second == tabletIdFrom || it->second == tabletIdTo);
        if (tabletIdTo == SelfTabletId) {
            db.Table<NColumnShard::Schema::BorrowedBlobIds>().Key(StorageId, i.ToStringNew()).Delete();
        } else {
            db.Table<NColumnShard::Schema::BorrowedBlobIds>().Key(StorageId, i.ToStringNew()).Update(NIceDb::TUpdate<NColumnShard::Schema::BorrowedBlobIds::TabletId>((ui64)tabletIdTo));
        }
    }
}

void TStorageSharedBlobsManager::OnTransactionExecuteAfterCleaning(const TBlobsCategories& removeTask, NTable::TDatabase& db) {
    TBlobManagerDb dbBlobs(db);
    for (auto&& i : removeTask.GetSharing()) {
        for (auto&& b : i.second) {
            dbBlobs.RemoveBlobSharing(StorageId, b, i.first);
        }
    }
    for (auto&& i : removeTask.GetBorrowed()) {
        for (auto&& blob : i.second) {
            dbBlobs.RemoveBorrowedBlob(StorageId, blob);
        }
    }
}

void TStorageSharedBlobsManager::OnTransactionCompleteAfterCleaning(const TBlobsCategories& removeTask) {
    for (auto i = removeTask.GetSharing().GetIterator(); i.IsValid(); ++i) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("action", "remove_share")("tablet_id_share", i.GetTabletId())("blob_id", i.GetBlobId().ToStringNew());
        SharedBlobIds.Remove(i.GetTabletId(), i.GetBlobId());
    }
    for (auto i = removeTask.GetBorrowed().GetIterator(); i.IsValid(); ++i) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("action", "remove_own")("tablet_id_own", i.GetTabletId())("blob_id", i.GetBlobId().ToStringNew());
        auto it = BorrowedBlobIds.find(i.GetBlobId());
        AFL_VERIFY(it != BorrowedBlobIds.end());
        BorrowedBlobIds.erase(it);
    }
}

}