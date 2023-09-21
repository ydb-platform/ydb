#include "blob_manager_db.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NColumnShard {

bool TBlobManagerDb::LoadLastGcBarrier(TGenStep& lastCollectedGenStep) {
    NIceDb::TNiceDb db(Database);
    ui64 gen = 0;
    ui64 step = 0;
    if (!Schema::GetSpecialValue(db, Schema::EValueIds::LastGcBarrierGen, gen) ||
        !Schema::GetSpecialValue(db, Schema::EValueIds::LastGcBarrierStep, step))
    {
        return false;
    }
    lastCollectedGenStep = {gen, step};
    return true;
}

void TBlobManagerDb::SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) {
    NIceDb::TNiceDb db(Database);
    Schema::SaveSpecialValue(db, Schema::EValueIds::LastGcBarrierGen, std::get<0>(lastCollectedGenStep));
    Schema::SaveSpecialValue(db, Schema::EValueIds::LastGcBarrierStep, std::get<1>(lastCollectedGenStep));
}

bool TBlobManagerDb::LoadLists(std::vector<NOlap::TUnifiedBlobId>& blobsToKeep, std::vector<NOlap::TUnifiedBlobId>& blobsToDelete,
    const NOlap::IBlobGroupSelector* dsGroupSelector)
{
    blobsToKeep.clear();
    blobsToDelete.clear();

    NIceDb::TNiceDb db(Database);

    {
        auto rowset = db.Table<Schema::BlobsToKeep>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::BlobsToKeep::BlobId>();
            NOlap::TUnifiedBlobId unifiedBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error);
            Y_VERIFY(unifiedBlobId.IsValid(), "%s", error.c_str());

            blobsToKeep.push_back(unifiedBlobId);
            if (!rowset.Next())
                return false;
        }
    }

    {
        auto rowset = db.Table<Schema::BlobsToDelete>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::BlobsToDelete::BlobId>();
            NOlap::TUnifiedBlobId unifiedBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error);
            Y_VERIFY(unifiedBlobId.IsValid(), "%s", error.c_str());
            blobsToDelete.push_back(unifiedBlobId);
            if (!rowset.Next())
                return false;
        }
    }

    return true;
}

void TBlobManagerDb::AddBlobToKeep(const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringLegacy()).Update();
}

void TBlobManagerDb::EraseBlobToKeep(const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringLegacy()).Delete();
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringNew()).Delete();
}

void TBlobManagerDb::AddBlobToDelete(const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringLegacy()).Update();
}

void TBlobManagerDb::EraseBlobToDelete(const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringLegacy()).Delete();
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringNew()).Delete();
}

bool TBlobManagerDb::LoadTierLists(const TString& storageId, std::deque<NOlap::TUnifiedBlobId>& blobsToDelete, std::deque<NOlap::TUnifiedBlobId>& draftBlobsToDelete) {
    draftBlobsToDelete.clear();
    blobsToDelete.clear();

    NIceDb::TNiceDb db(Database);

    {
        auto rowset = db.Table<Schema::TierBlobsToDelete>().Prefix(storageId).Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::TierBlobsToDelete::BlobId>();
            NOlap::TUnifiedBlobId unifiedBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            Y_VERIFY(unifiedBlobId.IsValid(), "%s", error.c_str());

            blobsToDelete.emplace_back(std::move(unifiedBlobId));
            if (!rowset.Next())
                return false;
        }
    }

    {
        auto rowset = db.Table<Schema::TierBlobsDraft>().Prefix(storageId).Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::TierBlobsDraft::BlobId>();
            NOlap::TUnifiedBlobId unifiedBlobId = NOlap::TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            Y_VERIFY(unifiedBlobId.IsValid(), "%s", error.c_str());

            draftBlobsToDelete.emplace_back(std::move(unifiedBlobId));
            if (!rowset.Next())
                return false;
        }
    }

    return true;
}

void TBlobManagerDb::AddTierBlobToDelete(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsToDelete>().Key(storageId, blobId.ToStringNew()).Update();
}

void TBlobManagerDb::RemoveTierBlobToDelete(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsToDelete>().Key(storageId, blobId.ToStringNew()).Delete();
}

void TBlobManagerDb::AddTierDraftBlobId(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsDraft>().Key(storageId, blobId.ToStringNew()).Update();
}

void TBlobManagerDb::RemoveTierDraftBlobId(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsDraft>().Key(storageId, blobId.ToStringNew()).Delete();
}

}
