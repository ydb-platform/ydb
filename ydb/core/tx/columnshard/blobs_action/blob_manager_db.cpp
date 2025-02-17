#include "blob_manager_db.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

namespace NKikimr::NOlap {

using namespace NKikimr::NColumnShard;

bool TBlobManagerDb::LoadGCBarrierPreparation(TGenStep& genStep) {
    ui64 gen = 0;
    ui64 step = 0;
    NIceDb::TNiceDb db(Database);
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::GCBarrierPreparationGen, gen) ||
        !Schema::GetSpecialValueOpt(db, Schema::EValueIds::GCBarrierPreparationStep, step))
    {
        return false;
    }
    genStep = TGenStep(gen, step);
    return true;
}

void TBlobManagerDb::SaveGCBarrierPreparation(const TGenStep& genStep) {
    SaveGCBarrierPreparation(Database, genStep);
}

void TBlobManagerDb::SaveGCBarrierPreparation(NTable::TDatabase& database, const TGenStep& genStep) {
    NIceDb::TNiceDb db(database);
    Schema::SaveSpecialValue(db, Schema::EValueIds::GCBarrierPreparationGen, genStep.Generation());
    Schema::SaveSpecialValue(db, Schema::EValueIds::GCBarrierPreparationStep, genStep.Step());
}

bool TBlobManagerDb::LoadLastGcBarrier(TGenStep& lastCollectedGenStep) {
    NIceDb::TNiceDb db(Database);
    ui64 gen = 0;
    ui64 step = 0;
    if (!Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastGcBarrierGen, gen) ||
        !Schema::GetSpecialValueOpt(db, Schema::EValueIds::LastGcBarrierStep, step)) {
        return false;
    }
    lastCollectedGenStep = TGenStep(gen, step);
    return true;
}

void TBlobManagerDb::SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) {
    NIceDb::TNiceDb db(Database);
    Schema::SaveSpecialValue(db, Schema::EValueIds::LastGcBarrierGen, lastCollectedGenStep.Generation());
    Schema::SaveSpecialValue(db, Schema::EValueIds::LastGcBarrierStep, lastCollectedGenStep.Step());
}

bool TBlobManagerDb::LoadLists(std::vector<TUnifiedBlobId>& blobsToKeep, TTabletsByBlob& blobsToDelete,
    const IBlobGroupSelector* dsGroupSelector, const TTabletId selfTabletId)
{
    blobsToKeep.clear();
    TTabletsByBlob blobsToDeleteLocal;

    NIceDb::TNiceDb db(Database);

    {
        auto rowset = db.Table<Schema::BlobsToKeep>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::BlobsToKeep::BlobId>();
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error);
            AFL_VERIFY(unifiedBlobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "LOAD_KEEP")("blob_id", unifiedBlobId.ToStringNew());
            blobsToKeep.push_back(unifiedBlobId);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::BlobsToDelete>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::BlobsToDelete::BlobId>();
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error);
            AFL_VERIFY(unifiedBlobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "LOAD_DELETE")("blob_id", unifiedBlobId.ToStringNew());
            blobsToDeleteLocal.Add(selfTabletId, unifiedBlobId);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::BlobsToDeleteWT>().Select();
        if (!rowset.IsReady())
            return false;

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::BlobsToDeleteWT::BlobId>();
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error);
            AFL_VERIFY(unifiedBlobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);
            blobsToDeleteLocal.Add((TTabletId)rowset.GetValue<Schema::BlobsToDeleteWT::TabletId>(), unifiedBlobId);
            if (!rowset.Next()) {
                return false;
            }
        }
    }
    std::swap(blobsToDeleteLocal, blobsToDelete);

    return true;
}

void TBlobManagerDb::AddBlobToKeep(const TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringLegacy()).Update();
}

void TBlobManagerDb::EraseBlobToKeep(const TUnifiedBlobId& blobId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("event", "ERASE_KEEP")("blob_id", blobId.ToStringNew());
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringLegacy()).Delete();
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringNew()).Delete();
}

void TBlobManagerDb::AddBlobToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToDeleteWT>().Key(blobId.ToStringLegacy(), (ui64)tabletId).Update();
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringLegacy()).Update();
}

void TBlobManagerDb::EraseBlobToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringLegacy()).Delete();
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringNew()).Delete();
    db.Table<Schema::BlobsToDeleteWT>().Key(blobId.ToStringLegacy(), (ui64)tabletId).Delete();
    db.Table<Schema::BlobsToDeleteWT>().Key(blobId.ToStringNew(), (ui64)tabletId).Delete();
}

bool TBlobManagerDb::LoadTierLists(const TString& storageId, TTabletsByBlob& blobsToDelete, std::deque<TUnifiedBlobId>& draftBlobsToDelete, const TTabletId selfTabletId) {
    TTabletsByBlob localBlobsToDelete;
    std::deque<TUnifiedBlobId> localDraftBlobsToDelete;

    NIceDb::TNiceDb db(Database);

    {
        auto rowset = db.Table<Schema::TierBlobsToDelete>().Prefix(storageId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::TierBlobsToDelete::BlobId>();
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            AFL_VERIFY(unifiedBlobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);

            localBlobsToDelete.Add(selfTabletId, unifiedBlobId);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::TierBlobsToDeleteWT>().Prefix(storageId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::TierBlobsToDeleteWT::BlobId>();
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            AFL_VERIFY(unifiedBlobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);

            localBlobsToDelete.Add((TTabletId)rowset.GetValue<Schema::TierBlobsToDeleteWT::TabletId>(), unifiedBlobId);
            if (!rowset.Next()) {
                return false;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::TierBlobsDraft>().Prefix(storageId).Select();
        if (!rowset.IsReady()) {
            return false;
        }

        TString error;

        while (!rowset.EndOfSet()) {
            const TString blobIdStr = rowset.GetValue<Schema::TierBlobsDraft::BlobId>();
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, nullptr, error);
            AFL_VERIFY(unifiedBlobId.IsValid())("event", "cannot_parse_blob")("error", error)("original_string", blobIdStr);

            localDraftBlobsToDelete.emplace_back(std::move(unifiedBlobId));
            if (!rowset.Next()) {
                return false;
            }
        }
    }
    
    std::swap(localBlobsToDelete, blobsToDelete);
    std::swap(localDraftBlobsToDelete, draftBlobsToDelete);

    return true;
}

void TBlobManagerDb::AddTierBlobToDelete(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsToDeleteWT>().Key(storageId, blobId.ToStringNew(), (ui64)tabletId).Update();
    db.Table<Schema::TierBlobsToDelete>().Key(storageId, blobId.ToStringNew()).Update();
}

void TBlobManagerDb::RemoveTierBlobToDelete(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsToDeleteWT>().Key(storageId, blobId.ToStringNew(), (ui64)tabletId).Delete();
    db.Table<Schema::TierBlobsToDelete>().Key(storageId, blobId.ToStringNew()).Delete();
}

void TBlobManagerDb::AddTierDraftBlobId(const TString& storageId, const TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsDraft>().Key(storageId, blobId.ToStringNew()).Update();
}

void TBlobManagerDb::RemoveTierDraftBlobId(const TString& storageId, const TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::TierBlobsDraft>().Key(storageId, blobId.ToStringNew()).Delete();
}

void TBlobManagerDb::RemoveBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::SharedBlobIds>().Key(storageId, blobId.ToStringNew(), (ui64)tabletId).Delete();
}

void TBlobManagerDb::AddBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::SharedBlobIds>().Key(storageId, blobId.ToStringNew(), (ui64)tabletId).Update();
}

void TBlobManagerDb::RemoveBorrowedBlob(const TString& storageId, const TUnifiedBlobId& blobId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BorrowedBlobIds>().Key(storageId, blobId.ToStringNew()).Delete();
}

void TBlobManagerDb::AddBorrowedBlob(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) {
    NIceDb::TNiceDb db(Database);
    db.Table<Schema::BorrowedBlobIds>().Key(storageId, blobId.ToStringNew()).Update(NIceDb::TUpdate<Schema::BorrowedBlobIds::TabletId>((ui64)tabletId));
}

}
