#include "blob_manager_db.h" 
#include "blob_manager.h" 
#include "columnshard_schema.h" 
 
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
 
bool TBlobManagerDb::LoadLists(TVector<TUnifiedBlobId>& blobsToKeep, TVector<TUnifiedBlobId>& blobsToDelete, 
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
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error); 
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
            TUnifiedBlobId unifiedBlobId = TUnifiedBlobId::ParseFromString(blobIdStr, dsGroupSelector, error); 
            Y_VERIFY(unifiedBlobId.IsValid(), "%s", error.c_str()); 
            blobsToDelete.push_back(unifiedBlobId); 
            if (!rowset.Next()) 
                return false; 
        } 
    } 
 
    return true; 
} 
 
void TBlobManagerDb::AddBlobToKeep(const TUnifiedBlobId& blobId) { 
    NIceDb::TNiceDb db(Database); 
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringLegacy()).Update(); 
} 
 
void TBlobManagerDb::EraseBlobToKeep(const TUnifiedBlobId& blobId) { 
    NIceDb::TNiceDb db(Database); 
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringLegacy()).Delete(); 
    db.Table<Schema::BlobsToKeep>().Key(blobId.ToStringNew()).Delete(); 
} 
 
void TBlobManagerDb::AddBlobToDelete(const TUnifiedBlobId& blobId) { 
    NIceDb::TNiceDb db(Database); 
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringLegacy()).Update(); 
} 
 
void TBlobManagerDb::EraseBlobToDelete(const TUnifiedBlobId& blobId) { 
    NIceDb::TNiceDb db(Database); 
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringLegacy()).Delete(); 
    db.Table<Schema::BlobsToDelete>().Key(blobId.ToStringNew()).Delete(); 
} 
 
void TBlobManagerDb::WriteSmallBlob(const TUnifiedBlobId& blobId, const TString& data) { 
    Y_VERIFY(blobId.IsSmallBlob()); 
    NIceDb::TNiceDb db(Database); 
    db.Table<Schema::SmallBlobs>().Key(blobId.ToStringNew()).Update( 
        NIceDb::TUpdate<Schema::SmallBlobs::Data>(data) 
    ); 
} 
 
void TBlobManagerDb::EraseSmallBlob(const TUnifiedBlobId& blobId) { 
    Y_VERIFY(blobId.IsSmallBlob()); 
    NIceDb::TNiceDb db(Database); 
    db.Table<Schema::SmallBlobs>().Key(blobId.ToStringLegacy()).Delete(); 
    db.Table<Schema::SmallBlobs>().Key(blobId.ToStringNew()).Delete(); 
} 
 
}
