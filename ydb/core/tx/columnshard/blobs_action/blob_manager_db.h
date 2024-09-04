#pragma once
#include "abstract/blob_set.h"
#include "abstract/common.h"
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/util/gen_step.h>

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NOlap {

// Garbage Collection generation and step
using TGenStep = ::NKikimr::TGenStep;

class IBlobManagerDb {
public:
    virtual ~IBlobManagerDb() = default;

    [[nodiscard]] virtual bool LoadGCBarrierPreparation(TGenStep& genStep) = 0;
    virtual void SaveGCBarrierPreparation(const TGenStep& genStep) = 0;
    [[nodiscard]] virtual bool LoadLastGcBarrier(TGenStep& lastCollectedGenStep) = 0;
    virtual void SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) = 0;

    [[nodiscard]] virtual bool LoadLists(std::vector<TUnifiedBlobId>& blobsToKeep, TTabletsByBlob& blobsToDelete,
        const IBlobGroupSelector* dsGroupSelector, const TTabletId selfTabletId) = 0;
    virtual void AddBlobToKeep(const TUnifiedBlobId& blobId) = 0;
    virtual void EraseBlobToKeep(const TUnifiedBlobId& blobId) = 0;

    virtual void AddBlobToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;
    virtual void EraseBlobToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;

    [[nodiscard]] virtual bool LoadTierLists(const TString& storageId, TTabletsByBlob& blobsToDelete, std::deque<TUnifiedBlobId>& draftBlobsToDelete, const TTabletId selfTabletId) = 0;

    virtual void AddTierBlobToDelete(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;
    virtual void RemoveTierBlobToDelete(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;
    virtual void AddTierDraftBlobId(const TString& storageId, const TUnifiedBlobId& blobId) = 0;
    virtual void RemoveTierDraftBlobId(const TString& storageId, const TUnifiedBlobId& blobId) = 0;

    virtual void AddBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;
    virtual void RemoveBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;

    virtual void AddBorrowedBlob(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) = 0;
    virtual void RemoveBorrowedBlob(const TString& storageId, const TUnifiedBlobId& blobId) = 0;
};


class TBlobManagerDb : public IBlobManagerDb {
public:
    explicit TBlobManagerDb(NTable::TDatabase& db)
        : Database(db)
    {}

    [[nodiscard]] bool LoadLastGcBarrier(TGenStep& lastCollectedGenStep) override;
    void SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) override;

    [[nodiscard]] bool LoadGCBarrierPreparation(TGenStep& genStep) override;
    void SaveGCBarrierPreparation(const TGenStep& genStep) override;
    static void SaveGCBarrierPreparation(NTable::TDatabase& database, const TGenStep& step);

    [[nodiscard]] bool LoadLists(std::vector<TUnifiedBlobId>& blobsToKeep, TTabletsByBlob& blobsToDelete,
        const IBlobGroupSelector* dsGroupSelector, const TTabletId selfTabletId) override;

    void AddBlobToKeep(const TUnifiedBlobId& blobId) override;
    void EraseBlobToKeep(const TUnifiedBlobId& blobId) override;
    void AddBlobToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) override;
    void EraseBlobToDelete(const TUnifiedBlobId& blobId, const TTabletId tabletId) override;

    [[nodiscard]] bool LoadTierLists(const TString& storageId, TTabletsByBlob& blobsToDelete, std::deque<TUnifiedBlobId>& draftBlobsToDelete, const TTabletId selfTabletId) override;

    void AddTierBlobToDelete(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) override;
    void RemoveTierBlobToDelete(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) override;

    void AddTierDraftBlobId(const TString& storageId, const TUnifiedBlobId& blobId) override;
    void RemoveTierDraftBlobId(const TString& storageId, const TUnifiedBlobId& blobId) override;

    void AddBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) override;
    void RemoveBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) override;

    void AddBorrowedBlob(const TString& storageId, const TUnifiedBlobId& blobId, const TTabletId tabletId) override;
    void RemoveBorrowedBlob(const TString& storageId, const TUnifiedBlobId& blobId) override;

    NTable::TDatabase& GetDatabase() {
        return Database;
    }

private:
    NTable::TDatabase& Database;
};

}
