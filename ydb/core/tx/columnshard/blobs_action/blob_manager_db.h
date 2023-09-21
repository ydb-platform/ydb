#pragma once
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/blob.h>

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NColumnShard {

// Garbage Collection generation and step
using TGenStep = std::tuple<ui32, ui32>;

class IBlobManagerDb {
public:
    virtual ~IBlobManagerDb() = default;

    virtual bool LoadLastGcBarrier(TGenStep& lastCollectedGenStep) = 0;
    virtual void SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) = 0;

    virtual bool LoadLists(std::vector<NOlap::TUnifiedBlobId>& blobsToKeep, std::vector<NOlap::TUnifiedBlobId>& blobsToDelete,
        const NOlap::IBlobGroupSelector* dsGroupSelector) = 0;
    virtual void AddBlobToKeep(const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual void EraseBlobToKeep(const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual void AddBlobToDelete(const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual void EraseBlobToDelete(const NOlap::TUnifiedBlobId& blobId) = 0;

    virtual bool LoadTierLists(const TString& storageId, std::deque<NOlap::TUnifiedBlobId>& blobsToDelete, std::deque<NOlap::TUnifiedBlobId>& draftBlobsToDelete) = 0;

    virtual void AddTierBlobToDelete(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual void RemoveTierBlobToDelete(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual void AddTierDraftBlobId(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) = 0;
    virtual void RemoveTierDraftBlobId(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) = 0;
};


class TBlobManagerDb : public IBlobManagerDb {
public:
    explicit TBlobManagerDb(NTable::TDatabase& db)
        : Database(db)
    {}

    bool LoadLastGcBarrier(TGenStep& lastCollectedGenStep) override;
    void SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) override;

    bool LoadLists(std::vector<NOlap::TUnifiedBlobId>& blobsToKeep, std::vector<NOlap::TUnifiedBlobId>& blobsToDelete,
        const NOlap::IBlobGroupSelector* dsGroupSelector) override;

    void AddBlobToKeep(const NOlap::TUnifiedBlobId& blobId) override;
    void EraseBlobToKeep(const NOlap::TUnifiedBlobId& blobId) override;
    void AddBlobToDelete(const NOlap::TUnifiedBlobId& blobId) override;
    void EraseBlobToDelete(const NOlap::TUnifiedBlobId& blobId) override;

    bool LoadTierLists(const TString& storageId, std::deque<NOlap::TUnifiedBlobId>& blobsToDelete, std::deque<NOlap::TUnifiedBlobId>& draftBlobsToDelete) override;

    void AddTierBlobToDelete(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) override;

    void RemoveTierBlobToDelete(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) override;

    void AddTierDraftBlobId(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) override;
    void RemoveTierDraftBlobId(const TString& storageId, const NOlap::TUnifiedBlobId& blobId) override;

private:
    NTable::TDatabase& Database;
};

}
