#pragma once
#include "defs.h"

#include "blob_manager.h"

namespace NKikimr::NTable {
class TDatabase;
}

namespace NKikimr::NColumnShard {

class IBlobManagerDb {
public:
    virtual ~IBlobManagerDb() = default;

    virtual bool LoadLastGcBarrier(TGenStep& lastCollectedGenStep) = 0;
    virtual void SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) = 0;

    virtual bool LoadLists(std::vector<TUnifiedBlobId>& blobsToKeep, std::vector<TUnifiedBlobId>& blobsToDelete,
        const NOlap::IBlobGroupSelector* dsGroupSelector) = 0;
    virtual void AddBlobToKeep(const TUnifiedBlobId& blobId) = 0;
    virtual void EraseBlobToKeep(const TUnifiedBlobId& blobId) = 0;
    virtual void AddBlobToDelete(const TUnifiedBlobId& blobId) = 0;
    virtual void EraseBlobToDelete(const TUnifiedBlobId& blobId) = 0;
    virtual void WriteSmallBlob(const TUnifiedBlobId& blobId, const TString& data) = 0;
    virtual void EraseSmallBlob(const TUnifiedBlobId& blobId) = 0;

    virtual bool LoadEvicted(THashMap<TEvictedBlob, TString>& evicted,
                             THashMap<TEvictedBlob, TString>& dropped,
                             const NOlap::IBlobGroupSelector& dsGroupSelector) = 0;
    virtual void UpdateEvictBlob(const TEvictedBlob& evict, const TString& meta) = 0;
    virtual void DropEvictBlob(const TEvictedBlob& evict) = 0;
    virtual void EraseEvictBlob(const TEvictedBlob& evict) = 0;
};


class TBlobManagerDb : public IBlobManagerDb {
public:
    explicit TBlobManagerDb(NTable::TDatabase& db)
        : Database(db)
    {}

    bool LoadLastGcBarrier(TGenStep& lastCollectedGenStep) override;
    void SaveLastGcBarrier(const TGenStep& lastCollectedGenStep) override;

    bool LoadLists(std::vector<TUnifiedBlobId>& blobsToKeep, std::vector<TUnifiedBlobId>& blobsToDelete,
        const NOlap::IBlobGroupSelector* dsGroupSelector) override;
    void AddBlobToKeep(const TUnifiedBlobId& blobId) override;
    void EraseBlobToKeep(const TUnifiedBlobId& blobId) override;
    void AddBlobToDelete(const TUnifiedBlobId& blobId) override;
    void EraseBlobToDelete(const TUnifiedBlobId& blobId) override;
    void WriteSmallBlob(const TUnifiedBlobId& blobId, const TString& data) override;
    void EraseSmallBlob(const TUnifiedBlobId& blobId) override;

    virtual bool LoadEvicted(THashMap<TEvictedBlob, TString>& evicted,
                             THashMap<TEvictedBlob, TString>& dropped,
                             const NOlap::IBlobGroupSelector& dsGroupSelector) override;
    void UpdateEvictBlob(const TEvictedBlob& evict, const TString& meta) override;
    void DropEvictBlob(const TEvictedBlob& evict) override;
    void EraseEvictBlob(const TEvictedBlob& evict) override;

private:
    NTable::TDatabase& Database;
};

}
