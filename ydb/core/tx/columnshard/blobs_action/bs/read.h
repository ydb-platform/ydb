#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/read.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

class TReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    const TActorId BlobCacheActorId;
protected:
    virtual void DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& ranges) override {
        for (auto&& i : ranges) {
            NBlobCache::TReadBlobRangeOptions readOpts{.CacheAfterRead = true, .IsBackgroud = GetIsBackgroundProcess(), .WithDeadline = false};
            std::vector<TBlobRange> rangesLocal(i.second.begin(), i.second.end());
            TActorContext::AsActorContext().Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(rangesLocal), std::move(readOpts)));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("blob_id", i.first)("count", i.second.size());
        }
    }
public:

    TReadingAction(const TString& storageId, const TActorId& blobCacheActorId)
        : TBase(storageId)
        , BlobCacheActorId(blobCacheActorId)
    {

    }
};

}
