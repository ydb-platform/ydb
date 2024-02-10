#include "read.h"
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TReadingAction::DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& ranges) {
    for (auto&& i : ranges) {
        NBlobCache::TReadBlobRangeOptions readOpts{.CacheAfterRead = true, .IsBackgroud = GetIsBackgroundProcess(), .WithDeadline = false};
        std::vector<TBlobRange> rangesLocal(i.second.begin(), i.second.end());
        TActorContext::AsActorContext().Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(rangesLocal), std::move(readOpts)));
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("blob_id", i.first)("count", i.second.size());
    }
}

}
