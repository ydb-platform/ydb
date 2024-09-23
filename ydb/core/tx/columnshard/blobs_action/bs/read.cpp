#include "read.h"
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <util/string/join.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TReadingAction::DoStartReading(THashSet<TBlobRange>&& ranges) {
    NBlobCache::TReadBlobRangeOptions readOpts{.CacheAfterRead = true, .IsBackgroud = GetIsBackgroundProcess(), .WithDeadline = false};
    std::vector<TBlobRange> rangesLocal(ranges.begin(), ranges.end());
    TActorContext::AsActorContext().Send(BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(rangesLocal), std::move(readOpts)));
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_BLOBS_BS)("blob_ids", JoinSeq(",", ranges))("count", ranges.size());
}

}
