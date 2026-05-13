#include "read.h"

#include <ydb/core/tx/columnshard/blob_cache.h>

#include <util/string/join.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_BS

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TReadingAction::DoStartReading(THashSet<TBlobRange>&& ranges) {
    NBlobCache::TReadBlobRangeOptions readOpts{ .CacheAfterRead = true, .IsBackgroud = GetIsBackgroundProcess(), .WithDeadline = false };
    std::vector<TBlobRange> rangesLocal(ranges.begin(), ranges.end());
    TActorContext::AsActorContext().Send(
        BlobCacheActorId, new NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch(std::move(rangesLocal), std::move(readOpts)));
    YDB_LOG_DEBUG("",
        {"blob_ids", JoinSeq(",", ranges)},
        {"count", ranges.size()});
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
