#include "read.h"

#include <ydb/core/tx/columnshard/blob_cache.h>

#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_BLOBS_BS

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {

void TReadingAction::DoStartReading(THashSet<TBlobRange>&& ranges) {
    NBlobCache::TReadBlobRangeOptions readOpts{ .CacheAfterRead = GetCacheAfterRead(), .IsBackgroud = GetIsBackgroundProcess(),
        .WithDeadline = false };
    std::vector<TBlobRange> rangesLocal(ranges.begin(), ranges.end());
    auto request = std::make_unique<NBlobCache::TEvBlobCache::TEvReadBlobRangeBatch>(std::move(rangesLocal), std::move(readOpts));
    for (const auto& range : request->BlobRanges) {
        auto context = GetReadStatsContext(range);
        if (context.Collector) {
            request->ReadStats.emplace(range, std::move(context));
        }
    }
    TActorContext::AsActorContext().Send(
        BlobCacheActorId, request.release());
    YDB_LOG_DEBUG("",
        {"blobIds", JoinSeq(",", ranges)},
        {"count", ranges.size()});
}

}   // namespace NKikimr::NOlap::NBlobOperations::NBlobStorage
