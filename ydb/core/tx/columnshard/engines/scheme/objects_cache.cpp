#include "objects_cache.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

std::shared_ptr<const TIndexInfo> TSchemaObjectsCache::GetIndexInfoCache(TIndexInfo&& indexInfo) {
    const ui64 schemaVersion = indexInfo.GetVersion();
    std::unique_lock lock(SchemasMutex);
    auto* findSchema = SchemasByVersion.FindPtr(schemaVersion);
    std::shared_ptr<const TIndexInfo> cachedSchema;
    if (findSchema) {
        cachedSchema = findSchema->lock();
    }
    if (!cachedSchema) {
        cachedSchema = std::make_shared<TIndexInfo>(std::move(indexInfo));
        SchemasByVersion[schemaVersion] = cachedSchema;
    }
    return cachedSchema;
}

}   // namespace NKikimr::NOlap
