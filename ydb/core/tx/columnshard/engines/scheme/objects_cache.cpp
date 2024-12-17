#include "objects_cache.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

std::shared_ptr<const TIndexInfo> TSchemaObjectsCache::GetIndexInfoCache(TIndexInfo&& indexInfo) {
    const ui64 schemaVersion = indexInfo.GetVersion();
    std::unique_lock lock(SchemasMutex);
    auto* findSchema = SchemasByVersion.FindPtr(schemaVersion);
    if (!findSchema || findSchema->expired()) {
        SchemasByVersion[schemaVersion] = std::make_shared<TIndexInfo>(std::move(indexInfo));
    }
    return findSchema->lock();
}

}   // namespace NKikimr::NOlap
