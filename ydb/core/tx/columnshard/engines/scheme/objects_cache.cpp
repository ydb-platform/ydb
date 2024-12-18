#include "objects_cache.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

std::shared_ptr<const TIndexInfo> TSchemaObjectsCache::UpsertIndexInfo(const ui64 presetId, TIndexInfo&& indexInfo) {
    const TSchemaVersionId versionId(presetId, indexInfo.GetVersion());
    TGuard lock(SchemasMutex);
    auto* findSchema = SchemasByVersion.FindPtr(versionId);
    std::shared_ptr<const TIndexInfo> cachedSchema;
    if (findSchema) {
        cachedSchema = findSchema->lock();
    }
    if (!cachedSchema) {
        cachedSchema = std::make_shared<TIndexInfo>(std::move(indexInfo));
        SchemasByVersion[versionId] = cachedSchema;
    }
    return cachedSchema;
}

}   // namespace NKikimr::NOlap
