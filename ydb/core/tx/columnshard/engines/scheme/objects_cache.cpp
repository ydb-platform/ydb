#include "objects_cache.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

TSchemaObjectsCache::TSchemasCache::TEntryGuard TSchemaObjectsCache::GetOrInsertIndexInfo(TIndexInfo&& indexInfo) {
    const TSchemaVersionId versionId(indexInfo.GetPresetId(), indexInfo.GetVersion());
    return SchemasByVersion.GetOrInsert(versionId, std::move(indexInfo));
}

std::optional<TSchemaObjectsCache::TSchemasCache::TEntryGuard> TSchemaObjectsCache::FindIndexInfo(const ui64 presetId, const ui64 version) {
    return SchemasByVersion.Find(TSchemaVersionId(presetId, version));
}

}   // namespace NKikimr::NOlap
