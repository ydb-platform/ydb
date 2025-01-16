#include "objects_cache.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

TSchemaObjectsCache::TSchemasCache::TEntryGuard TSchemaObjectsCache::UpsertIndexInfo(const ui64 presetId, TIndexInfo&& indexInfo) {
    const TSchemaVersionId versionId(presetId, indexInfo.GetVersion());
    return SchemasByVersion.Upsert(versionId, std::move(indexInfo));
}

}   // namespace NKikimr::NOlap
