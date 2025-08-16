#include "objects_cache.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

TSchemaObjectsCache::TSchemasCache::TEntryGuard TSchemaObjectsCache::UpsertIndexInfo(TIndexInfo&& indexInfo) {
    const TSchemaVersionId versionId(indexInfo.GetPresetId(), indexInfo.GetVersion());
    return SchemasByVersion.Upsert(versionId, std::move(indexInfo));
}

}   // namespace NKikimr::NOlap
