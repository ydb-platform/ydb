#include "preset_schemas.h"

#include <ydb/core/tx/columnshard/engines/reader/sys_view/chunks/chunks.h>

namespace NKikimr::NOlap {

TVersionedPresetSchemas::TVersionedPresetSchemas(const ui64 defaultPresetId, const std::shared_ptr<IStoragesManager>& storagesManager,
    const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache)
    : DefaultPresetId(defaultPresetId)
    , StoragesManager(storagesManager)
    , SchemaObjectsCache(schemaObjectsCache) {
    AFL_VERIFY(StoragesManager);
    AFL_VERIFY(SchemaObjectsCache);
    RegisterPreset(defaultPresetId);
    using TSchemaAdapter = NReader::NSysView::NChunks::TSchemaAdapter<NKikimr::NSysView::Schema::PrimaryIndexStats>;
    RegisterPreset(TSchemaAdapter::GetPresetId());
    MutableVersionedIndex(TSchemaAdapter::GetPresetId())
        .AddIndex(TSnapshot::Zero(), SchemaObjectsCache->UpsertIndexInfo(TSchemaAdapter::GetIndexInfo(StoragesManager, SchemaObjectsCache)));
}

}   // namespace NKikimr::NOlap
