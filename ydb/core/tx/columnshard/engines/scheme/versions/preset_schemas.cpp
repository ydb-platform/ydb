#include "preset_schemas.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/schema.h>

namespace NKikimr::NOlap {

TVersionedPresetSchemas::TVersionedPresetSchemas(const ui64 defaultPresetId, const std::shared_ptr<IStoragesManager>& storagesManager,
    const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache)
    : DefaultPresetId(defaultPresetId)
    , SchemaObjectsCache(schemaObjectsCache)
{
    YDB_LOG_VERIFY(storagesManager);
    YDB_LOG_VERIFY(SchemaObjectsCache);
    RegisterPreset(defaultPresetId);
    for (auto&& key : NReader::NSimple::NSysView::NAbstract::ISchemaAdapter::TFactory::GetRegisteredKeys()) {
        if (get<1>(key).StartsWith("store_")) {
            continue;
        }
        auto obj = NReader::NSimple::NSysView::NAbstract::ISchemaAdapter::TFactory::MakeHolder(key);
        YDB_LOG_VERIFY(!!obj);
        RegisterPreset(obj->GetPresetId());
        MutableVersionedIndex(obj->GetPresetId()).AddIndex(TSnapshot::Zero(),
            SchemaObjectsCache->GetOrInsertIndexInfo(obj->GetIndexInfo(storagesManager, SchemaObjectsCache)));
    }
}

}   // namespace NKikimr::NOlap
