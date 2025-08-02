#pragma once
#include "versioned_index.h"

#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>

namespace NKikimr::NOlap {

class TVersionedPresetSchemas {
private:
    THashMap<ui64, std::shared_ptr<TVersionedIndex>> PresetVersionedIndex;
    THashMap<ui64, std::shared_ptr<const TVersionedIndex>> PresetVersionedIndexCopy;

    const ui64 DefaultPresetId;
    std::shared_ptr<TSchemaObjectsCache> SchemaObjectsCache;

public:
    void RegisterPreset(const ui64 presetId) {
        AFL_VERIFY(PresetVersionedIndex.emplace(presetId, std::make_shared<TVersionedIndex>()).second)("preset_id", presetId);
    }

    const TVersionedIndex& GetDefaultVersionedIndex() const {
        return GetVersionedIndex(DefaultPresetId);
    }

    TVersionedIndex& MutableVersionedIndex(const ui64 presetId) {
        auto it = PresetVersionedIndex.find(presetId);
        AFL_VERIFY(it != PresetVersionedIndex.end());
        return *it->second;
    }

    TVersionedIndex& MutableDefaultVersionedIndex() {
        auto it = PresetVersionedIndex.find(DefaultPresetId);
        AFL_VERIFY(it != PresetVersionedIndex.end());
        return *it->second;
    }

    const TVersionedIndex& GetVersionedIndex(const ui64 presetId) const {
        auto it = PresetVersionedIndex.find(presetId);
        AFL_VERIFY(it != PresetVersionedIndex.end());
        return *it->second;
    }

    const std::shared_ptr<const TVersionedIndex>& GetVersionedIndexCopy(const ui64 presetId) {
        auto itOriginal = PresetVersionedIndex.find(presetId);
        AFL_VERIFY(itOriginal != PresetVersionedIndex.end());
        auto itCopy = PresetVersionedIndexCopy.find(presetId);
        if (itCopy == PresetVersionedIndexCopy.end()) {
            itCopy = PresetVersionedIndexCopy.emplace(presetId, itOriginal->second->DeepCopy()).first;
        } else if (!itCopy->second->IsEqualTo(*itOriginal->second)) {
            itCopy->second = itOriginal->second->DeepCopy();
        }
        return itCopy->second;
    }

    const std::shared_ptr<const TVersionedIndex>& GetDefaultVersionedIndexCopy() {
        return GetVersionedIndexCopy(DefaultPresetId);
    }

    TVersionedPresetSchemas(const ui64 defaultPresetId, const std::shared_ptr<IStoragesManager>& storagesManager,
        const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache);
};

}   // namespace NKikimr::NOlap
