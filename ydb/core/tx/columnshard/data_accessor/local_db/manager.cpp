#include "collector.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/engines/storage/granule/stages.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

std::shared_ptr<NKikimr::ITxReader> TManager::DoBuildLoader(
    const TVersionedIndex& /*versionedIndex*/, TGranuleMeta* /*granule*/, const std::shared_ptr<IBlobGroupSelector>& /*dsGroupSelector*/) {
    return nullptr;
}

std::unique_ptr<IGranuleDataAccessor> TManager::DoBuildCollector(const ui64 pathId) {
    return std::make_unique<TCollector>(pathId, MemoryCacheSize, TabletActorId);
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
