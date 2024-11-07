#include "collector.h"
#include "manager.h"

#include <ydb/core/tx/columnshard/engines/storage/granule/stages.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

std::shared_ptr<NKikimr::ITxReader> TManager::DoBuildGranuleLoader(
    const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
    return std::make_shared<NLoading::TGranuleOnlyPortionsReader>("granule", &versionedIndex, granule, dsGroupSelector);
}

std::unique_ptr<IGranuleDataAccessor> TManager::DoBuildCollector(const ui64 pathId) {
    return std::make_unique<TCollector>(pathId, TabletActorId);
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
