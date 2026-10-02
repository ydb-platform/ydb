#include "manager.h"

#include <ydb/core/tx/columnshard/engines/storage/granule/stages.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

std::shared_ptr<NKikimr::ITxReader> TManager::DoBuildLoader(
    const TVersionedIndex& /*versionedIndex*/, TGranuleMeta* /*granule*/, const std::shared_ptr<IBlobGroupSelector>& /*dsGroupSelector*/) {
    return nullptr;
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
