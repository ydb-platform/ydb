#include "manager.h"

#include <ydb/core/tx/columnshard/engines/storage/granule/stages.h>
#include <ydb/core/tx/columnshard/tx_reader/composite.h>

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

std::shared_ptr<NKikimr::ITxReader> TManager::DoBuildLoader(
    const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
    auto result = std::make_shared<TTxCompositeReader>("granule");
    auto portionsLoadContext = std::make_shared<NLoading::TPortionsLoadContext>();
    result->AddChildren(
        std::make_shared<NLoading::TGranuleStartAccessorsLoading>("start", &versionedIndex, granule, dsGroupSelector, portionsLoadContext));
    result->AddChildren(
        std::make_shared<NLoading::TGranuleColumnsReader>("columns", &versionedIndex, granule, dsGroupSelector, portionsLoadContext));
    result->AddChildren(
        std::make_shared<NLoading::TGranuleIndexesReader>("indexes", &versionedIndex, granule, dsGroupSelector, portionsLoadContext));
    result->AddChildren(
        std::make_shared<NLoading::TGranuleFinishAccessorsLoading>("finish", &versionedIndex, granule, dsGroupSelector, portionsLoadContext));
    return result;
}

}   // namespace NKikimr::NOlap::NDataAccessorControl::NInMem
