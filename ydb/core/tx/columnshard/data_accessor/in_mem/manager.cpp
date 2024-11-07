#include "manager.h"

namespace NKikimr::NOlap::NDataAccessorControl::NInMem {

std::shared_ptr<NKikimr::ITxReader> TManager::DoBuildGranuleLoader(
    const TVersionedIndex& versionedIndex, TGranuleMeta* granule, const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) {
    auto result = std::make_shared<TTxCompositeReader>("granule");
    auto portionsLoadContext = std::make_shared<NLoading::TPortionsLoadContext>();
    result->AddChildren(std::make_shared<NLoading::TGranulePortionsReader>("portions", &vIndex, granule, dsGroupSelector, portionsLoadContext));
    result->AddChildren(std::make_shared<NLoading::TGranuleColumnsReader>("columns", &vIndex, granule, dsGroupSelector, portionsLoadContext));
    result->AddChildren(std::make_shared<NLoading::TGranuleIndexesReader>("indexes", &vIndex, granule, dsGroupSelector, portionsLoadContext));
    result->AddChildren(std::make_shared<NLoading::TGranuleFinishLoading>("finish", &vIndex, granule, dsGroupSelector, portionsLoadContext));
    return result;
}

std::unique_ptr<NKikimr::NOlap::IGranuleDataAccessor> TManager::DoBuildCollector() {
    return std::make_unique<TCollector>();
}

}