#include "fetcher.h"
#include "manager.h"

namespace NKikimr::NMetadataInitializer {

std::vector<NMetadata::IOperationsManager::TPtr> TFetcher::DoGetManagers() const {
    return { std::make_shared<TManager>() };
}

}
