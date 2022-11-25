#include "fetcher.h"
#include "manager.h"

namespace NKikimr::NMetadata::NSecret {

std::vector<IOperationsManager::TPtr> TSnapshotsFetcher::DoGetManagers() const {
    return {
        std::make_shared<TSecretManager>(),
        std::make_shared<TAccessManager>(),
    };
}

}
