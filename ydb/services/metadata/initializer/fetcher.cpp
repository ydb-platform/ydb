#include "fetcher.h"

namespace NKikimr::NMetadata::NInitializer {

std::vector<IClassBehaviour::TPtr> TFetcher::DoGetManagers() const {
    return { TDBInitialization::GetBehaviour() };
}

}
