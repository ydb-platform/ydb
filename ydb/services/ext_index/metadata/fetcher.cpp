#include "fetcher.h"
#include "behaviour.h"

namespace NKikimr::NMetadata::NCSIndex {

std::vector<IClassBehaviour::TPtr> TFetcher::DoGetManagers() const {
    return {
        TBehaviour::GetInstance()
    };
}

}
