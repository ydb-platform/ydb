#include "fetcher.h"
#include "manager.h"

namespace NKikimr::NMetadata::NSecret {

std::vector<IClassBehaviour::TPtr> TSnapshotsFetcher::DoGetManagers() const {
    return {
        TSecret::GetBehaviour(),
        TAccess::GetBehaviour()
    };
}

}
