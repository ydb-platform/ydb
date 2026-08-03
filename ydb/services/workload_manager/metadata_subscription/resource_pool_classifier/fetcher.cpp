#include "fetcher.h"


namespace NKikimr::NWorkloadManager {

std::vector<NMetadata::IClassBehaviour::TPtr> TResourcePoolClassifierSnapshotsFetcher::DoGetManagers() const {
    return {TResourcePoolClassifierConfig::GetBehaviour()};
}

}  // namespace NKikimr::NWorkloadManager
