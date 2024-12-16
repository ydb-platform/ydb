#include "fetcher.h"


namespace NKikimr::NKqp {

std::vector<NMetadata::IClassBehaviour::TPtr> TResourcePoolClassifierSnapshotsFetcher::DoGetManagers() const {
    return {TResourcePoolClassifierConfig::GetBehaviour()};
}

}  // namespace NKikimr::NKqp
