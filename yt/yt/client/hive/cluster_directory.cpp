#include "cluster_directory.h"

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

TError TClusterDirectoryUpdateResult::GetCumulativeError() const
{
    std::vector<TError> failedClusterErrors;
    for (auto&& [cluster, error] : ClusterToErrorMapping) {
        if (!error.IsOK()) {
            failedClusterErrors.push_back(error);
        }
    }

    if (failedClusterErrors.empty()) {
        return {};
    }

    return TError("Cluster directory update failed")
        << std::move(failedClusterErrors);
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NHiveClient
