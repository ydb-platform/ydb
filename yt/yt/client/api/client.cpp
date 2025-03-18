#include "client.h"
#include "transaction.h"
#include "private.h"

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ApiLogger;

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> TClusterAwareClientBase::GetClusterName(bool fetchIfNull)
{
    {
        auto guard = ReaderGuard(SpinLock_);
        if (ClusterName_) {
            return ClusterName_;
        }
    }

    auto clusterName = GetConnection()->GetClusterName();
    if (fetchIfNull && !clusterName) {
        clusterName = FetchClusterNameFromMasterCache();
    }

    if (!clusterName) {
        return {};
    }

    auto guard = WriterGuard(SpinLock_);
    if (!ClusterName_) {
        ClusterName_ = clusterName;
    }

    return ClusterName_;
}

std::optional<std::string> TClusterAwareClientBase::FetchClusterNameFromMasterCache()
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::MasterCache;
    auto clusterNameYsonOrError = WaitFor(GetNode(ClusterNamePath, options));
    if (!clusterNameYsonOrError.IsOK()) {
        YT_LOG_WARNING(clusterNameYsonOrError, "Could not fetch cluster name from from master cache (Path: %v)",
            ClusterNamePath);
        return {};
    }
    return ConvertTo<std::string>(clusterNameYsonOrError.Value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

