#include "client.h"
#include "transaction.h"
#include "private.h"

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ApiLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<std::optional<std::string>> TClusterAwareClientBase::GetClusterName(bool fetchIfNull)
{
    auto clusterName = ClusterName_.Load();
    if (clusterName) {
        return MakeFuture(clusterName);
    }

    clusterName = GetConnection()->GetClusterName();
    if (clusterName) {
        ClusterName_.Store(clusterName);
        return MakeFuture(clusterName);
    }

    if (!fetchIfNull) {
        return MakeFuture<std::optional<std::string>>({});
    }

    return FetchClusterNameFromMasterCache().Apply(
        BIND([this, this_ = MakeStrong(this)] (const std::optional<std::string>& clusterName) -> std::optional<std::string> {
            ClusterName_.Store(clusterName);
            return clusterName;
        }));
}

TFuture<std::optional<std::string>> TClusterAwareClientBase::FetchClusterNameFromMasterCache()
{
    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;

    return GetNode(ClusterNamePath, options).Apply(
        BIND([] (const TErrorOr<TYsonString>& clusterNameYsonOrError) -> std::optional<std::string> {
            if (!clusterNameYsonOrError.IsOK()) {
                YT_LOG_WARNING(clusterNameYsonOrError, "Could not fetch cluster name from from master cache (Path: %v)",
                    ClusterNamePath);
                return {};
            }

            return ConvertTo<std::string>(clusterNameYsonOrError.Value());
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

