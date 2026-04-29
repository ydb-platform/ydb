#pragma once

#include <yt/yql/providers/yt/lib/access_provider/yt_access_provider.h>
#include <yt/yql/providers/yt/lib/access_provider/proto/access_provider.pb.h>

namespace NYql {

IYtAccessProvider::TPtr CreateYtAccessProvider(const ITvmClient::TPtr& tvmClient, const TYtAccessProviderConfig& config);

}; // namespace NYql
