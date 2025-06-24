#pragma once

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

NYT::IClientPtr CreateClient(const TClusterConnection& clusterConnection);

} // namespace NYql::NFmr
