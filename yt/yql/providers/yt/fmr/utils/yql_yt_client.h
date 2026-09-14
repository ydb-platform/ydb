#pragma once

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

void NormalizeRichPath(NYT::TRichYPath& richPath);

NYT::IClientPtr CreateClient(const TClusterConnection& clusterConnection);

} // namespace NYql::NFmr
