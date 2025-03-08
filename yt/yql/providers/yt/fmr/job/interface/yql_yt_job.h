#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

class IFmrJob: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrJob>;

    virtual ~IFmrJob() = default;

    virtual std::variant<TError, TStatistics> Download(const TDownloadTaskParams& params, const TClusterConnection& clusterConnection = TClusterConnection()) = 0;

    virtual std::variant<TError, TStatistics> Upload(const TUploadTaskParams& params, const TClusterConnection& clusterConnection = TClusterConnection()) = 0;

    virtual std::variant<TError, TStatistics> Merge(const TMergeTaskParams& params, const TClusterConnection& clusterConnection = TClusterConnection()) = 0;
};

} // namespace NYql
