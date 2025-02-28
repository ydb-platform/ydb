#pragma once

#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

class IFmrJob: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrJob>;

    virtual ~IFmrJob() = default;

    virtual TMaybe<TString> Download(const TDownloadTaskParams& params, const TClusterConnection& clusterConnection = TClusterConnection()) = 0;

    virtual TMaybe<TString> Upload(const TUploadTaskParams& params, const TClusterConnection& clusterConnection = TClusterConnection()) = 0;

    virtual TMaybe<TString> Merge(const TMergeTaskParams& params, const TClusterConnection& clusterConnection = TClusterConnection()) = 0;
};

} // namespace NYql
