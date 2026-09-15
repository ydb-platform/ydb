#pragma once

#include <yt/yql/providers/yt/fmr/file/metadata/interface/yql_yt_file_metadata_interface.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct TYtFileMetadataServiceOptions {
    TString RemotePath;
    TString YtServerName;
    TMaybe<TString> YtToken;
    ui64 MaxExistRequestBatchSize = 10; // TODO - add to settings file.
    TDuration TimeToSleepBetweenExistRequests = TDuration::Seconds(1);
    ui64 ThreadsNum = 3;
};

IFileMetadataService::TPtr MakeYtFileMetadataService(const TYtFileMetadataServiceOptions& options);

} // namespace NYql::NFmr
