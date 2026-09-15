#pragma once

#include <yt/yql/providers/yt/fmr/file/upload/interface/yql_yt_file_upload_interface.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

struct TYtFileUploadServiceOptions {
    TString RemotePath;
    TString YtServerName;
    TMaybe<TString> YtToken;
    TDuration ExpirationInterval = TDuration::Hours(12);
    ui64 ThreadsNum = 3;
};

IFileUploadService::TPtr MakeYtFileUploadService(const TYtFileUploadServiceOptions& options);

} // namespace NYql::NFmr
