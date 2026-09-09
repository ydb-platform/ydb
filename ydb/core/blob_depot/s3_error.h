#pragma once

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>

#include <util/generic/strbuf.h>

namespace NKikimr::NBlobDepot {

    inline bool IsS3SlowDown(const Aws::S3::S3Error& error) {
        return error.GetErrorType() == Aws::S3::S3Errors::SLOW_DOWN
            || error.GetExceptionName() == "SlowDown"
            || error.GetExceptionName() == "TooManyRequests"
            || error.GetResponseCode() == Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS;
    }

    inline bool IsS3SlowDownCode(TStringBuf code) {
        return code == "SlowDown" || code == "TooManyRequests";
    }

} // namespace NKikimr::NBlobDepot
