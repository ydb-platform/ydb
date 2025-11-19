#if defined(KIKIMR_DISABLE_S3_WRAPPER)
#error "s3 wrapper is disabled"
#endif

#include "retry_policy.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>

namespace NKikimr::NWrappers {

bool ShouldRetry(const Aws::S3::S3Error& error) {
    if (error.ShouldRetry()) {
        return true;
    }

    const auto& exceptionName = error.GetExceptionName();
    if ("TooManyRequests" == exceptionName) {
        return true;
    } else if ("OperationAborted" == exceptionName) {
        return true;
    }

    return false;
}

}
