#include "aws.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/internal/AWSHttpResourceClient.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/Aws.h>
#include <contrib/libs/curl/include/curl/curl.h>

Aws::SDKOptions MakeDefaultOptions() {
    static Aws::SDKOptions options;
    options.httpOptions.initAndCleanupCurl = false;
    return options;
}

namespace NKikimr {

void InitAwsAPI() {
    curl_global_init(CURL_GLOBAL_ALL);
    Aws::InitAPI(MakeDefaultOptions());
    Aws::Internal::CleanupEC2MetadataClient(); // speeds up config construction
}

void ShutdownAwsAPI() {
    Aws::ShutdownAPI(MakeDefaultOptions());
    curl_global_cleanup();
}

} // NKikimr
