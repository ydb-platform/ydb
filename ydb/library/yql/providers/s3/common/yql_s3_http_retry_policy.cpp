#include "yql_s3_http_retry_policy.h"

namespace NYql {

IHTTPGateway::TRetryPolicy::TPtr GetFqS3HttpRetryPolicy() {
    return GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{
        .MaxTime = TDuration::MilliSeconds(1000),
        .RetriedCurlCodes = FqRetriedCurlCodes(),
    });
}

}
