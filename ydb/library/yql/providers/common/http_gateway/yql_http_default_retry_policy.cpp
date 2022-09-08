#include "yql_http_default_retry_policy.h"

namespace NYql {

IRetryPolicy<long>::TPtr GetHTTPDefaultRetryPolicy() {
    return IRetryPolicy<long>::GetExponentialBackoffPolicy([](long httpCode) {
        switch (httpCode) {
            case 0: return ERetryErrorClass::ShortRetry;
            case 503:return ERetryErrorClass::LongRetry;
            default: return ERetryErrorClass::NoRetry;
        }
    });
}

}
