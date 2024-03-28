#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <library/cpp/retry/retry_policy.h>

#include <util/generic/ptr.h>

namespace NYdb::NTopic {


//! Retry policy.
//! Calculates delay before next retry.
//! Has several default implementations:
//! - exponential backoff policy;
//! - retries with fixed interval;
//! - no retries.
struct IRetryPolicy: ::IRetryPolicy<EStatus> {
    //!
    //! Default implementations.
    //!
    static TPtr GetDefaultPolicy(); // Exponential backoff with infinite retry attempts.
    static TPtr GetNoRetryPolicy(); // Denies all kind of retries.

    //! Randomized exponential backoff policy.
    static TPtr GetExponentialBackoffPolicy(
        TDuration minDelay = TDuration::MilliSeconds(10),
        // Delay for statuses that require waiting before retry (such as OVERLOADED).
        TDuration minLongRetryDelay = TDuration::MilliSeconds(200), TDuration maxDelay = TDuration::Seconds(30),
        size_t maxRetries = std::numeric_limits<size_t>::max(), TDuration maxTime = TDuration::Max(),
        double scaleFactor = 2.0, std::function<ERetryErrorClass(EStatus)> customRetryClassFunction = {});

    //! Randomized fixed interval policy.
    static TPtr GetFixedIntervalPolicy(TDuration delay = TDuration::MilliSeconds(100),
                                       // Delay for statuses that require waiting before retry (such as OVERLOADED).
                                       TDuration longRetryDelay = TDuration::MilliSeconds(300),
                                       size_t maxRetries = std::numeric_limits<size_t>::max(),
                                       TDuration maxTime = TDuration::Max(),
                                       std::function<ERetryErrorClass(EStatus)> customRetryClassFunction = {});
};

}  // namespace NYdb::NTopic
