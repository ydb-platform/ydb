#pragma once

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/retry/retry_policy.h>

namespace NKikimr::NOlap::NBlobOperations::NRead {

inline IRetryPolicy<>::TPtr MakeReadRetryPolicy() {
    if (HasAppData()) {
        const auto& rp = AppDataVerified().ColumnShardConfig.GetReadRetryPolicy();
        if (rp.GetMaxRetries() == 0) {
            return IRetryPolicy<>::GetNoRetryPolicy();
        }
        return IRetryPolicy<>::GetExponentialBackoffPolicy([]() {
            return ERetryErrorClass::ShortRetry;
        }, TDuration::MilliSeconds(rp.GetInitialRetryDelayMs()), TDuration::MilliSeconds(rp.GetInitialRetryDelayMs()),
            TDuration::MilliSeconds(rp.GetMaxRetryDelayMs()), rp.GetMaxRetries());
    }
    return IRetryPolicy<>::GetExponentialBackoffPolicy([]() {
        return ERetryErrorClass::ShortRetry;
    }, TDuration::MilliSeconds(100), TDuration::MilliSeconds(100), TDuration::Seconds(5), 10);
}

}   // namespace NKikimr::NOlap::NBlobOperations::NRead
