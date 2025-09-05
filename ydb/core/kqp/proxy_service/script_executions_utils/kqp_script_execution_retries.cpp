#include "kqp_script_execution_retries.h"

#include <util/string/builder.h>

namespace NKikimr::NKqp {

TRetryPolicyItem::TRetryPolicyItem(ui64 retryCount, ui64 retryLimit, TDuration retryPeriod, TDuration backoffPeriod)
    : RetryCount(retryCount)
    , RetryLimit(retryLimit)
    , RetryPeriod(retryPeriod)
    , BackoffPeriod(backoffPeriod)
{}

TRetryPolicyItem TRetryPolicyItem::FromProto(Ydb::StatusIds::StatusCode status, const NKikimrKqp::TScriptExecutionRetryState& retryState) {
    for (const auto& mapping : retryState.GetRetryPolicyMapping()) {
        for (const auto mappingStatus : mapping.GetStatusCode()) {
            if (mappingStatus != status) {
                continue;
            }

            switch (mapping.GetPolicyCase()) {
                case NKikimrKqp::TScriptExecutionRetryState::TMapping::kBackoffPolicy: {
                    const auto& backoffPolicy = mapping.GetBackoffPolicy();
                    return TRetryPolicyItem(
                        backoffPolicy.GetRetryRateLimit(),
                        backoffPolicy.GetRetryCountLimit(),
                        TDuration::MilliSeconds(backoffPolicy.GetRetryPeriodMs()),
                        TDuration::MilliSeconds(backoffPolicy.GetBackoffPeriodMs())
                    );
                }
                default: {
                    throw std::runtime_error(TStringBuilder() << "Unsupported retry policy: " << mapping.DebugString());
                }
            }
        }
    }

    return TRetryPolicyItem();
}

TRetryLimiter::TRetryLimiter(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate)
    : RetryCount(retryCount)
    , RetryCounterUpdatedAt(retryCounterUpdatedAt)
    , RetryRate(retryRate)
{}

void TRetryLimiter::Assign(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate) {
    RetryCount = retryCount;
    RetryCounterUpdatedAt = retryCounterUpdatedAt;
    RetryRate = retryRate;
}

bool TRetryLimiter::UpdateOnRetry(TInstant lastSeenAt, const TRetryPolicyItem& policy, TInstant now) {
    const auto lastPeriod = lastSeenAt - RetryCounterUpdatedAt;
    if (lastPeriod >= policy.RetryPeriod) {
        RetryRate = 0.0;
    } else {
        RetryRate += 1.0;
        const auto rate = lastPeriod / policy.RetryPeriod * policy.RetryCount;
        if (RetryRate > rate) {
            RetryRate -= rate;
        } else {
            RetryRate = 0.0;
        }
    }

    bool shouldRetry = true;
    if (RetryRate >= policy.RetryCount) {
        shouldRetry = false;
        LastError = TStringBuilder() << "failure rate " << RetryRate << " exceeds limit of "  << policy.RetryCount;
    } else if (policy.RetryLimit && RetryCount >= policy.RetryLimit) {
        shouldRetry = false;
        LastError = TStringBuilder() << "retry count reached limit of " << policy.RetryLimit;
    }

    if (shouldRetry) {
        RetryCount++;
        RetryCounterUpdatedAt = now;
        Backoff = policy.BackoffPeriod * (RetryRate + 1);
    }

    return shouldRetry;
}

}  // namespace NKikimr::NKqp
