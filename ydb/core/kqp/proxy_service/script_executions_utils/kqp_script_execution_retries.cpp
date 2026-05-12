#include "kqp_script_execution_retries.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <google/protobuf/timestamp.pb.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/random/random.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp {

namespace {

class TLinearBackoffPolicy final : public TRetryPolicyItem::IPolicy {
    using TResult = TRetryPolicyItem::TRetryResult;

public:
    TLinearBackoffPolicy() = default;

    explicit TLinearBackoffPolicy(const NKikimrKqp::TScriptExecutionRetryState::TBackoffPolicy& config)
        : RetryPeriod(TDuration::MilliSeconds(config.GetRetryPeriodMs()))
        , BackoffPeriod(TDuration::MilliSeconds(config.GetBackoffPeriodMs()))
        , RetryRateLimit(config.GetRetryRateLimit())
        , RetryCountLimit(config.GetRetryCountLimit())
    {}

    TLinearBackoffPolicy(ui64 retryCount, ui64 retryLimit, TDuration retryPeriod, TDuration backoffPeriod)
        : RetryPeriod(retryPeriod)
        , BackoffPeriod(backoffPeriod)
        , RetryRateLimit(retryCount)
        , RetryCountLimit(retryLimit)
    {}

    TResult Update(TInstant startedAt, TInstant lastSeenAt, TInstant now, TRetryPolicyState& state) const final {
        Y_UNUSED(startedAt);

        const auto lastPeriod = lastSeenAt - state.RetryCounterUpdatedAt;
        if (lastPeriod >= RetryPeriod) {
            state.RetryRate = 0.0;
        } else {
            state.RetryRate += 1.0;
            const auto rate = lastPeriod / RetryPeriod * RetryRateLimit;
            if (state.RetryRate > rate) {
                state.RetryRate -= rate;
            } else {
                state.RetryRate = 0.0;
            }
        }

        state.RetryCounterUpdatedAt = now;

        TResult result;
        if (state.RetryRate >= RetryRateLimit) {
            result.LastError = TStringBuilder() << "failure rate " << state.RetryRate << " exceeds limit of " << RetryRateLimit;
        } else if (RetryCountLimit && state.RetryCount >= RetryCountLimit) {
            result.LastError = TStringBuilder() << "retry count reached limit of " << RetryCountLimit;
        } else {
            state.RetryCount++;
            result.Retry = true;
            result.Backoff = BackoffPeriod * (state.RetryRate + 1);
        }

        return result;
    }

    bool IsInitialized() const final {
        return RetryRateLimit > 0;
    }

private:
    const TDuration RetryPeriod = TDuration::Seconds(1);
    const TDuration BackoffPeriod;
    const ui64 RetryRateLimit = 0;
    const ui64 RetryCountLimit = 0;
};

class TExponentialBackoffPolicy final : public TRetryPolicyItem::IPolicy {
    using TResult = TRetryPolicyItem::TRetryResult;

public:
    explicit TExponentialBackoffPolicy(const NKikimrKqp::TScriptExecutionRetryState::TExponentialDelayPolicy& config)
        : BackoffMultiplier(config.GetBackoffMultiplier())
        , InitialBackoff(NProtoInterop::CastFromProto(config.GetInitialBackoff()))
        , MaxBackoff(NProtoInterop::CastFromProto(config.GetMaxBackoff()))
        , ResetBackoffThreshold(NProtoInterop::CastFromProto(config.GetResetBackoffThreshold()))
        , QueryUptimeThreshold(NProtoInterop::CastFromProto(config.GetQueryUptimeThreshold()))
        , JitterFactor(config.GetJitterFactor())
    {
        Y_VALIDATE(BackoffMultiplier > 1.0, "Backoff multiplier should be greater than 1.0");
        Y_VALIDATE(InitialBackoff > TDuration::Zero(), "Initial backoff should be non zero");
        Y_VALIDATE(JitterFactor >= 0.0 && JitterFactor <= 1.0, "Jitter factor should be in range [0.0, 1.0]");

        if (QueryUptimeThreshold && ResetBackoffThreshold) {
            Y_VALIDATE(QueryUptimeThreshold <= ResetBackoffThreshold, "Immediate retry duration should be not greater than reset retry state threshold");
        }
    }

    TResult Update(TInstant startedAt, TInstant lastSeenAt, TInstant now, TRetryPolicyState& state) const final {
        const auto uptime = lastSeenAt - startedAt;
        if (ResetBackoffThreshold && uptime >= ResetBackoffThreshold) {
            // Reset retry state after uptime threshold
            state.RetryCount = 0;
        }

        state.RetryCounterUpdatedAt = now;
        state.RetryCount++;
        TResult result = {.Retry = true};

        if (QueryUptimeThreshold && uptime >= QueryUptimeThreshold) {
            // Immediate retry after uptime threshold
            return result;
        }

        double backoff = InitialBackoff.GetValue();
        const double maxBackoff = MaxBackoff ? MaxBackoff.GetValue() : MaxFloor<TDuration::TValue>();
        for (ui64 i = 0; i + 1 < state.RetryCount && backoff < maxBackoff; ++i) {
            backoff *= BackoffMultiplier;
        }
        backoff = std::min(backoff, maxBackoff);

        if (JitterFactor) {
            backoff = std::min(backoff * (1 + (1 - 2 * RandomNumber<double>()) * JitterFactor), maxBackoff);
        }

        result.Backoff = TDuration::FromValue(backoff);
        return result;
    }

    bool IsInitialized() const final {
        return true;
    }

private:
    const double BackoffMultiplier = 0.0;
    const TDuration InitialBackoff;
    const TDuration MaxBackoff;
    const TDuration ResetBackoffThreshold;
    const TDuration QueryUptimeThreshold;
    const double JitterFactor = 0.0;
};

} // anonymous namespace

TRetryPolicyState::TRetryPolicyState(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate)
    : RetryCount(retryCount)
    , RetryCounterUpdatedAt(retryCounterUpdatedAt)
    , RetryRate(retryRate)
{}

TRetryPolicyState::TRetryPolicyState(const NKikimrKqp::TScriptExecutionRetryState& retryState)
    : RetryCount(retryState.GetRetryCounter())
    , RetryCounterUpdatedAt(NProtoInterop::CastFromProto(retryState.GetRetryCounterUpdatedAt()))
    , RetryRate(retryState.GetRetryRate())
{}

void TRetryPolicyState::SaveToProto(NKikimrKqp::TScriptExecutionRetryState& retryState) const {
    retryState.SetRetryCounter(RetryCount);
    *retryState.MutableRetryCounterUpdatedAt() = NProtoInterop::CastToProto(RetryCounterUpdatedAt);
    retryState.SetRetryRate(RetryRate);
}

TRetryPolicyItem::TRetryPolicyItem()
    : Policy(std::make_shared<TLinearBackoffPolicy>())
{}

TRetryPolicyItem::TRetryPolicyItem(ui64 retryCount, ui64 retryLimit, TDuration retryPeriod, TDuration backoffPeriod)
    : Policy(std::make_shared<TLinearBackoffPolicy>(retryCount, retryLimit, retryPeriod, backoffPeriod))
    , PolicyInitialized(Policy->IsInitialized())
{}

TRetryPolicyItem::TRetryPolicyItem(IPolicy::TPtr policy)
    : Policy(std::move(policy))
    , PolicyInitialized(Policy && Policy->IsInitialized())
{}

std::optional<TRetryPolicyItem> TRetryPolicyItem::FromProto(Ydb::StatusIds::StatusCode status, const NKikimrKqp::TScriptExecutionRetryState& retryState) {
    for (const auto& mapping : retryState.GetRetryPolicyMapping()) {
        for (const auto mappingStatus : mapping.GetStatusCode()) {
            if (mappingStatus != status) {
                continue;
            }

            switch (mapping.GetPolicyCase()) {
                case NKikimrKqp::TScriptExecutionRetryState::TMapping::kBackoffPolicy: {
                    return TRetryPolicyItem(std::make_shared<TLinearBackoffPolicy>(mapping.GetBackoffPolicy()));
                }
                case NKikimrKqp::TScriptExecutionRetryState::TMapping::kExponentialDelayPolicy: {
                    return TRetryPolicyItem(std::make_shared<TExponentialBackoffPolicy>(mapping.GetExponentialDelayPolicy()));
                }
                default: {
                    throw std::runtime_error(TStringBuilder() << "Unsupported retry policy: " << mapping.DebugString());
                }
            }
        }
    }

    return std::nullopt;
}

void TRetryLimiter::Assign(ui64 retryCount, TInstant retryCounterUpdatedAt, double retryRate) {
    RetryCount = retryCount;
    RetryCounterUpdatedAt = retryCounterUpdatedAt;
    RetryRate = retryRate;
}

bool TRetryLimiter::UpdateOnRetry(TInstant lastSeenAt, const TRetryPolicyItem& policy, TInstant now) {
    return UpdateOnRetry(RetryCounterUpdatedAt, lastSeenAt, policy, now);
}

bool TRetryLimiter::UpdateOnRetry(TInstant startedAt, TInstant lastSeenAt, const TRetryPolicyItem& policy, TInstant now) {
    if (!policy.Policy) {
        LastError = "No retry policy set";
        return false;
    }

    auto result = policy.Policy->Update(startedAt, lastSeenAt, now, *this);
    LastError = std::move(result.LastError);
    Backoff = result.Backoff;

    if (!result.Retry && !LastError) {
        LastError = "Retry limit reached";
    }

    return result.Retry;
}

}  // namespace NKikimr::NKqp
