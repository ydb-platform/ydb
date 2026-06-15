#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry_async.h>

#include <optional>
#include <utility>

namespace NYdb::inline Dev::NRetry {

enum class ERetryIdempotentDefault {
    False,
    True,
};

inline bool IsRetryEnabled(const TRetryOperationSettings& settings) {
    return settings.MaxRetries_ > 0;
}

inline TRetryOperationSettings ResolveRetrySettings(
    const TRetryOperationSettings& clientDefault,
    const std::optional<TRetryOperationSettings>& operationOverride,
    const std::optional<TRetryOperationSettings>& explicitOverride,
    TDuration operationClientTimeout,
    ERetryIdempotentDefault idempotentDefault)
{
    TRetryOperationSettings settings = explicitOverride.value_or(
        operationOverride.value_or(clientDefault));

    if (operationClientTimeout != TDuration::Max()) {
        if (settings.MaxTimeout_ == TDuration::Max() || operationClientTimeout < settings.MaxTimeout_) {
            settings.MaxTimeout(operationClientTimeout);
        }
    }

    if (!explicitOverride && !operationOverride && idempotentDefault == ERetryIdempotentDefault::True
        && !clientDefault.IdempotentWasSet_)
    {
        settings.Idempotent(true);
    }

    return settings;
}

inline TRetryOperationSettings ResolveRetrySettings(
    const TRetryOperationSettings& clientDefault,
    const std::optional<TRetryOperationSettings>& operationOverride,
    TDuration operationClientTimeout,
    ERetryIdempotentDefault idempotentDefault)
{
    return ResolveRetrySettings(
        clientDefault, operationOverride, std::nullopt, operationClientTimeout, idempotentDefault);
}

template <typename TClient, typename TRunOnce>
auto RunUnaryWithRetry(TClient& client, TRetryOperationSettings settings, TRunOnce&& runOnce)
    -> decltype(runOnce(TDuration::Max()))
{
    if (client.GetInRetryOperationContext()) {
        return runOnce(TDuration::Max());
    }
    if (!IsRetryEnabled(settings)) {
        return runOnce(settings.MaxTimeout_);
    }

    using TResult = decltype(runOnce(TDuration::Max()));

    auto operation = [runOnce = std::forward<TRunOnce>(runOnce)](TClient& /*clientRef*/, TDuration remainingTimeout) -> TResult {
        return runOnce(remainingTimeout);
    };

    using TRetryAsync = Async::TRetryWithoutSession<TClient, decltype(operation), TResult>;
    using TRetryContextAsync = Async::TRetryContext<TClient, TResult>;

    return typename TRetryContextAsync::TPtr(new TRetryAsync(client, std::move(operation), settings))->Execute();
}

} // namespace NYdb::NRetry
