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

template <typename TClient>
bool ShouldUseUnaryRetryContext(TClient& client, const TRetryOperationSettings& settings) {
    return settings.CancellationToken_.stop_possible()
        || (IsRetryEnabled(settings) && !client.GetInRetryOperationContext());
}

template <typename TClient, typename TRequestSettings, typename TRunOnce>
auto RunUnaryWithRetry(TClient& client, TRetryOperationSettings settings,
    const TRequestSettings& requestSettings, TRunOnce&& runOnce)
    -> decltype(runOnce(std::declval<TRequestSettings&>()))
{
    const bool nested = client.GetInRetryOperationContext();
    const bool retryEnabled = IsRetryEnabled(settings) && !nested;
    using TResult = decltype(runOnce(std::declval<TRequestSettings&>()));

    auto operation = [requestSettings = requestSettings, runOnce = std::forward<TRunOnce>(runOnce), nested, retryEnabled]
        (TClient& /*clientRef*/, TDuration remainingTimeout) mutable -> TResult {
        auto attemptSettings = retryEnabled ? requestSettings : std::move(requestSettings);
        if (!nested && remainingTimeout != TDuration::Max()) {
            attemptSettings.ClientTimeout(remainingTimeout);
        }
        return runOnce(attemptSettings);
    };

    if (!ShouldUseUnaryRetryContext(client, settings)) {
        return operation(client, settings.MaxTimeout_);
    }
    if (nested) {
        settings.MaxRetries(0);
    }

    using TRetryAsync = Async::TRetryWithoutSession<TClient, decltype(operation), TResult>;
    using TRetryContextAsync = Async::TRetryContext<TClient, TResult>;

    return typename TRetryContextAsync::TPtr(new TRetryAsync(client, std::move(operation), settings))->Execute();
}

} // namespace NYdb::NRetry
