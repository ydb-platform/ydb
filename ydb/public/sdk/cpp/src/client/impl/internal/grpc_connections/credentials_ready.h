#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>

#include <library/cpp/threading/future/core/coroutine_traits.h>

#include <exception>
#include <memory>
#include <optional>

namespace NYdb::inline Dev::NDeferredCredentials {

inline TPlainStatus InitFailedStatus(const std::exception& e) {
    return TPlainStatus(EStatus::CLIENT_UNAUTHENTICATED,
        TStringBuilder() << "Credentials provider initialization failed. " << e.what());
}

inline TPlainStatus InitFailedStatus() {
    return TPlainStatus(EStatus::CLIENT_UNAUTHENTICATED,
        "Credentials provider initialization failed");
}

inline TPlainStatus InitDeadlineExceededStatus() {
    return TPlainStatus(EStatus::CLIENT_DEADLINE_EXCEEDED,
        "Request deadline exceeded while waiting for credentials");
}

inline TPlainStatus InitCancelledStatus() {
    return TPlainStatus(EStatus::CLIENT_CANCELLED,
        "Client is stopped");
}

using TWaitResult = std::optional<TPlainStatus>;

template <typename TScheduleDelayedTask, typename TSubscribeCancel>
NThreading::TFuture<TWaitResult> WaitUntilReady(
    NThreading::TFuture<void> credentialsReady,
    TDeadline deadline,
    TScheduleDelayedTask&& scheduleDelayedTask,
    TSubscribeCancel&& subscribeCancel)
{
    auto result = NThreading::NewPromise<TWaitResult>();

    credentialsReady.Subscribe([result](const NThreading::TFuture<void>& future) mutable {
        try {
            future.GetValue();
        } catch (const std::exception& e) {
            result.TrySetValue(InitFailedStatus(e));
            return;
        } catch (...) {
            result.TrySetValue(InitFailedStatus());
            return;
        }
        result.TrySetValue(TWaitResult{});
    });

    if (!subscribeCancel([result]() mutable {
        result.TrySetValue(InitCancelledStatus());
    })) {
        result.TrySetValue(InitCancelledStatus());
        co_return co_await result.GetFuture();
    }

    if (!(deadline == TDeadline::Max())) {
        scheduleDelayedTask([result]() mutable {
            result.TrySetValue(InitDeadlineExceededStatus());
        }, deadline);
    }

    co_return co_await result.GetFuture();
}

template <typename TCallback>
class TCallbackOnce {
public:
    explicit TCallbackOnce(TCallback&& callback)
        : Callback_(std::move(callback))
    {}

    void Complete(TWaitResult status) {
        auto callback = std::move(*Callback_);
        Callback_.reset();
        callback(std::move(status));
    }

private:
    std::optional<TCallback> Callback_;
};

template <typename TCallbackFactory, typename TScheduleDelayedTask, typename TSubscribeCancel>
bool DeferUntilReady(
    const TDbDriverStatePtr& dbState,
    bool useAuth,
    TDeadline deadline,
    TCallbackFactory&& callbackFactory,
    TScheduleDelayedTask&& scheduleDelayedTask,
    TSubscribeCancel&& subscribeCancel)
{
    if (!useAuth) {
        return false;
    }

    auto credentialsReady = dbState->GetCredentialsReady();
    if (!credentialsReady.Initialized()) {
        return false;
    }

    if (!credentialsReady.IsReady()) {
        auto callbackValue = callbackFactory();
        auto callback = std::make_shared<TCallbackOnce<decltype(callbackValue)>>(std::move(callbackValue));
        auto wait = WaitUntilReady(
            std::move(credentialsReady),
            deadline,
            std::forward<TScheduleDelayedTask>(scheduleDelayedTask),
            std::forward<TSubscribeCancel>(subscribeCancel));

        wait.Subscribe([callback](const NThreading::TFuture<TWaitResult>& future) mutable {
            callback->Complete(future.GetValue());
        });

        return true;
    }

    try {
        credentialsReady.GetValue();
    } catch (const std::exception& e) {
        callbackFactory()(InitFailedStatus(e));
        return true;
    } catch (...) {
        callbackFactory()(InitFailedStatus());
        return true;
    }

    return false;
}

} // namespace NYdb::Dev::NDeferredCredentials
