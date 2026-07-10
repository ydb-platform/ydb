#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>

#include <exception>
#include <memory>
#include <optional>

#include <util/string/builder.h>

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
    return TPlainStatus(EStatus::CLIENT_CANCELLED, "Client is stopped");
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
        return result.GetFuture();
    }

    if (deadline != TDeadline::Max()) {
        try {
            scheduleDelayedTask([result]() mutable {
                result.TrySetValue(InitDeadlineExceededStatus());
            }, deadline);
        } catch (...) {
            result.TrySetValue(InitCancelledStatus());
        }
    }

    return result.GetFuture();
}

template <typename TCallback>
class TCallbackOnce {
public:
    explicit TCallbackOnce(TCallback&& callback)
        : Callback_(std::move(callback))
    {}

    void Complete(TWaitResult status) {
        Y_ABORT_UNLESS(Callback_.has_value());
        auto callback = std::move(*Callback_);
        Callback_.reset();
        callback(std::move(status));
    }

private:
    std::optional<TCallback> Callback_;
};

template <typename TCallback, typename TScheduleDelayedTask>
void ScheduleCompletion(
    const std::shared_ptr<TCallbackOnce<TCallback>>& callback,
    TScheduleDelayedTask& scheduleDelayedTask,
    TWaitResult status)
{
    try {
        scheduleDelayedTask([callback, status = std::move(status)]() mutable {
            callback->Complete(std::move(status));
        }, TDeadline::Now());
    } catch (...) {
        callback->Complete(InitCancelledStatus());
    }
}

template <typename TCallbackFactory, typename TScheduleDelayedTask, typename TSubscribeCancel, typename TIsCancelled>
bool DeferUntilReady(
    const TDbDriverStatePtr& dbState,
    bool useAuth,
    TDeadline deadline,
    TCallbackFactory&& callbackFactory,
    TScheduleDelayedTask&& scheduleDelayedTask,
    TSubscribeCancel&& subscribeCancel,
    TIsCancelled&& isCancelled)
{
    auto schedule = std::forward<TScheduleDelayedTask>(scheduleDelayedTask);
    auto subscribe = std::forward<TSubscribeCancel>(subscribeCancel);
    auto cancelled = std::forward<TIsCancelled>(isCancelled);

    if (!useAuth) {
        return false;
    }

    auto credentialsReady = dbState->GetCredentialsReady();
    if (!credentialsReady.Initialized()) {
        return false;
    }

    if (credentialsReady.IsReady()) {
        TWaitResult status;
        try {
            credentialsReady.GetValue();
        } catch (const std::exception& e) {
            status = InitFailedStatus(e);
        } catch (...) {
            status = InitFailedStatus();
        }
        if (!status && cancelled()) {
            status = InitCancelledStatus();
        }
        if (status) {
            auto callbackValue = callbackFactory();
            auto callback = std::make_shared<TCallbackOnce<decltype(callbackValue)>>(std::move(callbackValue));
            ScheduleCompletion(callback, schedule, std::move(status));
            return true;
        }
        return false;
    }

    auto callbackValue = callbackFactory();
    auto callback = std::make_shared<TCallbackOnce<decltype(callbackValue)>>(std::move(callbackValue));
    auto wait = WaitUntilReady(
        std::move(credentialsReady),
        deadline,
        schedule,
        subscribe);

    wait.Subscribe([callback, schedule = std::move(schedule), cancelled = std::move(cancelled)](const NThreading::TFuture<TWaitResult>& future) mutable {
        auto status = future.GetValue();
        if (!status && cancelled()) {
            status = InitCancelledStatus();
        }
        ScheduleCompletion(callback, schedule, std::move(status));
    });

    return true;
}

} // namespace NYdb::Dev::NDeferredCredentials
