#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

#include <library/cpp/threading/future/future.h>

#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

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
            scheduleDelayedTask([result](bool ok) mutable {
                result.TrySetValue(ok
                    ? InitDeadlineExceededStatus()
                    : InitCancelledStatus());
            }, deadline);
        } catch (...) {
            result.TrySetValue(InitCancelledStatus());
        }
    }

    return result.GetFuture();
}

template <typename TCallbackFactory, typename TScheduleDelayedTask, typename TSubscribeCancel, typename TIsCancelled>
bool DeferUntilReady(
    NThreading::TFuture<void> credentialsReady,
    TDeadline deadline,
    TCallbackFactory&& callbackFactory,
    TScheduleDelayedTask&& scheduleDelayedTask,
    TSubscribeCancel&& subscribeCancel,
    TIsCancelled&& isCancelled)
{
    NThreading::TFuture<TWaitResult> wait;

    if (credentialsReady.IsReady()) {
        TWaitResult status;
        try {
            credentialsReady.GetValue();
        } catch (const std::exception& e) {
            status = InitFailedStatus(e);
        } catch (...) {
            status = InitFailedStatus();
        }
        if (!status && isCancelled()) {
            status = InitCancelledStatus();
        }
        if (!status) {
            return false;
        }
        wait = NThreading::MakeFuture(std::move(status));
    } else {
        wait = WaitUntilReady(
            std::move(credentialsReady),
            deadline,
            scheduleDelayedTask,
            subscribeCancel);
    }

    auto callbackValue = callbackFactory();
    auto callback = std::make_shared<std::decay_t<decltype(callbackValue)>>(std::move(callbackValue));

    wait.Subscribe([callback, schedule = std::forward<TScheduleDelayedTask>(scheduleDelayedTask)](const NThreading::TFuture<TWaitResult>& future) mutable {
        auto status = future.GetValue();
        schedule([callback, status = std::move(status)](bool ok) mutable {
            if (!ok) {
                status = InitCancelledStatus();
            }
            (*callback)(std::move(status));
        }, TDeadline::Now());
    });

    return true;
}

} // namespace NYdb::Dev::NDeferredCredentials
