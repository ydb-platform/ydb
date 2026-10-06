#include "provider_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <algorithm>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

std::exception_ptr StoppedError();
void SetException(NThreading::TPromise<std::string> promise, std::exception_ptr error) noexcept;

std::exception_ptr StoppedError() {
    return std::make_exception_ptr(TError("provider stopped", false, {}));
}

void SetException(NThreading::TPromise<std::string> promise, std::exception_ptr error) noexcept {
    try {
        promise.TrySetException(std::move(error));
    } catch (...) {
        // The promise is already settled; a throwing subscriber must not interrupt cleanup.
    }
}

} // namespace

TProviderBase::TProviderBase(TOidcConfig config, std::weak_ptr<ICoreFacility> facility)
    : Config(std::move(config))
    , Facility(std::move(facility))
    , Pending(NThreading::NewPromise<std::string>())
{
}

TProviderBase::~TProviderBase() {
    Stop();
}

void TProviderBase::Start() {
    try {
        Worker = std::thread([this] { Run(); });
    } catch (...) {
        Fail(std::current_exception());
    }
}

std::string TProviderBase::GetAuthInfo() const {
    return GetAuthInfoAsync().GetValueSync();
}

NThreading::TFuture<std::string> TProviderBase::GetAuthInfoAsync() const {
    std::string token;
    std::exception_ptr error;
    with_lock (Mutex) {
        if (Stopping || Facility.expired()) {
            error = StoppedError();
        } else if (Tokens.has_value() && Tokens->AccessToken.IsValid(TInstant::Now())) {
            token = "Bearer " + Tokens->AccessToken.Token;
        } else if (Error != nullptr) {
            error = Error;
        } else {
            return Pending.GetFuture();
        }
    }
    if (error != nullptr) {
        return NThreading::MakeErrorFuture<std::string>(error);
    }
    return NThreading::MakeFuture(std::move(token));
}

bool TProviderBase::IsValid() const {
    with_lock (Mutex) {
        return !Stopping && !Facility.expired() &&
               ((Tokens.has_value() && Tokens->AccessToken.IsValid(TInstant::Now())) || Error == nullptr);
    }
}

void TProviderBase::Stop() {
    RequestStop();
    if (Worker.joinable()) {
        Worker.join();
    }
    CancelDeliveries();
}

void TProviderBase::RequestStop() {
    NThreading::TPromise<std::string> pending;
    with_lock (Mutex) {
        if (Stopping) {
            return;
        }
        Stopping = true;
        pending = Pending;
    }
    Changed.notify_all();
    Cancellation.Cancel();
    SetException(pending, StoppedError());
    CancelDeliveries();
}

void TProviderBase::Run() {
    try {
        if (IsStopped()) {
            RequestStop();
            return;
        }
        RunTokens();
    } catch (...) {
        Fail(std::current_exception());
    }

    for (;;) {
        CompleteDiscardedDeliveries();
        with_lock (Mutex) {
            const bool finished = std::all_of(Deliveries.begin(), Deliveries.end(), [](const auto& delivery) {
                return delivery.Promise.GetFuture().HasValue() || delivery.Promise.GetFuture().HasException();
            });
            if (finished) {
                return;
            }
        }
        if (!Wait(TDuration::MilliSeconds(100))) {
            return;
        }
    }
}

TRefreshingProviderBase::TRefreshingProviderBase(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility)
    : TProviderBase(config, std::move(facility))
    , Protocol(Config, Cancellation.Token())
{
}

void TRefreshingProviderBase::RunTokens() {
    TTokenCache current = ReadCache().value_or(TTokenCache{});

    const bool unknownRefresh = !current.AccessToken.ExpiresAt.has_value() && current.RefreshToken.has_value();
    if (current.AccessToken.IsValid(TInstant::Now()) && !unknownRefresh) {
        Publish(current);
        if (!WaitForRefresh(current)) {
            return;
        }
    }

    TDuration retryDelay = TDuration::MilliSeconds(200);
    for (;;) {
        if (IsStopped()) {
            RequestStop();
            return;
        }
        try {
            current = Update(current);
            Write(current);
            Publish(current);
            retryDelay = TDuration::MilliSeconds(200);
            if (!WaitForRefresh(current)) {
                return;
            }
        } catch (const TError& error) {
            if (!error.Retryable) {
                throw;
            }
            // Settle existing waiters while retrying in the background. GetAuthInfoAsync()
            // keeps serving a still-valid token; Publish() clears the error on recovery.
            Fail(std::current_exception());
            if (!Wait(retryDelay)) {
                return;
            }
            retryDelay = std::min(retryDelay * 2, TDuration::Seconds(30));
        }
    }
}

bool TProviderBase::IsStopped() const {
    with_lock (Mutex) {
        return Stopping || Facility.expired();
    }
}

bool TProviderBase::Wait(TDuration delay) {
    with_lock (Mutex) {
        auto remaining = std::chrono::microseconds(delay.MicroSeconds());
        const auto end = std::chrono::steady_clock::now() + remaining;
        while (!Stopping && !Facility.expired()) {
            {
                auto unguard = Unguard(Mutex);
                CompleteDiscardedDeliveries();
            }
            if (Stopping) {
                break;
            }
            if (remaining <= std::chrono::microseconds::zero()) {
                return true;
            }
            Changed.wait_for(Mutex, std::min(remaining, std::chrono::microseconds(100'000)), [this] { return Stopping; });
            remaining = std::chrono::duration_cast<std::chrono::microseconds>(end - std::chrono::steady_clock::now());
        }
    }
    RequestStop();
    return false;
}

bool TRefreshingProviderBase::WaitForRefresh(const TTokenCache& current) {
    if (!current.AccessToken.ExpiresAt.has_value()) {
        return false;
    }
    const auto now = TInstant::Now();
    if (*current.AccessToken.ExpiresAt <= now) {
        return true;
    }
    // Preserve short lifetimes: a fixed multi-second floor could delay refresh
    // until after expiry. Never reinterpret a known expiry as an unknown one.
    return Wait(std::max((*current.AccessToken.ExpiresAt - now) / 2, TDuration::MilliSeconds(1)));
}

void TProviderBase::Write(const TTokenCache& tokens) const {
    try {
        if (Config.Cacher_ != nullptr) {
            Config.Cacher_->Write(tokens);
        }
    } catch (...) {
        // Persistence is optional; keep the acquired token usable in memory.
        // User-supplied exception messages may contain credentials.
    }
}

TTokenCache TRefreshingProviderBase::Update(const TTokenCache& current) {
    if (current.RefreshToken.has_value() && current.RefreshToken->IsValid(TInstant::Now())) {
        try {
            return Protocol.Refresh(*current.RefreshToken);
        } catch (const TError& error) {
            if (error.Retryable || error.Code != "invalid_grant") {
                throw;
            }
        }
    }
    return AcquireToken();
}

void TProviderBase::Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error) {
    const auto callbackLifetime = std::make_shared<int>(0);
    bool stopped;
    with_lock (Mutex) {
        stopped = Stopping;
        Deliveries.erase(std::remove_if(Deliveries.begin(), Deliveries.end(), [](const auto& delivery) {
                             return delivery.Promise.GetFuture().HasValue() || delivery.Promise.GetFuture().HasException();
                         }), Deliveries.end());
        if (!stopped) {
            Deliveries.push_back({pending, callbackLifetime});
        }
    }
    if (stopped) {
        SetException(pending, StoppedError());
        return;
    }
    auto completion = [pending, token = std::move(token), error, callbackLifetime]() mutable {
        Y_UNUSED(callbackLifetime);
        try {
            if (error != nullptr) {
                SetException(pending, error);
            } else if (!token->IsValid(TInstant::Now())) {
                SetException(pending, std::make_exception_ptr(TError("access token expired before delivery", false, {})));
            } else {
                pending.TrySetValue("Bearer " + token->Token);
            }
        } catch (...) {
            // Covers both preparation failures and subscribers throwing after
            // settlement. Neither may escape into the response queue executor.
            SetException(pending, std::current_exception());
        }
    };
    try {
        if (auto facility = Facility.lock(); facility != nullptr) {
            facility->PostToResponseQueue(std::move(completion));
        } else {
            SetException(pending, StoppedError());
        }
    } catch (...) {
        SetException(pending, std::current_exception());
    }
}

void TProviderBase::CompleteDiscardedDeliveries() {
    std::vector<NThreading::TPromise<std::string>> discarded;
    with_lock (Mutex) {
        auto it = Deliveries.begin();
        while (it != Deliveries.end()) {
            const auto future = it->Promise.GetFuture();
            if (future.HasValue() || future.HasException()) {
                it = Deliveries.erase(it);
            } else if (it->CallbackLifetime.expired()) {
                discarded.push_back(it->Promise);
                it = Deliveries.erase(it);
            } else {
                ++it;
            }
        }
    }

    for (auto& promise : discarded) {
        SetException(promise, StoppedError());
    }
}

void TProviderBase::Publish(const TTokenCache& current) {
    NThreading::TPromise<std::string> pending;
    with_lock (Mutex) {
        if (Stopping) {
            return;
        }
        Tokens = current;
        Error = nullptr;
        pending = Pending;
        Pending = NThreading::NewPromise<std::string>();
    }
    Complete(pending, current.AccessToken, {});
}

void TProviderBase::Fail(std::exception_ptr error) {
    NThreading::TPromise<std::string> pending;
    with_lock (Mutex) {
        Error = error;
        pending = Pending;
    }
    Complete(pending, std::nullopt, error);
}

void TProviderBase::CancelDeliveries() {
    std::vector<TDelivery> deliveries;
    with_lock (Mutex) {
        deliveries.swap(Deliveries);
    }
    for (auto& delivery : deliveries) {
        SetException(delivery.Promise, StoppedError());
    }
}

std::optional<TTokenCache> TProviderBase::ReadCache() const {
    try {
        return Config.Cacher_ != nullptr ? Config.Cacher_->Read() : std::nullopt;
    } catch (...) {
        // A broken cache must not prevent a fresh authorization attempt.
        return std::nullopt;
    }
}

TProtocol& TRefreshingProviderBase::GetProtocol() {
    return Protocol;
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
