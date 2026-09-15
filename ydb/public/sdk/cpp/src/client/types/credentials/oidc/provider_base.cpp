#include "provider_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <algorithm>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

std::exception_ptr StoppedError() {
    return std::make_exception_ptr(TError("provider stopped", false, {}));
}

} // namespace

TProviderBase::TProviderBase(TOidcConfig config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : Config(std::move(config))
    , Facility(std::move(facility))
    , Standalone(standalone)
    , Pending(NThreading::NewPromise<std::string>())
{
}

TProviderBase::~TProviderBase() = default;

NThreading::TFuture<std::string> TProviderBase::GetAuthInfoAsync() const {
    std::string token;
    std::exception_ptr error;
    with_lock (Mutex) {
        if (Stopping || (!Standalone && Facility.expired())) {
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
        return !Stopping && (Standalone || !Facility.expired()) &&
               ((Tokens.has_value() && Tokens->AccessToken.IsValid(TInstant::Now())) || Error == nullptr);
    }
}

void TProviderBase::Stop() {
    NThreading::TPromise<std::string> pending;
    with_lock (Mutex) {
        Stopping = true;
        pending = Pending;
    }
    Changed.notify_all();
    Cancellation.Cancel();
    pending.TrySetException(StoppedError());
    CancelDeliveries();
}

void TProviderBase::Run() {
    try {
        if (IsStopped()) {
            Stop();
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

TRefreshingProviderBase::TRefreshingProviderBase(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : TProviderBase(config, std::move(facility), standalone)
    , Protocol(Config, Cancellation.Token())
{
}

void TRefreshingProviderBase::RunTokens() {
    TTokenCache current = ReadCache().value_or(TTokenCache{});

    const bool unknownRefresh = !current.AccessToken.ExpiresAt && current.RefreshToken.has_value();
    if (current.AccessToken.IsValid(TInstant::Now()) && !unknownRefresh) {
        Publish(current);
        if (!WaitForRefresh(current)) {
            return;
        }
    }

    TDuration retryDelay = TDuration::MilliSeconds(200);
    for (;;) {
        if (IsStopped()) {
            Stop();
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
            if (!Wait(retryDelay)) {
                return;
            }
            retryDelay = std::min(retryDelay * 2, TDuration::Seconds(30));
        }
    }
}

bool TProviderBase::IsStopped() const {
    with_lock (Mutex) {
        return Stopping || (!Standalone && Facility.expired());
    }
}

bool TProviderBase::Wait(TDuration delay) {
    with_lock (Mutex) {
        auto remaining = std::chrono::microseconds(delay.MicroSeconds());
        const auto end = std::chrono::steady_clock::now() + remaining;
        while (!Stopping && (Standalone || !Facility.expired())) {
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
    Stop();
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
    return Wait(std::max((*current.AccessToken.ExpiresAt - now) / 2, TDuration::MilliSeconds(1)));
}

void TProviderBase::Write(const TTokenCache& tokens) const {
    if (Config.Cacher_) {
        Config.Cacher_->Write(tokens);
    }
}

TTokenCache TRefreshingProviderBase::Update(const TTokenCache& current) {
    if (current.RefreshToken && current.RefreshToken->IsValid(TInstant::Now())) {
        try {
            return Protocol.Refresh(*current.RefreshToken);
        } catch (const TError& error) {
            if (error.Code != "invalid_grant") {
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
        pending.TrySetException(StoppedError());
        return;
    }
    auto completion = [pending, token = std::move(token), error, callbackLifetime]() mutable {
        Y_UNUSED(callbackLifetime);
        if (error) {
            pending.TrySetException(error);
        } else if (!token->IsValid(TInstant::Now())) {
            pending.TrySetException(std::make_exception_ptr(TError("access token expired before delivery", false, {})));
        } else {
            pending.TrySetValue("Bearer " + token->Token);
        }
    };
    try {
        if (Standalone) {
            completion();
        } else if (auto facility = Facility.lock()) {
            facility->PostToResponseQueue(std::move(completion));
        } else {
            pending.TrySetException(StoppedError());
        }
    } catch (...) {
        pending.TrySetException(std::current_exception());
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
        try {
            promise.TrySetException(StoppedError());
        } catch (...) {
            continue;
        }
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
        delivery.Promise.TrySetException(StoppedError());
    }
}

std::optional<TTokenCache> TProviderBase::ReadCache() const {
    return Config.Cacher_ ? Config.Cacher_->Read() : std::nullopt;
}

TProtocol& TRefreshingProviderBase::GetProtocol() {
    return Protocol;
}

TCredentialsProviderAdapter::TCredentialsProviderAdapter(std::shared_ptr<TProviderBase> provider)
    : Provider(std::move(provider))
    , Worker([provider = Provider] { provider->Run(); })
{
}

TCredentialsProviderAdapter::~TCredentialsProviderAdapter() {
    Provider->Stop();
    if (Worker.get_id() == std::this_thread::get_id()) {
        Worker.detach();
    } else {
        Worker.join();
    }
    Provider->CancelDeliveries();
}

std::string TCredentialsProviderAdapter::GetAuthInfo() const {
    return GetAuthInfoAsync().GetValueSync();
}

NThreading::TFuture<std::string> TCredentialsProviderAdapter::GetAuthInfoAsync() const {
    return Provider->GetAuthInfoAsync();
}

bool TCredentialsProviderAdapter::IsValid() const {
    return Provider->IsValid();
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
