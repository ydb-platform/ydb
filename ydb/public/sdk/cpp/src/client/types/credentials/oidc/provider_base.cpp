#include "provider_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include <algorithm>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

std::exception_ptr StoppedError() {
    return std::make_exception_ptr(TError("provider stopped", false, {}));
}

} // namespace

TProviderState::TProviderState(TOidcConfig config, std::weak_ptr<ICoreFacility> facility, bool standalone)
    : Config(std::move(config))
    , Facility(std::move(facility))
    , Standalone(standalone)
    , Pending(NThreading::NewPromise<std::string>())
    , Protocol(Config, Cancellation.Token())
{
}

NThreading::TFuture<std::string> TProviderState::GetAuthInfoAsync() const {
    std::string token;
    std::exception_ptr error;
    {
        std::lock_guard lock(Mutex);
        if (Stopping || (!Standalone && Facility.expired())) {
            error = StoppedError();
        } else if (Tokens && Tokens->AccessToken.IsValid(TInstant::Now())) {
            token = "Bearer " + Tokens->AccessToken.Token;
        } else if (Error) {
            error = Error;
        } else {
            return Pending.GetFuture();
        }
    }
    if (error) {
        return NThreading::MakeErrorFuture<std::string>(error);
    }
    return NThreading::MakeFuture(std::move(token));
}

bool TProviderState::IsValid() const {
    std::lock_guard lock(Mutex);
    return !Stopping && (Standalone || !Facility.expired()) &&
           ((Tokens && Tokens->AccessToken.IsValid(TInstant::Now())) || !Error);
}

void TProviderState::Stop() {
    NThreading::TPromise<std::string> pending;
    {
        std::lock_guard lock(Mutex);
        Stopping = true;
        pending = Pending;
    }
    Changed.notify_all();
    Cancellation.Cancel();
    pending.TrySetException(StoppedError());
    CancelDeliveries();
}

void TProviderState::Run() {
    RunTokens();
    // A facility may discard response tasks after token acquisition has
    // finished. Keep observing its lifetime until every delivery completes.
    for (;;) {
        CompleteDiscardedDeliveries();
        {
            std::lock_guard lock(Mutex);
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

void TProviderState::RunTokens() {
    try {
        if (IsStopped()) {
            Stop();
            return;
        }
        TTokenCache current = Bootstrap();

        // Unknown access expiry with a refresh token is resolved by
        // refreshing once before publishing, avoiding an infinite lifetime.
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
                Publish(current);
                retryDelay = TDuration::MilliSeconds(200);
                if (!WaitForRefresh(current)) {
                    return;
                }
            } catch (const TError& error) {
                if (!error.Retryable) {
                    throw;
                }
                // Retry does not discard a still-valid access token.
                if (!Wait(retryDelay)) {
                    return;
                }
                retryDelay = std::min(retryDelay * 2, TDuration::Seconds(30));
            }
        }
    } catch (...) {
        Fail(std::current_exception());
    }
}

bool TProviderState::IsStopped() const {
    std::lock_guard lock(Mutex);
    return Stopping || (!Standalone && Facility.expired());
}

bool TProviderState::Wait(TDuration delay) {
    std::unique_lock lock(Mutex);
    // Periodic weak-facility checks keep background work bounded even if a
    // caller keeps its provider after destroying the supplied facility.
    auto remaining = std::chrono::microseconds(delay.MicroSeconds());
    const auto end = std::chrono::steady_clock::now() + remaining;
    while (!Stopping && (Standalone || !Facility.expired())) {
        lock.unlock();
        CompleteDiscardedDeliveries();
        lock.lock();
        if (Stopping) {
            break;
        }
        if (remaining <= std::chrono::microseconds::zero()) {
            return true;
        }
        Changed.wait_for(lock, std::min(remaining, std::chrono::microseconds(100'000)), [this] { return Stopping; });
        remaining = std::chrono::duration_cast<std::chrono::microseconds>(end - std::chrono::steady_clock::now());
    }
    lock.unlock();
    Stop();
    return false;
}

bool TProviderState::WaitForRefresh(const TTokenCache& current) {
    if (!current.AccessToken.ExpiresAt) {
        return false;
    }
    const auto now = TInstant::Now();
    if (*current.AccessToken.ExpiresAt <= now) {
        return true;
    }
    return Wait(std::max((*current.AccessToken.ExpiresAt - now) / 2, TDuration::MilliSeconds(1)));
}

std::unique_ptr<ITokenCacheLock> TProviderState::LockCache() {
    auto* locking = dynamic_cast<ILockingTokenCacher*>(Config.Cacher_.get());
    if (!locking) {
        return {};
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    for (;;) {
        if (auto lock = locking->TryLock()) {
            return lock;
        }
        if (!Wait(TDuration::MilliSeconds(50))) {
            throw TError("provider stopped", false, {});
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            throw TError("token cache lock timeout", true, {});
        }
    }
}

void TProviderState::Write(const TTokenCache& tokens) const {
    if (Config.Cacher_) {
        Config.Cacher_->Write(tokens);
    }
}

TTokenCache TProviderState::Update(TTokenCache current) {
    auto lock = LockCache();
    if (Config.Cacher_) {
        if (auto cached = Config.Cacher_->Read()) {
            // Another process may have rotated the refresh token while we
            // waited for the lock. Always use its latest refresh credential.
            if (cached->AccessToken.IsValid(TInstant::Now()) &&
                (cached->AccessToken.Token != current.AccessToken.Token ||
                 cached->AccessToken.ExpiresAt != current.AccessToken.ExpiresAt)) {
                return *cached;
            }
            current = std::move(*cached);
        }
    }
    if (current.RefreshToken && current.RefreshToken->IsValid(TInstant::Now())) {
        try {
            auto refreshed = Protocol.Refresh(*current.RefreshToken);
            Write(refreshed);
            return refreshed;
        } catch (const TError& error) {
            if (error.Code != "invalid_grant") {
                throw;
            }
            current.RefreshToken.reset();
        }
    }
    return AcquireToken(current, lock);
}

void TProviderState::Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error) {
    // Keep the old promise reachable until the response task executes; Stop
    // must also complete it if its facility discards queued callbacks.
    const auto callbackLifetime = std::make_shared<int>(0);
    bool stopped;
    {
        std::lock_guard lock(Mutex);
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
        (void)callbackLifetime; // Its last owner identifies a discarded response task.
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

void TProviderState::CompleteDiscardedDeliveries() {
    std::vector<NThreading::TPromise<std::string>> discarded;
    {
        std::lock_guard lock(Mutex);
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
    // Tasks may be destroyed under an executor's mutex. Deliver
    // cancellation on our worker, never from a task destructor.
    for (auto& promise : discarded) {
        try {
            promise.TrySetException(StoppedError());
        } catch (...) {
            // The promise is already resolved. A subscriber's
            // exception must not abort the other cancellations.
            continue;
        }
    }
}

void TProviderState::Publish(const TTokenCache& current) {
    NThreading::TPromise<std::string> pending;
    {
        std::lock_guard lock(Mutex);
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

void TProviderState::Fail(std::exception_ptr error) {
    NThreading::TPromise<std::string> pending;
    {
        std::lock_guard lock(Mutex);
        Error = error;
        pending = Pending;
    }
    Complete(pending, std::nullopt, error);
}

void TProviderState::CancelDeliveries() {
    std::vector<TDelivery> deliveries;
    {
        std::lock_guard lock(Mutex);
        deliveries.swap(Deliveries);
    }
    for (auto& delivery : deliveries) {
        delivery.Promise.TrySetException(StoppedError());
    }
}

std::optional<TTokenCache> TProviderState::ReadCache() const {
    return Config.Cacher_ ? Config.Cacher_->Read() : std::nullopt;
}

TProtocol& TProviderState::GetProtocol() {
    return Protocol;
}

TProviderBase::TProviderBase(std::shared_ptr<TProviderState> state)
    : State(std::move(state))
    , Worker([state = State] { state->Run(); })
{
}

TProviderBase::~TProviderBase() {
    State->Stop();
    if (Worker.get_id() == std::this_thread::get_id()) {
        Worker.detach(); // State remains owned by the running callback.
    } else {
        Worker.join();
    }
    State->CancelDeliveries();
}

std::string TProviderBase::GetAuthInfo() const {
    return GetAuthInfoAsync().GetValueSync();
}

NThreading::TFuture<std::string> TProviderBase::GetAuthInfoAsync() const {
    return State->GetAuthInfoAsync();
}

bool TProviderBase::IsValid() const {
    return State->IsValid();
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
