#include "provider_base.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/system/guard.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NOidc::NPrivate {

namespace {

std::optional<TTokenCache> ReadCache(const std::shared_ptr<ITokenCacher>& cacher);
void WriteCache(const std::shared_ptr<ITokenCacher>& cacher, const TTokenCache& tokens);

std::optional<TTokenCache> ReadCache(const std::shared_ptr<ITokenCacher>& cacher) {
    try {
        return cacher != nullptr ? cacher->Read() : std::nullopt;
    } catch (...) {
        // A broken cache must not prevent a fresh authorization attempt.
        return std::nullopt;
    }
}

void WriteCache(const std::shared_ptr<ITokenCacher>& cacher, const TTokenCache& tokens) {
    try {
        if (cacher != nullptr) {
            cacher->Write(tokens);
        }
    } catch (...) {
        // Persistence is optional; keep the acquired token usable in memory.
        // User-supplied exception messages may contain credentials.
        return;
    }
}

} // namespace

TProviderBase::TProviderBase(TOidcConfig config)
    : Config(std::move(config))
{
}

TProviderBase::~TProviderBase() {
    Stop();
}

TCredentialsProviderPtr TProviderBase::CreateProvider(std::weak_ptr<ICoreFacility> facility) {
    auto context = std::make_shared<TProviderContext>(std::move(facility));
    auto provider = std::make_shared<TCredentialsProvider>(shared_from_this(), context);
    bool start = false;
    with_lock (Mutex) {
        std::erase_if(Contexts, [](const auto& previous) { return previous.expired(); });
        Contexts.push_back(context);
        if (!Started && !Stopping && !context->IsStopped()) {
            Started = true;
            start = true;
        }
    }
    if (start) {
        Start();
    }
    return provider;
}

std::thread TProviderBase::CreateWorker() {
    return std::thread([this] { Run(); });
}

void TProviderBase::Start() {
    try {
        Worker = CreateWorker();
    } catch (...) {
        // No worker can detect discarded response-queue deliveries after startup fails.
        Fail(std::current_exception(), false);
    }
}

NThreading::TFuture<std::string> TProviderBase::GetAuthInfoAsync(const std::shared_ptr<TProviderContext>& context) const {
    with_lock (Mutex) {
        if (Stopping || context->IsStopped()) {
            return NThreading::MakeErrorFuture<std::string>(StoppedError());
        }
        if (Tokens.has_value() && Tokens->AccessToken.IsValid(TInstant::Now())) {
            return NThreading::MakeFuture("Bearer " + Tokens->AccessToken.Token);
        }
        if (Error != nullptr) {
            return NThreading::MakeErrorFuture<std::string>(Error);
        }
        return context->GetPending();
    }
}

bool TProviderBase::IsValid(const std::shared_ptr<TProviderContext>& context) const {
    with_lock (Mutex) {
        return !Stopping && !context->IsStopped() &&
               ((Tokens.has_value() && Tokens->AccessToken.IsValid(TInstant::Now())) || Error == nullptr);
    }
}

void TProviderBase::Stop() {
    RequestStop();
    if (Worker.joinable()) {
        Worker.join();
    }
}

std::vector<std::shared_ptr<TProviderContext>> TProviderBase::GetContexts() {
    std::vector<std::shared_ptr<TProviderContext>> contexts;
    with_lock (Mutex) {
        auto it = Contexts.begin();
        while (it != Contexts.end()) {
            if (auto context = it->lock(); context != nullptr) {
                contexts.push_back(std::move(context));
                ++it;
            } else {
                it = Contexts.erase(it);
            }
        }
    }
    return contexts;
}

void TProviderBase::RequestStop() {
    with_lock (Mutex) {
        if (Stopping) {
            return;
        }
        Stopping = true;
    }
    Changed.notify_all();
    Cancellation.Cancel();
    for (const auto& context : GetContexts()) {
        context->Stop(false);
    }
}

void TProviderBase::Run() {
    try {
        if (IsStopped()) {
            return;
        }
        RunTokens();
    } catch (...) {
        Fail(std::current_exception(), true);
    }
    while (CompleteDiscardedDeliveries()) {
        if (!Wait(TDuration::MilliSeconds(100))) {
            return;
        }
    }
}

TRefreshingProviderBase::TRefreshingProviderBase(const TOidcConfig& config)
    : TProviderBase(config)
    , Protocol(Config, Cancellation.Token())
{
}

void TRefreshingProviderBase::RunTokens() {
    TTokenCache current = ReadCache(Config.Cacher_).value_or(TTokenCache{});

    const bool unknownRefresh = !current.AccessToken.ExpiresAt.has_value() && current.RefreshToken.has_value();
    if (current.AccessToken.IsValid(TInstant::Now()) && !unknownRefresh) {
        Publish(current, false);
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
            Publish(current, true);
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
            Fail(std::current_exception(), true);
            if (!Wait(retryDelay)) {
                return;
            }
            retryDelay = std::min(retryDelay * 2, TDuration::Seconds(30));
        }
    }
}

bool TProviderBase::IsStopped() const {
    with_lock (Mutex) {
        return Stopping;
    }
}

bool TProviderBase::Wait(TDuration delay) {
    with_lock (Mutex) {
        auto remaining = std::chrono::microseconds(delay.MicroSeconds());
        const auto end = std::chrono::steady_clock::now() + remaining;
        while (!Stopping) {
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

bool TProviderBase::CompleteDiscardedDeliveries() {
    bool pending = false;
    for (const auto& context : GetContexts()) {
        pending = context->CompleteDiscardedDeliveries() || pending;
    }
    return pending;
}

void TProviderBase::Publish(const TTokenCache& current, bool writeCache) {
    if (writeCache) {
        WriteCache(Config.Cacher_, current);
    }
    std::vector<std::pair<std::shared_ptr<TProviderContext>, NThreading::TPromise<std::string>>> pending;
    with_lock (Mutex) {
        if (Stopping) {
            return;
        }
        Tokens = current;
        Error = nullptr;
        for (const auto& weakContext : Contexts) {
            if (auto context = weakContext.lock(); context != nullptr) {
                pending.emplace_back(context, context->TakePending());
            }
        }
    }
    for (auto& [context, promise] : pending) {
        context->Complete(promise, current.AccessToken, {});
    }
}

void TProviderBase::Fail(std::exception_ptr error, bool useResponseQueue) {
    std::vector<std::pair<std::shared_ptr<TProviderContext>, NThreading::TPromise<std::string>>> pending;
    with_lock (Mutex) {
        Error = error;
        for (const auto& weakContext : Contexts) {
            if (auto context = weakContext.lock(); context != nullptr) {
                pending.emplace_back(context, context->TakePending());
            }
        }
    }
    for (auto& [context, promise] : pending) {
        if (useResponseQueue) {
            context->Complete(promise, std::nullopt, error);
        } else {
            SetExceptionAsync(promise, error);
        }
    }
}

TProtocol& TRefreshingProviderBase::GetProtocol() {
    return Protocol;
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
