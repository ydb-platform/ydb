#include "provider.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/provider_base.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/system/compiler.h>
#include <util/system/guard.h>
#include <util/thread/pool.h>

#include <exception>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NOidc::NPrivate {
namespace {

IThreadPool& ErrorExecutor();
void SetException(NThreading::TPromise<std::string> promise, std::exception_ptr error) noexcept;
void SetExceptionAsync(NThreading::TPromise<std::string> promise, std::exception_ptr error);

IThreadPool& ErrorExecutor() {
    // A shared executor outlives providers. Error subscribers may release the
    // last provider and join its authentication worker, so never run them there.
    // Adaptive threads allow one subscriber to wait for another provider's error.
    static const auto executor = [] {
        auto pool = std::make_unique<TAdaptiveThreadPool>(TThreadPoolParams().SetThreadName("OidcErrors"));
        pool->Start(0, 0);
        return pool;
    }();
    return *executor;
}

void SetException(NThreading::TPromise<std::string> promise, std::exception_ptr error) noexcept {
    try {
        promise.TrySetException(std::move(error));
    } catch (...) {
        // A throwing subscriber must not interrupt delivery or cleanup.
        return;
    }
}

void SetExceptionAsync(NThreading::TPromise<std::string> promise, std::exception_ptr error) {
    ErrorExecutor().SafeAddFunc([promise = std::move(promise), error = std::move(error)] {
        SetException(promise, error);
    });
}

} // namespace

std::exception_ptr StoppedError() {
    return std::make_exception_ptr(TError("provider stopped", false, {}));
}

TProviderContext::TProviderContext(std::weak_ptr<ICoreFacility> facility)
    : Facility(std::move(facility))
    , Lifetime(std::make_shared<int>(0))
    , Pending(NThreading::NewPromise<std::string>())
{
}

TProviderContext::~TProviderContext() {
    Stop(false);
}

bool TProviderContext::IsStopped() const {
    with_lock (Mutex) {
        return Stopping || Facility.expired();
    }
}

NThreading::TFuture<std::string> TProviderContext::GetPending() const {
    with_lock (Mutex) {
        if (Stopping || Facility.expired()) {
            return NThreading::MakeErrorFuture<std::string>(StoppedError());
        }
        return Pending.GetFuture();
    }
}

NThreading::TPromise<std::string> TProviderContext::TakePending() {
    with_lock (Mutex) {
        auto pending = Pending;
        Pending = NThreading::NewPromise<std::string>();
        return pending;
    }
}

void TProviderContext::Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error) {
    const auto callbackLifetime = std::make_shared<int>(0);
    std::weak_ptr<void> lifetime;
    bool stopped;
    with_lock (Mutex) {
        stopped = Stopping || Facility.expired();
        std::erase_if(Deliveries, [](const auto& delivery) {
            return delivery.Promise.GetFuture().IsReady();
        });
        if (!stopped) {
            lifetime = Lifetime;
            Deliveries.push_back({pending, callbackLifetime});
        }
    }
    if (stopped) {
        SetExceptionAsync(pending, StoppedError());
        return;
    }
    auto completion = [pending, token = std::move(token), error, callbackLifetime,
                       lifetime, facility = Facility]() mutable {
        Y_UNUSED(callbackLifetime);
        try {
            if (lifetime.expired() || facility.expired()) {
                SetException(pending, StoppedError());
            } else if (error != nullptr) {
                SetException(pending, error);
            } else if (!token->IsValid(TInstant::Now())) {
                SetException(pending, std::make_exception_ptr(TError("access token expired before delivery", false, {})));
            } else {
                pending.TrySetValue("Bearer " + token->Token);
            }
        } catch (...) {
            SetException(pending, std::current_exception());
        }
    };
    try {
        if (auto facility = Facility.lock(); facility != nullptr) {
            facility->PostToResponseQueue(std::move(completion));
        } else {
            SetExceptionAsync(pending, StoppedError());
        }
    } catch (...) {
        SetExceptionAsync(pending, std::current_exception());
    }
}

bool TProviderContext::CompleteDiscardedDeliveries() {
    if (Facility.expired()) {
        Stop(true);
        return false;
    }
    std::vector<NThreading::TPromise<std::string>> discarded;
    bool pending;
    with_lock (Mutex) {
        for (auto it = Deliveries.begin(); it != Deliveries.end();) {
            if (it->Promise.GetFuture().IsReady()) {
                it = Deliveries.erase(it);
            } else if (it->CallbackLifetime.expired()) {
                discarded.push_back(it->Promise);
                it = Deliveries.erase(it);
            } else {
                ++it;
            }
        }
        pending = !Deliveries.empty();
    }
    for (auto& promise : discarded) {
        SetExceptionAsync(promise, StoppedError());
    }
    return pending;
}

void TProviderContext::Stop(bool async) {
    NThreading::TPromise<std::string> pending;
    std::list<TDelivery> deliveries;
    with_lock (Mutex) {
        if (Stopping) {
            return;
        }
        Stopping = true;
        Lifetime.reset();
        pending = Pending;
        deliveries.swap(Deliveries);
    }
    const auto complete = async ? SetExceptionAsync : SetException;
    complete(pending, StoppedError());
    for (auto& delivery : deliveries) {
        complete(delivery.Promise, StoppedError());
    }
}

TCredentialsProvider::TCredentialsProvider(std::shared_ptr<TProviderBase> source, std::shared_ptr<TProviderContext> context)
    : Source(std::move(source))
    , Context(std::move(context))
{
}

TCredentialsProvider::~TCredentialsProvider() {
    Context->Stop(false);
}

std::string TCredentialsProvider::GetAuthInfo() const {
    return GetAuthInfoAsync().GetValueSync();
}

NThreading::TFuture<std::string> TCredentialsProvider::GetAuthInfoAsync() const {
    return Source->GetAuthInfoAsync(Context);
}

bool TCredentialsProvider::IsValid() const {
    return Source->IsValid(Context);
}

} // namespace NYdb::inline Dev::NOidc::NPrivate
