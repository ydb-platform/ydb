#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/threading/future/future.h>

#include <util/system/mutex.h>

#include <exception>
#include <list>
#include <memory>
#include <optional>
#include <string>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TProviderBase;

// Only delivery state belongs to a driver. Tokens and the grant worker belong
// to TProviderBase and are shared by every provider created by one factory.
class TProviderContext {
    struct TDelivery {
        NThreading::TPromise<std::string> Promise;
        std::weak_ptr<void> CallbackLifetime;
    };

public:
    explicit TProviderContext(std::weak_ptr<ICoreFacility> facility);
    ~TProviderContext();

    bool IsStopped() const;
    NThreading::TFuture<std::string> GetPending() const;
    NThreading::TPromise<std::string> TakePending();
    void Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error);
    bool CompleteDiscardedDeliveries();
    // Cancellation detected by the authentication worker must complete asynchronously.
    void Stop(bool async);

private:
    std::weak_ptr<ICoreFacility> Facility;
    mutable TMutex Mutex;
    bool Stopping = false;
    std::shared_ptr<void> Lifetime;
    NThreading::TPromise<std::string> Pending;
    std::list<TDelivery> Deliveries;
};

class TCredentialsProvider final: public ICredentialsProvider {
public:
    TCredentialsProvider(std::shared_ptr<TProviderBase> source, std::shared_ptr<TProviderContext> context);
    ~TCredentialsProvider() override;

    std::string GetAuthInfo() const override;
    NThreading::TFuture<std::string> GetAuthInfoAsync() const override;
    bool IsValid() const override;

private:
    std::shared_ptr<TProviderBase> Source;
    std::shared_ptr<TProviderContext> Context;
};

std::exception_ptr StoppedError();
void SetExceptionAsync(NThreading::TPromise<std::string> promise, std::exception_ptr error);

} // namespace NYdb::inline Dev::NOidc::NPrivate
