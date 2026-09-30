#pragma once

#include "private.h"

#include <util/system/mutex.h>

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
    void Stop();

private:
    std::weak_ptr<ICoreFacility> Facility;
    mutable TMutex Mutex;
    bool Stopping = false;
    std::shared_ptr<void> Lifetime;
    NThreading::TPromise<std::string> Pending;
    std::vector<TDelivery> Deliveries;
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

} // namespace NYdb::inline Dev::NOidc::NPrivate
