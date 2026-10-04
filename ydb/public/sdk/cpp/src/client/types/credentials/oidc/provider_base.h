#pragma once

#include "private.h"
#include "protocol.h"

#include <util/system/mutex.h>

#include <condition_variable>
#include <thread>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TProviderBase: public ICredentialsProvider {
    struct TDelivery {
        NThreading::TPromise<std::string> Promise;
        std::weak_ptr<void> CallbackLifetime;
    };

public:
    TProviderBase(TOidcConfig config, std::weak_ptr<ICoreFacility> facility);
    ~TProviderBase() override;

    std::string GetAuthInfo() const override;
    NThreading::TFuture<std::string> GetAuthInfoAsync() const override;
    bool IsValid() const override;
    void Stop();

protected:
    void Start();
    virtual void RunTokens() = 0;

    bool Wait(TDuration delay);
    std::optional<TTokenCache> ReadCache() const;
    void Write(const TTokenCache& tokens) const;
    void Fail(std::exception_ptr error);
    void Publish(const TTokenCache& current);
    bool IsStopped() const;
    void RequestStop();

    TOidcConfig Config;
    NThreading::TCancellationTokenSource Cancellation;

private:
    void Run();
    void CancelDeliveries();
    void Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error);
    void CompleteDiscardedDeliveries();

    std::weak_ptr<ICoreFacility> Facility;
    mutable TMutex Mutex;
    std::condition_variable_any Changed;
    bool Stopping = false;
    std::optional<TTokenCache> Tokens;
    std::exception_ptr Error;
    NThreading::TPromise<std::string> Pending;
    std::vector<TDelivery> Deliveries;
    std::thread Worker;
};

class TRefreshingProviderBase: public TProviderBase {
public:
    TRefreshingProviderBase(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility);

protected:
    virtual TTokenCache AcquireToken() = 0;

    TProtocol& GetProtocol();

private:
    void RunTokens() override;
    bool WaitForRefresh(const TTokenCache& current);
    TTokenCache Update(const TTokenCache& current);

    TProtocol Protocol;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
