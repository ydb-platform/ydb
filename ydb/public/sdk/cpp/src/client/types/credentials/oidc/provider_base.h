#pragma once

#include "private.h"
#include "protocol.h"

#include <util/system/mutex.h>

#include <condition_variable>
#include <thread>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TProviderBase {
    struct TDelivery {
        NThreading::TPromise<std::string> Promise;
        std::weak_ptr<void> CallbackLifetime;
    };

public:
    TProviderBase(TOidcConfig config, std::weak_ptr<ICoreFacility> facility, bool standalone);
    virtual ~TProviderBase();

    NThreading::TFuture<std::string> GetAuthInfoAsync() const;
    bool IsValid() const;
    void Stop();
    void Run();
    void CancelDeliveries();

protected:
    virtual void RunTokens() = 0;

    bool Wait(TDuration delay);
    std::optional<TTokenCache> ReadCache() const;
    void Write(const TTokenCache& tokens) const;
    void Fail(std::exception_ptr error);
    void Publish(const TTokenCache& current);
    bool IsStopped() const;

    TOidcConfig Config;
    NThreading::TCancellationTokenSource Cancellation;

private:
    void Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error);
    void CompleteDiscardedDeliveries();

    std::weak_ptr<ICoreFacility> Facility;
    const bool Standalone;
    mutable TMutex Mutex;
    std::condition_variable_any Changed;
    bool Stopping = false;
    std::optional<TTokenCache> Tokens;
    std::exception_ptr Error;
    NThreading::TPromise<std::string> Pending;
    std::vector<TDelivery> Deliveries;
};

class TRefreshingProviderBase: public TProviderBase {
public:
    TRefreshingProviderBase(const TOidcConfig& config, std::weak_ptr<ICoreFacility> facility, bool standalone);

protected:
    virtual TTokenCache AcquireToken() = 0;

    TProtocol& GetProtocol();

private:
    void RunTokens() override;
    bool WaitForRefresh(const TTokenCache& current);
    TTokenCache Update(const TTokenCache& current);

    TProtocol Protocol;
};

class TCredentialsProviderAdapter final: public ICredentialsProvider {
public:
    explicit TCredentialsProviderAdapter(std::shared_ptr<TProviderBase> provider);
    ~TCredentialsProviderAdapter() override;

    std::string GetAuthInfo() const override;
    NThreading::TFuture<std::string> GetAuthInfoAsync() const override;
    bool IsValid() const override;

private:
    std::shared_ptr<TProviderBase> Provider;
    std::thread Worker;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
