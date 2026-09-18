#pragma once

#include "private.h"

#include <condition_variable>
#include <mutex>
#include <thread>

namespace NYdb::inline Dev::NOidc::NPrivate {

// The worker owns this state independently of the credentials provider. A
// callback may release the provider while a flow is still unwinding.
class TProviderState {
    struct TDelivery {
        NThreading::TPromise<std::string> Promise;
        std::weak_ptr<void> CallbackLifetime;
    };

public:
    TProviderState(TOidcConfig config, std::weak_ptr<ICoreFacility> facility, bool standalone);
    virtual ~TProviderState() = default;

    NThreading::TFuture<std::string> GetAuthInfoAsync() const;
    bool IsValid() const;
    void Stop();
    void Run();
    void CancelDeliveries();

protected:
    virtual TTokenCache Bootstrap() = 0;
    virtual TTokenCache AcquireToken(const TTokenCache& current, std::unique_ptr<ITokenCacheLock>& lock) = 0;
    virtual bool WaitForRefresh(const TTokenCache& current);

    bool Wait(TDuration delay);
    std::unique_ptr<ITokenCacheLock> LockCache();
    std::optional<TTokenCache> ReadCache() const;
    void Write(const TTokenCache& tokens) const;
    void Fail(std::exception_ptr error);
    TProtocol& GetProtocol();

    TOidcConfig Config;

private:
    void RunTokens();
    bool IsStopped() const;
    TTokenCache Update(TTokenCache current);
    void Complete(NThreading::TPromise<std::string> pending, std::optional<TOAuthToken> token, std::exception_ptr error);
    void CompleteDiscardedDeliveries();
    void Publish(const TTokenCache& current);

    std::weak_ptr<ICoreFacility> Facility;
    const bool Standalone;
    mutable std::mutex Mutex;
    std::condition_variable Changed;
    bool Stopping = false;
    std::optional<TTokenCache> Tokens;
    std::exception_ptr Error;
    NThreading::TPromise<std::string> Pending;
    std::vector<TDelivery> Deliveries;
    NThreading::TCancellationTokenSource Cancellation;
    TProtocol Protocol;
};

class TProviderBase: public ICredentialsProvider {
public:
    ~TProviderBase() override;

    std::string GetAuthInfo() const override;
    NThreading::TFuture<std::string> GetAuthInfoAsync() const override;
    bool IsValid() const override;

protected:
    explicit TProviderBase(std::shared_ptr<TProviderState> state);

private:
    std::shared_ptr<TProviderState> State;
    std::thread Worker;
};

} // namespace NYdb::inline Dev::NOidc::NPrivate
