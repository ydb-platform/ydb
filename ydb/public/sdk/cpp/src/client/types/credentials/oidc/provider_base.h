#pragma once

#include "private.h"
#include "protocol.h"

#include <util/system/mutex.h>

#include <condition_variable>
#include <thread>

namespace NYdb::inline Dev::NOidc::NPrivate {

class TProviderContext;

// One shared authentication state and worker per factory, independent of drivers.
class TProviderBase: public std::enable_shared_from_this<TProviderBase> {
public:
    explicit TProviderBase(TOidcConfig config);
    virtual ~TProviderBase();

    TCredentialsProviderPtr CreateProvider(std::weak_ptr<ICoreFacility> facility);
    NThreading::TFuture<std::string> GetAuthInfoAsync(const std::shared_ptr<TProviderContext>& context) const;
    bool IsValid(const std::shared_ptr<TProviderContext>& context) const;
    void Stop();

protected:
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
    void Start();
    void Run();
    std::vector<std::shared_ptr<TProviderContext>> GetContexts();
    bool CompleteDiscardedDeliveries();

    mutable TMutex Mutex;
    std::condition_variable_any Changed;
    bool Started = false;
    bool Stopping = false;
    std::optional<TTokenCache> Tokens;
    std::exception_ptr Error;
    std::vector<std::weak_ptr<TProviderContext>> Contexts;
    std::thread Worker;
};

class TRefreshingProviderBase: public TProviderBase {
public:
    explicit TRefreshingProviderBase(const TOidcConfig& config);

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
