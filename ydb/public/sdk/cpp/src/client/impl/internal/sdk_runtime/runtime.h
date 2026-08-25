#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace NYdb::inline Dev {

class TScopedQueueClientContext;

class TDriverScope final
    : public NYdbGrpc::IQueueClientContextProvider
    , public std::enable_shared_from_this<TDriverScope>
{
public:
    using TPtr = std::shared_ptr<TDriverScope>;

    NYdbGrpc::IQueueClientContextPtr CreateContext() override;
    NYdbGrpc::TQueueClientCallbackGuardFactory GetCallbackGuardFactory() override;

    void Cancel();
    void WaitCallbacksDrained();
    void CloseCallbacksAndWait();
    void DeferOrRun(std::function<void()> action);

    template<class TOnEntered, class TOnStopped>
    decltype(auto) RunGuarded(TOnEntered&& onEntered, TOnStopped&& onStopped) {
        TCallbackGuard guard(shared_from_this());
        if (guard.IsEntered()) {
            return std::forward<TOnEntered>(onEntered)();
        }
        return std::forward<TOnStopped>(onStopped)();
    }

    static bool IsCurrentThreadInCallback() noexcept;

private:
    class TCallbackGuard final : public NYdbGrpc::IQueueClientCallbackGuard {
    public:
        explicit TCallbackGuard(TPtr scope);
        ~TCallbackGuard();

        bool IsEntered() const noexcept override;

    private:
        TPtr Scope_;
        bool Entered_ = false;
    };

    friend class TSdkRuntime;
    friend class TScopedQueueClientContext;

    explicit TDriverScope(NYdbGrpc::IQueueClientContextPtr rootContext);

    bool TryEnterCallback() noexcept;
    void LeaveCallback() noexcept;
    NYdbGrpc::IQueueClientContextPtr CreateChildContext(
        NYdbGrpc::IQueueClientContext& parentContext);
    NYdbGrpc::IQueueClientContextPtr WrapContext(NYdbGrpc::IQueueClientContextPtr context);

    std::mutex ContextMutex_;
    NYdbGrpc::IQueueClientContextPtr RootContext_;

    std::mutex CallbackMutex_;
    std::condition_variable CallbackDrained_;
    std::uint64_t InFlightCallbacks_ = 0;
    bool CallbacksClosed_ = false;
};

class TSdkRuntime final {
public:
    TDriverScope::TPtr CreateDriverScope(NYdbGrpc::IQueueClientContextProvider& contextProvider);

private:
    TSdkRuntime() = default;
    TSdkRuntime(const TSdkRuntime&) = delete;
    TSdkRuntime& operator=(const TSdkRuntime&) = delete;

    friend TSdkRuntime& GetSdkRuntime();
};

TSdkRuntime& GetSdkRuntime();

} // namespace NYdb
