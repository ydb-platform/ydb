#define INCLUDE_YDB_INTERNAL_H
#include "runtime.h"
#undef INCLUDE_YDB_INTERNAL_H

#include <thread>

namespace NYdb::inline Dev {

namespace {

thread_local std::uint32_t SdkResponseCallbackDepth = 0;

} // anonymous namespace

class TScopedQueueClientContext final : public NYdbGrpc::IQueueClientContext {
public:
    TScopedQueueClientContext(
        NYdbGrpc::IQueueClientContextPtr underlying,
        TDriverScope::TPtr scope)
        : Underlying_(std::move(underlying))
        , Scope_(std::move(scope))
    {
        Y_ABORT_UNLESS(Underlying_);
        Y_ABORT_UNLESS(Scope_);
    }

    NYdbGrpc::IQueueClientContextPtr CreateContext() override {
        return Scope_->CreateChildContext(*Underlying_);
    }

    NYdbGrpc::TQueueClientCallbackGuardFactory GetCallbackGuardFactory() override {
        return Scope_->GetCallbackGuardFactory();
    }

    grpc::CompletionQueue* CompletionQueue() override {
        return Underlying_->CompletionQueue();
    }

    bool IsCancelled() const override {
        return Underlying_->IsCancelled();
    }

    bool Cancel() override {
        return Underlying_->Cancel();
    }

    void SubscribeCancel(std::function<void()> callback) override {
        Underlying_->SubscribeCancel(std::move(callback));
    }

private:
    NYdbGrpc::IQueueClientContextPtr Underlying_;
    TDriverScope::TPtr Scope_;
};

TDriverScope::TDriverScope(NYdbGrpc::IQueueClientContextPtr rootContext)
    : RootContext_(std::move(rootContext))
{
    Y_ABORT_UNLESS(RootContext_);
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::CreateContext() {
    std::lock_guard lock(ContextMutex_);
    if (!RootContext_) {
        return nullptr;
    }
    return WrapContext(RootContext_->CreateContext());
}

NYdbGrpc::TQueueClientCallbackGuardFactory TDriverScope::GetCallbackGuardFactory() {
    auto scope = shared_from_this();
    return [scope = std::move(scope)] {
        return std::make_unique<TCallbackGuard>(scope);
    };
}

void TDriverScope::Cancel() {
    NYdbGrpc::IQueueClientContextPtr rootContext;
    {
        std::lock_guard lock(ContextMutex_);
        rootContext = std::move(RootContext_);
    }

    if (rootContext) {
        rootContext->Cancel();
    }
}

void TDriverScope::WaitCallbacksDrained() {
    std::unique_lock lock(CallbackMutex_);
    CallbackDrained_.wait(lock, [this] {
        return InFlightCallbacks_ == 0;
    });
}

void TDriverScope::CloseCallbacksAndWait() {
    std::unique_lock lock(CallbackMutex_);
    CallbacksClosed_ = true;
    CallbackDrained_.wait(lock, [this] {
        return InFlightCallbacks_ == 0;
    });
}

void TDriverScope::DeferOrRun(std::function<void()> action) {
    if (!IsCurrentThreadInCallback() && !NYdbGrpc::IsGRpcCompletionThread()) {
        action();
        return;
    }

    auto scope = shared_from_this();
    try {
        std::thread([scope = std::move(scope), action = std::move(action)]() mutable {
            scope->WaitCallbacksDrained();
            action();
        }).detach();
    } catch (...) {
        Y_ABORT("Failed to defer YDB driver action from SDK callback thread");
    }
}

bool TDriverScope::IsCurrentThreadInCallback() noexcept {
    return SdkResponseCallbackDepth != 0;
}

bool TDriverScope::TryEnterCallback() noexcept {
    std::unique_lock lock(CallbackMutex_);
    if (CallbacksClosed_) {
        return false;
    }
    ++InFlightCallbacks_;
    return true;
}

void TDriverScope::LeaveCallback() noexcept {
    std::unique_lock lock(CallbackMutex_);
    Y_ABORT_UNLESS(InFlightCallbacks_ > 0);
    if (--InFlightCallbacks_ == 0) {
        CallbackDrained_.notify_all();
    }
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::CreateChildContext(
    NYdbGrpc::IQueueClientContext& parentContext)
{
    std::lock_guard lock(ContextMutex_);
    if (!RootContext_) {
        return nullptr;
    }
    return WrapContext(parentContext.CreateContext());
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::WrapContext(NYdbGrpc::IQueueClientContextPtr context) {
    if (!context) {
        return nullptr;
    }
    return std::make_shared<TScopedQueueClientContext>(std::move(context), shared_from_this());
}

TDriverScope::TCallbackGuard::TCallbackGuard(TPtr scope)
    : Scope_(std::move(scope))
{
    Entered_ = Scope_ && Scope_->TryEnterCallback();
    if (Entered_) {
        ++SdkResponseCallbackDepth;
    }
}

TDriverScope::TCallbackGuard::~TCallbackGuard() {
    if (!Entered_) {
        return;
    }

    --SdkResponseCallbackDepth;
    Scope_->LeaveCallback();
}

bool TDriverScope::TCallbackGuard::IsEntered() const noexcept {
    return Entered_;
}

TDriverScope::TPtr TSdkRuntime::CreateDriverScope(NYdbGrpc::IQueueClientContextProvider& contextProvider) {
    auto rootContext = contextProvider.CreateContext();
    Y_ABORT_UNLESS(rootContext);
    return TDriverScope::TPtr(new TDriverScope(std::move(rootContext)));
}

TSdkRuntime& GetSdkRuntime() {
    static TSdkRuntime* runtime = new TSdkRuntime();
    return *runtime;
}

} // namespace NYdb
