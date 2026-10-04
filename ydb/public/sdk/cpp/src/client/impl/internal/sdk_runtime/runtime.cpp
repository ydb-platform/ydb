#define INCLUDE_YDB_INTERNAL_H
#include "runtime.h"
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <utility>

namespace NYdb::inline Dev {

namespace {

thread_local const TDriverScope* CurrentDriverScope = nullptr;

class TCurrentDriverScope final {
public:
    explicit TCurrentDriverScope(const TDriverScope& scope) noexcept
        : Previous_(std::exchange(CurrentDriverScope, &scope))
    {
    }

    ~TCurrentDriverScope() {
        CurrentDriverScope = Previous_;
    }

private:
    const TDriverScope* Previous_;
};

} // anonymous namespace

class TDriverScope::TLease final {
public:
    explicit TLease(TDriverScope::TPtr scope)
        : Scope_(std::move(scope))
    {
    }

    ~TLease() {
        if (Armed_) {
            Scope_->Release();
        }
    }

    void Arm() noexcept {
        Armed_ = true;
    }

private:
    TDriverScope::TPtr Scope_;
    bool Armed_ = false;
};

class TScopedQueueClientContext final : public NYdbGrpc::IQueueClientContext {
public:
    TScopedQueueClientContext(
        TDriverScope::TPtr scope,
        NYdbGrpc::IQueueClientContextPtr underlying,
        const TDriverScope::TLeasePtr& lease)
        : Lease_(lease)
        , Scope_(std::move(scope))
        , Underlying_(std::move(underlying))
    {
        Y_ABORT_UNLESS(Scope_);
        Y_ABORT_UNLESS(Underlying_);
    }

    NYdbGrpc::IQueueClientContextPtr CreateContext() override {
        return Scope_->CreateChildContext(*Underlying_, Lease_);
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
    bool IsAdmittedBy(const TDriverScope* scope) const noexcept {
        return Scope_.get() == scope && Lease_;
    }

    TDriverScope::TLeasePtr Lease_;
    TDriverScope::TPtr Scope_;
    NYdbGrpc::IQueueClientContextPtr Underlying_;

    friend class TDriverScope;
};

TDriverScope::TDriverScope(NYdbGrpc::IQueueClientContextPtr rootContext)
    : RootContext_(std::move(rootContext))
{
    Y_ABORT_UNLESS(RootContext_);
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::CreateContext() {
    if (IsClosed()) {
        return nullptr;
    }

    auto context = RootContext_->CreateContext();
    if (!context) {
        return nullptr;
    }

    if (IsClosed()) {
        context->Cancel();
        return nullptr;
    }

    return WrapContext(std::move(context));
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::TryAdmitContext(
    NYdbGrpc::IQueueClientContextPtr context)
{
    if (const auto scoped = std::dynamic_pointer_cast<TScopedQueueClientContext>(context);
        scoped && scoped->IsAdmittedBy(this)) {
        return context;
    }

    auto lease = TryAcquire(CLOSED, 0);
    if (!lease) {
        return nullptr;
    }

    auto child = context ? context->CreateContext() : RootContext_->CreateContext();
    return WrapContext(std::move(child), lease);
}

void TDriverScope::RunCallback(std::function<void()> callback) {
    TCurrentDriverScope current(*this);
    callback();
}

bool TDriverScope::IsCurrentThread() const noexcept {
    return CurrentDriverScope != nullptr;
}

TDriverScope::TLeasePtr TDriverScope::TryAcquire(
    std::uint64_t rejectFlag,
    std::uint64_t setFlag)
{
    auto lease = std::make_shared<TLease>(shared_from_this());
    auto state = State_.load(std::memory_order_acquire);
    for (;;) {
        if (state & rejectFlag) {
            return {};
        }
        Y_ABORT_UNLESS((state & OPERATION_COUNT) != OPERATION_COUNT,
            "YDB driver operation count overflow");
        if (State_.compare_exchange_weak(
                state,
                (state | setFlag) + 1,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            lease->Arm();
            return lease;
        }
    }
}

TDriverScope::TLeasePtr TDriverScope::RequestStop() {
    return TryAcquire(STOPPING, STOPPING);
}

void TDriverScope::CloseAdmissions() {
    const auto oldState = State_.fetch_or(CLOSED, std::memory_order_acq_rel);
    Y_ABORT_UNLESS(oldState & STOPPING, "Cannot close a YDB driver before requesting stop");
    Y_ABORT_UNLESS(!(oldState & CLOSED), "YDB driver admissions closed twice");
    State_.notify_all();
}

void TDriverScope::Cancel() {
    RootContext_->Cancel();
}

void TDriverScope::Close() {
    CloseAdmissions();
    Cancel();
}

void TDriverScope::WaitClosed() const {
    auto state = State_.load(std::memory_order_acquire);
    while (!(state & CLOSED)) {
        State_.wait(state, std::memory_order_acquire);
        state = State_.load(std::memory_order_acquire);
    }
}

void TDriverScope::Wait() const {
    auto state = State_.load(std::memory_order_acquire);
    while (state & OPERATION_COUNT) {
        State_.wait(state, std::memory_order_acquire);
        state = State_.load(std::memory_order_acquire);
    }
}

bool TDriverScope::IsClosed() const noexcept {
    return State_.load(std::memory_order_acquire) & CLOSED;
}

bool TDriverScope::IsRetired() const noexcept {
    return State_.load(std::memory_order_acquire) & RETIRED;
}

void TDriverScope::Retire(void* object, TRetireDeleter deleter) noexcept {
    Y_ABORT_UNLESS(object);
    Y_ABORT_UNLESS(deleter);

    auto self = shared_from_this();
    RetiredObject_ = object;
    RetiredDeleter_ = deleter;

    const auto oldState = State_.fetch_or(RETIRED, std::memory_order_acq_rel);
    Y_ABORT_UNLESS(!(oldState & RETIRED), "YDB driver implementation retired twice");
    Y_ABORT_UNLESS(oldState & CLOSED, "YDB driver implementation retired before stop");

    if (!(oldState & OPERATION_COUNT)) {
        RetiredDeleter_(RetiredObject_);
    }
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::CreateChildContext(
    NYdbGrpc::IQueueClientContext& parentContext,
    const TLeasePtr& lease)
{
    return WrapContext(parentContext.CreateContext(), lease);
}

NYdbGrpc::IQueueClientContextPtr TDriverScope::WrapContext(
    NYdbGrpc::IQueueClientContextPtr context,
    const TLeasePtr& lease)
{
    if (!context) {
        return nullptr;
    }
    return std::make_shared<TScopedQueueClientContext>(
        shared_from_this(), std::move(context), lease);
}

void TDriverScope::Release() noexcept {
    const auto oldState = State_.fetch_sub(1, std::memory_order_acq_rel);
    Y_ABORT_UNLESS(oldState & OPERATION_COUNT, "Unbalanced YDB driver operation release");
    const auto newState = oldState - 1;
    if (!(newState & OPERATION_COUNT)) {
        State_.notify_all();
        if (newState & RETIRED) {
            RetiredDeleter_(RetiredObject_);
        }
    }
}

TSdkRuntime::TResources::TResources(TConfig config)
    : NetworkThreads(config.NetworkThreads)
    , ClientThreads(config.ClientThreads)
    , MaxQueueSize(config.MaxQueueSize)
    , HasCustomExecutor(static_cast<bool>(config.Executor))
    , Executor(config.Executor
        ? std::move(config.Executor)
        : CreateThreadPoolExecutor(ClientThreads, MaxQueueSize))
    , Network(NetworkThreads)
{
    Y_ABORT_UNLESS(Executor);
    Executor->Start();
}

TSdkRuntime::TResources& TSdkRuntime::Configure(TConfig config) {
    return GetOrCreate(std::move(config), true);
}

TSdkRuntime::TResources& TSdkRuntime::GetOrCreateForDriver(TConfig config) {
    return GetOrCreate(std::move(config), false);
}

TSdkRuntime::TResources& TSdkRuntime::GetOrCreate(TConfig config, bool validateConfig) {
    auto state = InitializationState_.load(std::memory_order_acquire);
    for (;;) {
        if (state == EInitializationState::Ready) {
            if (validateConfig) {
                ValidateConfig(config, *Resources_);
            } else {
                ValidateDriverConfig(config, *Resources_);
            }
            return *Resources_;
        }

        if (state == EInitializationState::Failed) {
            std::rethrow_exception(InitializationError_);
        }

        if (state == EInitializationState::Configuring) {
            InitializationState_.wait(state, std::memory_order_acquire);
            state = InitializationState_.load(std::memory_order_acquire);
            continue;
        }

        if (InitializationState_.compare_exchange_weak(
                state,
                EInitializationState::Configuring,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            try {
                Resources_ = new TResources(std::move(config));
                InitializationState_.store(EInitializationState::Ready, std::memory_order_release);
                InitializationState_.notify_all();
                return *Resources_;
            } catch (...) {
                InitializationError_ = std::current_exception();
                InitializationState_.store(
                    EInitializationState::Failed,
                    std::memory_order_release);
                InitializationState_.notify_all();
                throw;
            }
        }
    }
}

TDriverScope::TPtr TSdkRuntime::CreateDriverScope(TResources& resources) {
    auto rootContext = resources.Network.CreateContext();
    Y_ABORT_UNLESS(rootContext);
    return TDriverScope::TPtr(new TDriverScope(std::move(rootContext)));
}

void TSdkRuntime::ValidateConfig(const TConfig& config, const TResources& resources) {
    if (config.NetworkThreads != resources.NetworkThreads) {
        throw TContractViolation(TStringBuilder()
            << "YDB SDK runtime network thread count is already configured as "
            << resources.NetworkThreads << ", requested " << config.NetworkThreads);
    }

    if (resources.HasCustomExecutor) {
        if (config.Executor && config.Executor.get() != resources.Executor.get()) {
            throw TContractViolation(
                "YDB SDK runtime is already configured with a different executor");
        }
        return;
    }

    if (config.Executor) {
        throw TContractViolation(
            "YDB SDK runtime is already configured with its default executor");
    }
    if (config.ClientThreads != resources.ClientThreads) {
        throw TContractViolation(TStringBuilder()
            << "YDB SDK runtime client thread count is already configured as "
            << resources.ClientThreads << ", requested " << config.ClientThreads);
    }
    if (config.MaxQueueSize != resources.MaxQueueSize) {
        throw TContractViolation(TStringBuilder()
            << "YDB SDK runtime executor queue limit is already configured as "
            << resources.MaxQueueSize << ", requested " << config.MaxQueueSize);
    }
}

void TSdkRuntime::ValidateDriverConfig(const TConfig& config, const TResources& resources) {
    if (config.Executor && config.Executor.get() != resources.Executor.get()) {
        throw TContractViolation(
            "YDB SDK runtime is already configured with a different executor");
    }
}

TSdkRuntime& GetSdkRuntime() {
    static TSdkRuntime* runtime = new TSdkRuntime();
    return *runtime;
}

} // namespace NYdb
