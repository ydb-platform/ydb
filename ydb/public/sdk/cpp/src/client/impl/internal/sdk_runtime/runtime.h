#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>

namespace NYdb::inline Dev {

class TScopedQueueClientContext;

class TDriverScope final
    : public NYdbGrpc::IQueueClientContextProvider
    , public std::enable_shared_from_this<TDriverScope>
{
    class TLease;
    using TLeasePtr = std::shared_ptr<TLease>;

public:
    using TPtr = std::shared_ptr<TDriverScope>;

    NYdbGrpc::IQueueClientContextPtr CreateContext() override;
    NYdbGrpc::IQueueClientContextPtr TryAdmitContext(
        NYdbGrpc::IQueueClientContextPtr context = {});

    void RunCallback(std::function<void()> callback);
    bool IsCurrentThread() const noexcept;

    TLeasePtr RequestStop();
    void CloseAdmissions();
    void Cancel();
    void Close();
    void WaitClosed() const;
    void Wait() const;
    bool IsClosed() const noexcept;
    bool IsRetired() const noexcept;

    using TRetireDeleter = void (*)(void*) noexcept;

    void Retire(void* object, TRetireDeleter deleter) noexcept;

    template <class T>
    void Retire(T* object) noexcept {
        Retire(object, [](void* value) noexcept {
            delete static_cast<T*>(value);
        });
    }

private:
    static constexpr std::uint64_t STOPPING = std::uint64_t{1} << 63;
    static constexpr std::uint64_t CLOSED = std::uint64_t{1} << 62;
    static constexpr std::uint64_t RETIRED = std::uint64_t{1} << 61;
    static constexpr std::uint64_t FLAGS = STOPPING | CLOSED | RETIRED;
    static constexpr std::uint64_t OPERATION_COUNT = ~FLAGS;

    friend class TSdkRuntime;
    friend class TScopedQueueClientContext;
    friend class TLease;

    explicit TDriverScope(NYdbGrpc::IQueueClientContextPtr rootContext);

    NYdbGrpc::IQueueClientContextPtr CreateChildContext(
        NYdbGrpc::IQueueClientContext& parentContext,
        const TLeasePtr& lease);
    NYdbGrpc::IQueueClientContextPtr WrapContext(
        NYdbGrpc::IQueueClientContextPtr context,
        const TLeasePtr& lease = {});
    TLeasePtr TryAcquire(std::uint64_t rejectFlag, std::uint64_t setFlag);
    void Release() noexcept;

    const NYdbGrpc::IQueueClientContextPtr RootContext_;
    mutable std::atomic<std::uint64_t> State_{0};
    void* RetiredObject_ = nullptr;
    TRetireDeleter RetiredDeleter_ = nullptr;
};

class TSdkRuntime final {
public:
    struct TConfig final {
        std::size_t NetworkThreads = NYdbGrpc::DEFAULT_NUM_THREADS;
        std::size_t ClientThreads = 0;
        std::size_t MaxQueueSize = 100;
        IExecutor::TPtr Executor;
    };

    struct TResources final {
        const std::size_t NetworkThreads;
        const std::size_t ClientThreads;
        const std::size_t MaxQueueSize;
        const bool HasCustomExecutor;
        const IExecutor::TPtr Executor;
        NYdbGrpc::TGRpcClientLow Network;

    private:
        explicit TResources(TConfig config);

        friend class TSdkRuntime;
    };

    TResources& Configure(TConfig config);
    TResources& GetOrCreateForDriver(TConfig config);
    TDriverScope::TPtr CreateDriverScope(TResources& resources);

private:
    enum class EInitializationState : std::uint8_t {
        Unconfigured,
        Configuring,
        Ready,
        Failed,
    };

    TSdkRuntime() = default;
    TSdkRuntime(const TSdkRuntime&) = delete;
    TSdkRuntime& operator=(const TSdkRuntime&) = delete;

    TResources& GetOrCreate(TConfig config, bool validateConfig);
    static void ValidateConfig(const TConfig& config, const TResources& resources);
    static void ValidateDriverConfig(const TConfig& config, const TResources& resources);

    mutable std::atomic<EInitializationState> InitializationState_{
        EInitializationState::Unconfigured};
    TResources* Resources_ = nullptr;
    std::exception_ptr InitializationError_;

    friend TSdkRuntime& GetSdkRuntime();
};

TSdkRuntime& GetSdkRuntime();

} // namespace NYdb
