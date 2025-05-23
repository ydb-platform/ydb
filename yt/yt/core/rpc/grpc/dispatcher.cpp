#include "dispatcher.h"

#include "config.h"

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/misc/shutdown_priorities.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>

#include <contrib/libs/grpc/src/core/lib/event_engine/thread_pool.h>

#include <contrib/libs/grpc/src/core/lib/iomgr/executor.h>

#include <atomic>

namespace NYT::NRpc::NGrpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = GrpcLogger;

////////////////////////////////////////////////////////////////////////////////

void* TCompletionQueueTag::GetTag(int cookie)
{
    YT_ASSERT(cookie >= 0 && cookie < 8);
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(this) | cookie);
}

////////////////////////////////////////////////////////////////////////////////

TGrpcLibraryLock::TGrpcLibraryLock()
{
    YT_LOG_INFO("Initializing GRPC library");
    grpc_init_openssl();
    grpc_init();
}

TGrpcLibraryLock::~TGrpcLibraryLock()
{
    YT_LOG_INFO("Shutting down GRPC library");
    grpc_shutdown();
}

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    [[nodiscard]] bool IsInitialized() const noexcept
    {
        return Initialized_.load();
    }

    void Configure(const TDispatcherConfigPtr& config)
    {
        auto guard = Guard(ConfigLock_);

        if (IsInitialized()) {
            THROW_ERROR_EXCEPTION("GRPC dispatcher is already initialized and cannot be reconfigured");
        }

        Config_ = config;
    }

    TGrpcLibraryLockPtr GetLibraryLock()
    {
        EnsureInitialized();
        auto grpcLock = LibraryLock_.Lock();
        YT_VERIFY(grpcLock);
        return grpcLock;
    }

    TGuardedGrpcCompletionQueue* PickRandomGuardedCompletionQueue()
    {
        EnsureInitialized();
        return Threads_[RandomNumber<size_t>() % Threads_.size()]->GetGuardedCompletionQueue();
    }

private:
    class TDispatcherThread
        : public NThreading::TThread
    {
    public:
        TDispatcherThread(TGrpcLibraryLockPtr libraryLock, int index)
            : TThread(
                Format("Grpc/%v", index),
                {.ShutdownPriority = GrpcDispatcherThreadShutdownPriority})
            , LibraryLock_(std::move(libraryLock))
            , GuardedCompletionQueue_(TGrpcCompletionQueuePtr(grpc_completion_queue_create_for_next(nullptr)))
        { }

        TGuardedGrpcCompletionQueue* GetGuardedCompletionQueue()
        {
            return &GuardedCompletionQueue_;
        }

    private:
        TGrpcLibraryLockPtr LibraryLock_;
        TGuardedGrpcCompletionQueue GuardedCompletionQueue_;

        void StopPrologue() override
        {
            GuardedCompletionQueue_.Shutdown();
        }

        void StopEpilogue() override
        {
            GuardedCompletionQueue_.Reset();
            LibraryLock_.Reset();
        }

        void ThreadMain() override
        {
            YT_LOG_DEBUG("Dispatcher thread started");

            // Take raw completion queue for fetching tasks,
            // because `grpc_completion_queue_next` can be concurrent with other operations.
            grpc_completion_queue* completionQueue = GuardedCompletionQueue_.UnwrapUnsafe();

            bool done = false;
            while (!done) {
                auto event = grpc_completion_queue_next(
                    completionQueue,
                    gpr_inf_future(GPR_CLOCK_REALTIME),
                    nullptr);
                switch (event.type) {
                    case GRPC_OP_COMPLETE:
                        if (event.tag) {
                            auto* typedTag = reinterpret_cast<TCompletionQueueTag*>(reinterpret_cast<intptr_t>(event.tag) & ~7);
                            auto cookie = reinterpret_cast<intptr_t>(event.tag) & 7;
                            typedTag->Run(event.success != 0, cookie);
                        }
                        break;

                    case GRPC_QUEUE_SHUTDOWN:
                        done = true;
                        break;

                    default:
                        YT_ABORT();
                }
            }

            YT_LOG_DEBUG("Dispatcher thread stopped");
        }
    };

    using TDispatcherThreadPtr = TIntrusivePtr<TDispatcherThread>;

    void EnsureInitialized()
    {
        if (IsInitialized()) {
            return;
        }

        auto guard = Guard(ConfigLock_);

        if (IsInitialized()) {
            return;
        }

        DoInitialize();
    }

    void DoInitialize()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(ConfigLock_);
        YT_VERIFY(!IsInitialized());

        grpc_core::Executor::SetThreadsLimit(Config_->GrpcThreadCount);
        grpc_event_engine::experimental::ThreadPool::SetThreadsLimit(Config_->GrpcEventEngineThreadCount);

        // Initialize grpc only after configuration is done.
        auto grpcLock = New<TGrpcLibraryLock>();
        for (int index = 0; index < Config_->DispatcherThreadCount; ++index) {
            auto dispatcherThread = New<TDispatcherThread>(grpcLock, index);
            dispatcherThread->Start();
            Threads_.push_back(std::move(dispatcherThread));
        }
        LibraryLock_ = grpcLock;
        Initialized_.store(true);
    }


    std::atomic<bool> Initialized_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ConfigLock_);
    TDispatcherConfigPtr Config_ = New<TDispatcherConfig>();

    TWeakPtr<TGrpcLibraryLock> LibraryLock_;
    std::vector<TDispatcherThreadPtr> Threads_;
};

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : Impl_(std::make_unique<TImpl>())
{ }

TDispatcher::~TDispatcher() = default;

TDispatcher* TDispatcher::Get()
{
    return LeakySingleton<TDispatcher>();
}

void TDispatcher::Configure(const TDispatcherConfigPtr& config)
{
    Impl_->Configure(config);
}

bool TDispatcher::IsInitialized() const noexcept
{
    return Impl_->IsInitialized();
}

TGrpcLibraryLockPtr TDispatcher::GetLibraryLock()
{
    return Impl_->GetLibraryLock();
}

TGuardedGrpcCompletionQueue* TDispatcher::PickRandomGuardedCompletionQueue()
{
    return Impl_->PickRandomGuardedCompletionQueue();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
