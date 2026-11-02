#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NServiceDiscovery;

////////////////////////////////////////////////////////////////////////////////

namespace {

const std::string LightPoolName = "RpcLight";
const std::string HeavyPoolName = "RpcHeavy";

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : CompressionPoolInvoker_(BIND([this] {
            return CreatePrioritizedInvoker(CompressionPool_->GetInvoker(), "rpc_dispatcher");
        }))
    {
        // Configure with defaults.
        Configure(New<TDispatcherConfig>());
    }

    void Configure(const TDispatcherConfigPtr& config)
    {
        HeavyPool_->SetThreadCount(config->HeavyPoolSize);
        HeavyPool_->SetPollingPeriod(config->HeavyPoolPollingPeriod);
        CompressionPool_->SetThreadCount(config->CompressionPoolSize);
        FairShareCompressionPool_->SetThreadCount(config->CompressionPoolSize);
        AlertOnMissingRequestAnnotation_.store(config->AlertOnMissingRequestAnnotation);
        SendTracingBaggage_.store(config->SendTracingBaggage);
        DefaultRequestTimeout_.store(config->DefaultRequestTimeout);
        AlertOnUnsetRequestTimeout_.store(config->AlertOnUnsetRequestTimeout);
    }

    IInvokerPtr GetLightInvoker()
    {
        if (auto invoker = LightInvoker_.Acquire()) {
            return invoker;
        }

        return LightQueue_->GetInvoker();
    }

    IInvokerPtr GetHeavyInvoker()
    {
        if (auto invoker = HeavyInvoker_.Acquire()) {
            return invoker;
        }

        return HeavyPool_->GetInvoker();
    }

    void SetWorkerPool(ITwoLevelFairShareThreadPoolPtr workerPool)
    {
        if (workerPool) {
            // NB: Resolve invokers once here to avoid taking the pool's
            // mapping lock on every GetLightInvoker/GetHeavyInvoker call.
            LightInvoker_.Store(workerPool->GetInvoker(LightPoolName, DefaultExecutionTag));
            HeavyInvoker_.Store(workerPool->GetInvoker(HeavyPoolName, DefaultExecutionTag));
        } else {
            LightInvoker_.Store(nullptr);
            HeavyInvoker_.Store(nullptr);
        }

        WorkerPool_.Store(std::move(workerPool));
    }

    const IPrioritizedInvokerPtr& GetPrioritizedCompressionPoolInvoker()
    {
        return CompressionPoolInvoker_.Value();
    }

    const IFairShareThreadPoolPtr& GetFairShareCompressionThreadPool()
    {
        return FairShareCompressionPool_;
    }

    bool ShouldAlertOnMissingRequestAnnotation()
    {
        return AlertOnMissingRequestAnnotation_.load(std::memory_order::relaxed);
    }

    bool ShouldSendTracingBaggage()
    {
        return SendTracingBaggage_.load(std::memory_order::relaxed);
    }

    TDuration GetDefaultRequestTimeout()
    {
        return DefaultRequestTimeout_.load(std::memory_order::relaxed);
    }

    bool ShouldAlertOnUnsetRequestTimeout()
    {
        return AlertOnUnsetRequestTimeout_.load(std::memory_order::relaxed);
    }

    const IInvokerPtr& GetCompressionPoolInvoker()
    {
        return CompressionPool_->GetInvoker();
    }

    IServiceDiscoveryPtr GetServiceDiscovery()
    {
        return ServiceDiscovery_.Acquire();
    }

    void SetServiceDiscovery(IServiceDiscoveryPtr serviceDiscovery)
    {
        ServiceDiscovery_.Store(std::move(serviceDiscovery));
    }

private:
    const TActionQueuePtr LightQueue_ = New<TActionQueue>("RpcLight");
    const IThreadPoolPtr HeavyPool_ = CreateThreadPool(1, "RpcHeavy");
    const IThreadPoolPtr CompressionPool_ = CreateThreadPool(1, "Compression");
    const IFairShareThreadPoolPtr FairShareCompressionPool_ = CreateFairShareThreadPool(1, "FSCompression");

    TLazyIntrusivePtr<IPrioritizedInvoker> CompressionPoolInvoker_;

    TAtomicIntrusivePtr<ITwoLevelFairShareThreadPool> WorkerPool_;
    TAtomicIntrusivePtr<IInvoker> LightInvoker_;
    TAtomicIntrusivePtr<IInvoker> HeavyInvoker_;

    std::atomic<bool> AlertOnMissingRequestAnnotation_;
    std::atomic<bool> SendTracingBaggage_;

    std::atomic<TDuration> DefaultRequestTimeout_;
    std::atomic<bool> AlertOnUnsetRequestTimeout_;

    TAtomicIntrusivePtr<IServiceDiscovery> ServiceDiscovery_;
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

IInvokerPtr TDispatcher::GetLightInvoker()
{
    return Impl_->GetLightInvoker();
}

IInvokerPtr TDispatcher::GetHeavyInvoker()
{
    return Impl_->GetHeavyInvoker();
}

void TDispatcher::SetWorkerPool(NConcurrency::ITwoLevelFairShareThreadPoolPtr workerPool)
{
    Impl_->SetWorkerPool(std::move(workerPool));
}

const IPrioritizedInvokerPtr& TDispatcher::GetPrioritizedCompressionPoolInvoker()
{
    return Impl_->GetPrioritizedCompressionPoolInvoker();
}

const IInvokerPtr& TDispatcher::GetCompressionPoolInvoker()
{
    return Impl_->GetCompressionPoolInvoker();
}

const IFairShareThreadPoolPtr& TDispatcher::GetFairShareCompressionThreadPool()
{
    return Impl_->GetFairShareCompressionThreadPool();
}

bool TDispatcher::ShouldAlertOnMissingRequestAnnotation()
{
    return Impl_->ShouldAlertOnMissingRequestAnnotation();
}

bool TDispatcher::ShouldSendTracingBaggage()
{
    return Impl_->ShouldSendTracingBaggage();
}

TDuration TDispatcher::GetDefaultRequestTimeout()
{
    return Impl_->GetDefaultRequestTimeout();
}

bool TDispatcher::ShouldAlertOnUnsetRequestTimeout()
{
    return Impl_->ShouldAlertOnUnsetRequestTimeout();
}

IServiceDiscoveryPtr TDispatcher::GetServiceDiscovery()
{
    return Impl_->GetServiceDiscovery();
}

void TDispatcher::SetServiceDiscovery(IServiceDiscoveryPtr serviceDiscovery)
{
    Impl_->SetServiceDiscovery(std::move(serviceDiscovery));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
