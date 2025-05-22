#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NServiceDiscovery;

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
        AlertOnMissingRequestInfo_.store(config->AlertOnMissingRequestInfo);
        SendTracingBaggage_.store(config->SendTracingBaggage);
    }

    const IInvokerPtr& GetLightInvoker()
    {
        return LightQueue_->GetInvoker();
    }

    const IInvokerPtr& GetHeavyInvoker()
    {
        return HeavyPool_->GetInvoker();
    }

    const IPrioritizedInvokerPtr& GetPrioritizedCompressionPoolInvoker()
    {
        return CompressionPoolInvoker_.Value();
    }

    const IFairShareThreadPoolPtr& GetFairShareCompressionThreadPool()
    {
        return FairShareCompressionPool_;
    }

    bool ShouldAlertOnMissingRequestInfo()
    {
        return AlertOnMissingRequestInfo_.load(std::memory_order::relaxed);
    }

    bool ShouldSendTracingBaggage()
    {
        return SendTracingBaggage_.load(std::memory_order::relaxed);
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

    std::atomic<bool> AlertOnMissingRequestInfo_;
    std::atomic<bool> SendTracingBaggage_;

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

const IInvokerPtr& TDispatcher::GetLightInvoker()
{
    return Impl_->GetLightInvoker();
}

const IInvokerPtr& TDispatcher::GetHeavyInvoker()
{
    return Impl_->GetHeavyInvoker();
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

bool TDispatcher::ShouldAlertOnMissingRequestInfo()
{
    return Impl_->ShouldAlertOnMissingRequestInfo();
}

bool TDispatcher::ShouldSendTracingBaggage()
{
    return Impl_->ShouldSendTracingBaggage();
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
