#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/singleton.h>

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
    { }

    void Configure(const TDispatcherConfigPtr& config)
    {
        HeavyPool_->Configure(config->HeavyPoolSize);
        HeavyPool_->Configure(config->HeavyPoolPollingPeriod);
        CompressionPool_->Configure(config->CompressionPoolSize);
        FairShareCompressionPool_->Configure(config->CompressionPoolSize);
        AlertOnMissingRequestInfo_.store(config->AlertOnMissingRequestInfo);
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
    const IThreadPoolPtr HeavyPool_ = CreateThreadPool(TDispatcherConfig::DefaultHeavyPoolSize, "RpcHeavy");
    const IThreadPoolPtr CompressionPool_ = CreateThreadPool(TDispatcherConfig::DefaultCompressionPoolSize, "Compression");
    const IFairShareThreadPoolPtr FairShareCompressionPool_ = CreateFairShareThreadPool(TDispatcherConfig::DefaultCompressionPoolSize, "FSCompression");

    TLazyIntrusivePtr<IPrioritizedInvoker> CompressionPoolInvoker_;

    std::atomic<bool> AlertOnMissingRequestInfo_;

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
