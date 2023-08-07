#pragma once

#include "public.h"

#include <yt/yt/core/service_discovery/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();
    ~TDispatcher();

    static TDispatcher* Get();

    void Configure(const TDispatcherConfigPtr& config);

    //! Returns the invoker for the single thread used to dispatch light callbacks
    //! (e.g. discovery or request cancelation).
    const IInvokerPtr& GetLightInvoker();
    //! Returns the invoker for the thread pool used to dispatch heavy callbacks
    //! (e.g. serialization).
    const IInvokerPtr& GetHeavyInvoker();

    //! Returns the invoker for the thread pool used to dispatch compression callbacks.
    const IInvokerPtr& GetCompressionPoolInvoker();
    //! Returns the prioritized invoker for the thread pool used to
    //! dispatch compression callbacks. This invoker is a wrapper around compression pool invoker.
    const IPrioritizedInvokerPtr& GetPrioritizedCompressionPoolInvoker();
    //! Returns the fair-share thread pool with the similar semantics as previous two.
    //! NB: this thread pool is different from the underlying thread pool beneath two previous invokers.
    const NConcurrency::IFairShareThreadPoolPtr& GetFairShareCompressionThreadPool();

    //! Returns true if alert must be issued when a request is missing request info.
    bool ShouldAlertOnMissingRequestInfo();

    NServiceDiscovery::IServiceDiscoveryPtr GetServiceDiscovery();
    void SetServiceDiscovery(NServiceDiscovery::IServiceDiscoveryPtr serviceDiscovery);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
