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

    //! Returns the invoker used to dispatch light callbacks
    //! (e.g. discovery or request cancelation).
    IInvokerPtr GetLightInvoker();
    //! Returns the invoker for the thread pool used to dispatch heavy callbacks
    //! (e.g. serialization).
    IInvokerPtr GetHeavyInvoker();

    //! Reconfigures the dispatcher to dispatch light and heavy callbacks
    //! via invokers from the provided worker pool instead of its own
    //! light action queue and heavy thread pool.
    //! Passing nullptr restores the default invokers.
    void SetWorkerPool(NConcurrency::ITwoLevelFairShareThreadPoolPtr workerPool);

    //! Returns the invoker for the thread pool used to dispatch compression callbacks.
    const IInvokerPtr& GetCompressionPoolInvoker();
    //! Returns the prioritized invoker for the thread pool used to
    //! dispatch compression callbacks. This invoker is a wrapper around compression pool invoker.
    const IPrioritizedInvokerPtr& GetPrioritizedCompressionPoolInvoker();
    //! Returns the fair-share thread pool with the similar semantics as previous two.
    //! NB: This thread pool is different from the underlying thread pool beneath two previous invokers.
    const NConcurrency::IFairShareThreadPoolPtr& GetFairShareCompressionThreadPool();

    //! Returns true if alert must be issued when a request carries no annotations.
    bool ShouldAlertOnMissingRequestAnnotation();

    //! Returns true if tracing baggage must be sent.
    bool ShouldSendTracingBaggage();

    //! Returns default request timeout.
    TDuration GetDefaultRequestTimeout();

    //! Returns true if alert must be issued when rpc timeout is unset.
    bool ShouldAlertOnUnsetRequestTimeout();

    NServiceDiscovery::IServiceDiscoveryPtr GetServiceDiscovery();
    void SetServiceDiscovery(NServiceDiscovery::IServiceDiscoveryPtr serviceDiscovery);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
