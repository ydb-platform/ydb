#ifndef FAIR_SHARE_INVOKER_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include fair_share_invoker_pool.h"
// For the sake of sane code completion.
#include "fair_share_invoker_pool.h"
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class EInvoker>
    requires TEnumTraits<EInvoker>::IsEnum
TDiagnosableInvokerPoolPtr CreateEnumIndexedProfiledFairShareInvokerPool(
    IInvokerPtr underlyingInvoker,
    TFairShareCallbackQueueFactory callbackQueueFactory,
    TDuration actionTimeRelevancyHalflife,
    const std::string& poolName,
    NProfiling::IRegistryPtr registry)
{
    const auto& domainNames = TEnumTraits<EInvoker>::GetDomainNames();

    return CreateProfiledFairShareInvokerPool(
        std::move(underlyingInvoker),
        std::move(callbackQueueFactory),
        actionTimeRelevancyHalflife,
        poolName,
        /*bucketNames*/ {domainNames.begin(), domainNames.end()},
        std::move(registry));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
