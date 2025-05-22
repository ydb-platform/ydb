#ifndef SERVICE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include service_detail.h"
// For the sake of sane code completion.
#include "service_detail.h"
#endif

#include "helpers.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
void TDynamicConcurrencyLimit<TValue>::Reconfigure(TValue limit)
{
    ConfigLimit_.store(limit, std::memory_order::relaxed);
    SetDynamicLimit(limit);
}

template <class TValue>
TValue TDynamicConcurrencyLimit<TValue>::GetLimitFromConfiguration() const
{
    return ConfigLimit_.load(std::memory_order::relaxed);
}

template <class TValue>
TValue TDynamicConcurrencyLimit<TValue>::GetDynamicLimit() const
{
    return DynamicLimit_.load(std::memory_order::relaxed);
}

template <class TValue>
void TDynamicConcurrencyLimit<TValue>::SetDynamicLimit(std::optional<TValue> dynamicLimit)
{
    auto limit = dynamicLimit.has_value() ? *dynamicLimit : ConfigLimit_.load(std::memory_order::relaxed);
    auto oldLimit = DynamicLimit_.exchange(limit, std::memory_order::relaxed);

    if (oldLimit != limit) {
        Updated_.Fire();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class E>
void TServiceBase::DeclareServerFeature(E featureId)
{
    DoDeclareServerFeature(FeatureIdToInt(featureId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
