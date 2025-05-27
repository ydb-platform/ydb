#pragma once

#include "public.h"

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TContext>
concept SupportsPersist = requires(T* value, TContext& context) {
    { value->T::Persist(context) } -> std::same_as<void>;
};

template<typename T>
concept SupportsPhoenix = std::same_as<typename T::TPhoenixSupportTag, T>;

template<typename T>
concept DerivedFromPhoenix = std::derived_from<T, typename T::TPhoenixSupportTag>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix
