#pragma once

#include "public.h"

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TContext>
concept SupportsPersist = requires(T* value, TContext& context) {
    { value->T::Persist(context) } -> std::same_as<void>;
};

template<typename T>
concept SupportsPhoenix2 = std::same_as<typename T::TPhoenix2SupportTag, T>;

template<typename T>
concept DerivedFromPhoenix2 = std::derived_from<T, typename T::TPhoenix2SupportTag>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
