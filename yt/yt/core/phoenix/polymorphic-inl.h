#ifndef POLYMORPHIC_INL_H_
#error "Direct inclusion of this file is not allowed, include polymorphic.h"
// For the sake of sane code completion.
#include "polymorphic.h"
#endif

#include <concepts>

namespace NYT::NPhoenix2::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TPolymorphicTraits
{
    static constexpr bool Polymorphic = false;
    using TBase = T;
};

template <class T>
    requires std::derived_from<T, TPolymorphicBase>
struct TPolymorphicTraits<T>
{
    static constexpr bool Polymorphic = true;
    using TBase = TPolymorphicBase;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2::NDetail
