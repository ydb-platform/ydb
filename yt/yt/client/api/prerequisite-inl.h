#ifndef PREREQUISITE_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "prerequisite.h"
#endif

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
TDerived* IPrerequisite::As()
{
    auto* derived = dynamic_cast<TDerived*>(this);
    YT_VERIFY(derived);
    return derived;
}

template <class TDerived>
TDerived* IPrerequisite::TryAs()
{
    return dynamic_cast<TDerived*>(this);
}

template <class TDerived>
const TDerived* IPrerequisite::As() const
{
    const auto* derived = dynamic_cast<const TDerived*>(this);
    YT_VERIFY(derived);
    return derived;
}

template <class TDerived>
const TDerived* IPrerequisite::TryAs() const
{
    return dynamic_cast<const TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
