#ifndef INTERNED_ATTRIBUTES_INL_H_
#error "Direct inclusion of this file is not allowed, include interned_attributes.h"
// For the sake of sane code completion.
#include "interned_attributes.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

constexpr TInternedAttributeKey::operator size_t() const
{
    return Code_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
