#ifndef ATTRIBUTE_FILER_INL_H_
#error "Direct inclusion of this file is not allowed, include attribute_filter.h"
// For the sake of sane code completion.
#include "attribute_filter.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAttributeFilter::TAttributeFilter(std::initializer_list<T> keys)
    : Keys_({keys.begin(), keys.end()})
    , Universal_(false)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
