#ifndef YPATH_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_detail.h"
// For the sake of sane code completion.
#include "ypath_detail.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TContextPtr>
void TSupportsExistsBase::Reply(const TContextPtr& context, bool exists)
{
    context->Response().set_value(exists);
    context->SetResponseInfo("Result: %v", exists);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
