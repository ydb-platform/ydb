#ifndef CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include context.h"
// For the sake of sane code completion.
#include "context.h"
#endif

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE NDetail::TUniverseLoadSchedule* TLoadContext::GetLoadSchedule()
{
    return LoadSchedule_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix
