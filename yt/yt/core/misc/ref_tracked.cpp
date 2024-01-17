#include "ref_counted_tracker.h"

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRefCountedTypeCookie TRefCountedTrackerFacade::GetCookie(
    TRefCountedTypeKey typeKey,
    size_t instanceSize,
    const TSourceLocation& location)
{
    return TRefCountedTracker::Get()->GetCookie(
        typeKey,
        instanceSize,
        location);
}

void TRefCountedTrackerFacade::AllocateInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::AllocateInstance(cookie);
}

void TRefCountedTrackerFacade::FreeInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::FreeInstance(cookie);
}

void TRefCountedTrackerFacade::AllocateTagInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::AllocateTagInstance(cookie);
}

void TRefCountedTrackerFacade::FreeTagInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::FreeTagInstance(cookie);
}

void TRefCountedTrackerFacade::AllocateSpace(TRefCountedTypeCookie cookie, size_t size)
{
    TRefCountedTracker::AllocateSpace(cookie, size);
}

void TRefCountedTrackerFacade::FreeSpace(TRefCountedTypeCookie cookie, size_t size)
{
    TRefCountedTracker::FreeSpace(cookie, size);
}

void TRefCountedTrackerFacade::Dump()
{
    fprintf(stderr, "%s", TRefCountedTracker::Get()->GetDebugInfo().data());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
