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

// This facade is the single cross-library entry point TRefTracked<T> links against
// and the only friended caller of the private TRefCountedTracker counter functions,
// which inline into it. YT_PREVENT_TLS_CACHING pins each one out-of-line so the
// inlined local-exec TLS read re-reads the thread pointer on every call and never
// caches it across a fiber migration.

YT_PREVENT_TLS_CACHING void TRefCountedTrackerFacade::AllocateInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::AllocateInstance(cookie);
}

YT_PREVENT_TLS_CACHING void TRefCountedTrackerFacade::FreeInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::FreeInstance(cookie);
}

YT_PREVENT_TLS_CACHING void TRefCountedTrackerFacade::AllocateTagInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::AllocateTagInstance(cookie);
}

YT_PREVENT_TLS_CACHING void TRefCountedTrackerFacade::FreeTagInstance(TRefCountedTypeCookie cookie)
{
    TRefCountedTracker::FreeTagInstance(cookie);
}

YT_PREVENT_TLS_CACHING void TRefCountedTrackerFacade::AllocateSpace(TRefCountedTypeCookie cookie, size_t size)
{
    TRefCountedTracker::AllocateSpace(cookie, size);
}

YT_PREVENT_TLS_CACHING void TRefCountedTrackerFacade::FreeSpace(TRefCountedTypeCookie cookie, size_t size)
{
    TRefCountedTracker::FreeSpace(cookie, size);
}

void TRefCountedTrackerFacade::Dump()
{
    fprintf(stderr, "%s", TRefCountedTracker::Get()->GetDebugInfo().data());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
