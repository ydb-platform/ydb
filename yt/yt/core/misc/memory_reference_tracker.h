#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Tracks memory used by references.
/*!
 * Memory tracking is implemented by specific shared ref holders.
 * #Track returns reference with a holder that wraps the old one and also
 * enables accounting memory in memory tracker's internal state.

 * Subsequent #Track calls for this reference drop memory reference tracker's
 * holder unless #keepExistingTracking is true.
 */
struct IMemoryReferenceTracker
    : public TRefCounted
{
     //! Tracks reference in a tracker while the reference itself is alive.
    virtual TSharedRef Track(
        TSharedRef reference,
        bool keepExistingTracking = false) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryReferenceTracker)

////////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemory(
    const IMemoryReferenceTrackerPtr& tracker,
    TSharedRef reference,
    bool keepExistingTracking = false);
TSharedRefArray TrackMemory(
    const IMemoryReferenceTrackerPtr& tracker,
    TSharedRefArray array,
    bool keepExistingTracking = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
