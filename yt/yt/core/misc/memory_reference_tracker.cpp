#include "memory_reference_tracker.h"
#include "singleton.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNullMemoryReferenceTracker
    : public IMemoryReferenceTracker
{
public:
    TSharedRef Track(
        TSharedRef reference,
        bool /*keepExistingTracking*/) override
    {
        return reference;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMemoryReferenceTrackerPtr GetNullMemoryReferenceTracker()
{
    return LeakyRefCountedSingleton<TNullMemoryReferenceTracker>();
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef TrackMemory(
    const IMemoryReferenceTrackerPtr& tracker,
    TSharedRef reference,
    bool keepExistingTracking)
{
    if (!tracker || !reference) {
        return reference;
    }
    return tracker->Track(reference, keepExistingTracking);
}

TSharedRefArray TrackMemory(
    const IMemoryReferenceTrackerPtr& tracker,
    TSharedRefArray array,
    bool keepExistingTracking)
{
    if (!tracker || !array) {
        return array;
    }
    TSharedRefArrayBuilder builder(array.Size());
    for (const auto& part : array) {
        builder.Add(tracker->Track(part, keepExistingTracking));
    }
    return builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
