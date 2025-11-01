#include "consumer_offset_tracker.h"

namespace NKikimr::NPQ {

bool ImportantConsumerNeedToKeepCurrentKey(const TDuration availabilityPeriod, const ui64 offset, const TDataKey& currentKey, const TDataKey& nextKey, const TInstant now) {
    const TInstant endOfLife = currentKey.Timestamp + availabilityPeriod; // note: sum with saturation
    if (endOfLife < now) {
        // The current key is too old. It doesn't matter whether the consumer has read it or not. It can be retired.
        return false;
    }
    if (offset < nextKey.Key.GetOffset()) {
        // The first message in the next blob was not read by an important consumer.
        // We also save the current blob, since not all messages from it could be read.
        return true;
    }
    if (offset == nextKey.Key.GetOffset() && nextKey.Key.GetPartNo() != 0) {
        // We save all the blobs that contain parts of the last message read by an important consumer.
        return true;
    }
    return false;
}

TImportantConsumerOffsetTracker::TImportantConsumerOffsetTracker(std::vector<TImportantConsumerOffsetTracker::TConsumerOffset> consumersToCheck)
    : Consumers_(std::move(consumersToCheck))
{
}

bool TImportantConsumerOffsetTracker::ShouldKeepCurrentKey(const TDataKey& currentKey, const TDataKey& nextKey, const TInstant now) const {
    for (const auto& consumer : Consumers_) {
        if (ImportantConsumerNeedToKeepCurrentKey(consumer.AvailabilityPeriod, consumer.Offset, currentKey, nextKey, now)) {
            return true;
        }
    }
    return false;
}

} // namespace NKikimr::NPQ
