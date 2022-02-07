#include "message_delay_stats.h"

#include <algorithm>
#include <numeric>

namespace NKikimr::NSQS {

constexpr size_t BucketsCount = 901;
constexpr TDuration BucketLength = TDuration::Seconds(1);
const TDuration WindowLength = BucketLength * BucketsCount;

void TMessageDelayStatistics::AdvanceTime(TInstant now) {
    if (now < Start + BucketLength) {
        return;
    }

    const size_t bucketsDiff = (now - Start).GetValue() / BucketLength.GetValue();
    const size_t bucketsToClear = Min(bucketsDiff, BucketsCount);

    // clear buckets
    for (size_t i = FirstBucket; i < FirstBucket + bucketsToClear; ++i) {
        const size_t index = i < BucketsCount ? i : i - BucketsCount;
        Buckets[index] = 0;
    }
    FirstBucket += bucketsToClear;
    if (FirstBucket >= BucketsCount) {
        FirstBucket -= BucketsCount;
    }
    Start += BucketLength * bucketsDiff;
}

size_t TMessageDelayStatistics::UpdateAndGetMessagesDelayed(TInstant now) {
    if (Buckets.empty()) {
        return 0;
    }

    AdvanceTime(now);
    return std::accumulate(Buckets.begin(), Buckets.end(), 0);
}

void TMessageDelayStatistics::AddDelayedMessage(TInstant delayDeadline, TInstant now) {
    Y_ASSERT(delayDeadline > now);

    // allocate memory with first delayed message
    if (Buckets.empty()) {
        Buckets.resize(BucketsCount);
        Start = now;
    } else {
        AdvanceTime(now);
    }

    if (delayDeadline > Start + WindowLength) {
        return;
    }

    const size_t bucket = (delayDeadline - Start).GetValue() / BucketLength.GetValue();
    ++Buckets[(FirstBucket + bucket) % BucketsCount];
}

} // namespace NKikimr::NSQS
