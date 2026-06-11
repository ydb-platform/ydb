#include "helpers.h"

#include <yt/yt/core/misc/hazard_ptr.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TTagSet GetThreadTags(
    const std::string& threadName)
{
    TTagSet tags;
    tags.AddTag(TTag("thread", threadName));
    return tags;
}

TTagSet GetBucketTags(
    const std::string& threadName,
    const std::string& bucketName)
{
    TTagSet tags;

    tags.AddTag(TTag("thread", threadName));
    tags.AddTag(TTag("bucket", bucketName), -1);

    return tags;
}

TTagSet GetQueueTags(
    const std::string& threadName,
    const std::string& bucketName,
    const std::string& queueName)
{
    TTagSet tags;

    tags.AddTag(TTag("thread", threadName));
    tags.AddTag(TTag("bucket", bucketName), -1);
    tags.AddTag(TTag("queue", queueName), -1);

    return tags;
}

////////////////////////////////////////////////////////////////////////////////

YT_PREVENT_TLS_CACHING bool ReclaimHazardPointersPeriodically(TCpuInstant now, bool force)
{
    static const auto ReclaimPeriod = DurationToCpuDuration(HazardPointerReclaimPeriod);
    static thread_local TCpuInstant LastReclaimInstant;

    if (!force && now < LastReclaimInstant + ReclaimPeriod) {
        return false;
    }

    LastReclaimInstant = now;
    return ReclaimHazardPointers(/*flush*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
