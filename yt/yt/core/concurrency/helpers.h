#pragma once

#include "public.h"

#include <yt/yt/library/profiling/tag.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <util/datetime/base.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(
    const std::string& threadName);

NProfiling::TTagSet GetBucketTags(
    const std::string& threadName,
    const std::string& bucketName);

NProfiling::TTagSet GetQueueTags(
    const std::string& threadName,
    const std::string& bucketName,
    const std::string& queueName);

////////////////////////////////////////////////////////////////////////////////

//! Period between throttled hazard pointer reclamations performed by a worker thread,
//! and the maximum time such a thread stays parked while it still has retired-but-
//! unreclaimable hazard pointers.
constexpr auto HazardPointerReclaimPeriod = TDuration::Seconds(1);

//! Reclaims hazard pointers retired by the current thread, throttled to at most once
//! per #HazardPointerReclaimPeriod unless |force| is set. |now| is the current CPU
//! instant, supplied by the caller to avoid an extra clock read on hot paths. Returns
//! |true| if some retired pointers remain pending (they are still protected and could
//! not be reclaimed yet), in which case the caller must arrange for the thread to wake
//! up again to retry rather than blocking indefinitely.
bool ReclaimHazardPointersPeriodically(TCpuInstant now, bool force);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
