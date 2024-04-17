#pragma once

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTagSet GetThreadTags(const TString& threadName);

NProfiling::TTagSet GetBucketTags(
    const TString& threadName,
    const TString& bucketName);

NProfiling::TTagSet GetQueueTags(
    const TString& threadName,
    const TString& bucketName,
    const TString& queueName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

